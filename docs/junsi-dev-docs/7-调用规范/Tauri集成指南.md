# 调用规范

> 生成时间：2026-08-06 23:44

# Tauri 集成指南（前端调用下载核心）

本文档指导如何在 Tauri 2.x 桌面/移动应用中集成 `qomicex-downloader`。核心思路：`DownloadManager` 放入 Tauri State，用 `tauri::command` 暴露控制接口，事件通过 `emit` 推送到前端。

## 1. 引入依赖

```toml
# src-tauri/Cargo.toml
[dependencies]
qomicex-downloader = { path = "../../qomicex-downloader-rust" }
```

> Android 构建无需额外配置：本库使用 rustls（纯 Rust TLS），Tauri CLI 自带 NDK 环境，直接 `tauri android build` 即可。

## 2. 状态管理

```rust
// src-tauri/src/lib.rs
use std::sync::Arc;
use qomicex_downloader::{DownloadEvent, DownloadManager, DownloadOptions};
use tauri::{Emitter, Manager};
use tokio::sync::Mutex;

pub struct AppState {
    pub manager: DownloadManager,
}

#[tauri::command]
pub async fn shutdown(state: tauri::State<'_, AppState>) {
    state.manager.shutdown().await;
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            // 全局并发 4 + 默认激进稳定配置
            let manager = DownloadManager::new(DownloadOptions::default(), 4);
            app.manage(AppState { manager });
            spawn_event_bridge(app.handle().clone());
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_task, pause_task, resume_task, cancel_task, task_state, task_list, shutdown
        ])
        .run(tauri::generate_context!())
        .expect("Tauri 启动失败");
}
```

## 3. 事件桥：广播 → 前端

后台任务把 `DownloadManager` 的广播事件转发为 Tauri 事件（前端 `listen` 订阅）：

```rust
fn spawn_event_bridge(app: tauri::AppHandle) {
    // 注意：需要在 manage() 之后再获取 state
    let manager = app.state::<AppState>().manager.clone_subscription_handle();
    tauri::async_runtime::spawn(async move {
        let mut rx = manager.subscribe();
        loop {
            match rx.recv().await {
                Ok(ev) => { let _ = app.emit("download://event", ev); }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    });
}
```

> `DownloadEvent` 需派生 `Serialize`/`Clone` 才能通过 Tauri 序列化（本库事件自带 `Clone`；`Serialize` 可在 Tauri 项目内用 `#[derive(serde::Serialize)]` 包装，或后续给事件加 `serde` feature）。

## 4. 控制命令

```rust
#[tauri::command]
pub async fn add_task(
    state: tauri::State<'_, AppState>,
    url: String,
    dest: String,
) -> Result<u64, String> {
    let task = DownloadTask::new(url, std::path::PathBuf::from(dest));
    Ok(state.manager.add(task))
}

#[tauri::command]
pub async fn pause_task(state: tauri::State<'_, AppState>, id: u64) -> Result<(), String> {
    state.manager.pause(id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn resume_task(state: tauri::State<'_, AppState>, id: u64) -> Result<(), String> {
    state.manager.resume(id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn cancel_task(state: tauri::State<'_, AppState>, id: u64) -> Result<(), String> {
    state.manager.cancel(id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn task_state(
    state: tauri::State<'_, AppState>,
    id: u64,
) -> Result<String, String> {
    state.manager.state(id).await.map(|s| format!("{s:?}")).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn task_list(state: tauri::State<'_, AppState>) -> Result<Vec<(u64, String)>, String> {
    let list = state.manager.list().await;
    Ok(list.into_iter().map(|(id, st)| (id, format!("{st:?}"))).collect())
}
```

## 5. 前端调用

```ts
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';

// 订阅下载事件（三级进度：Progress / GlobalProgress / Log / StateChanged）
const unlisten = await listen<DownloadEvent>('download://event', (e) => {
  switch (e.payload.type) {
    case 'Progress':
      updateProgressBar(e.payload.downloaded, e.payload.total, e.payload.speed_bps);
      break;
    case 'GlobalProgress':
      updateGlobalStatus(e.payload.active_tasks, e.payload.downloaded, e.payload.total);
      break;
    case 'StateChanged':
      updateTaskState(e.payload.id, e.payload.state);
      break;
  }
});

// 添加下载
const id: number = await invoke('add_task', {
  url: 'https://example.com/asset.jar',
  dest: 'C:/downloads/asset.jar',
});

// 控制
await invoke('pause_task', { id });
await invoke('resume_task', { id });
await invoke('cancel_task', { id });
```

## 6. 最佳实践

- **目标路径**：桌面用 `app.path().download_dir()` 或用户选择的目录；Android 必须用应用私有目录（`app.path().app_cache_dir()`），`/sdcard` 需额外权限
- **进度 UI 节流**：事件已按 150ms（任务级）/250ms（全局）节流，直接渲染即可，无需前端再节流
- **初始化任务列表**：App 启动后调用 `task_list()` 恢复上次会话的任务（配合 `.part` 文件即可自动断点续传，重新 `add` 相同 dest 即可）
- **生命周期**：窗口关闭前调用 `shutdown()`，避免 worker 任务残留
- **错误展示**：`StateChanged { state: "Failed", detail }` 的 `detail` 即为可展示错误信息

## 7. Android 特有注意

- Tauri Android 项目直接引用本库即可，`tauri android build` 自动处理 NDK
- 下载路径必须使用应用沙箱目录；使用 `app.path().app_cache_dir().join("downloads")`
- Android 前台服务/通知：下载任务在 Tauri 后台线程运行，长任务建议用 `tauri-plugin-notification` 展示进度通知


## 修订记录
| 日期 | 版本 | 修改内容 | 修改人 |
| 2026-08-06 | v1.0 | 初版创建 | AI Agent |

### 2026-08-06 更新
## 2. 状态管理

```rust
// src-tauri/src/lib.rs
use std::sync::Arc;
use qomicex_downloader::{DownloadManager, DownloadOptions};
use tauri::{Emitter, Manager};

pub struct AppState {
    pub manager: Arc<DownloadManager>,
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            // 全局并发 4 + 默认激进稳定配置
            let manager = Arc::new(DownloadManager::new(DownloadOptions::default(), 4));
            spawn_event_bridge(app.handle().clone(), manager.clone());
            app.manage(AppState { manager });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_task, pause_task, resume_task, cancel_task, task_state, task_list, shutdown
        ])
        .run(tauri::generate_context!())
        .expect("Tauri 启动失败");
}
```

## 3. 事件桥：广播 → 前端

后台任务把 `DownloadManager` 的广播事件转发为 Tauri 事件（前端 `listen` 订阅）：

```rust
fn spawn_event_bridge(app: tauri::AppHandle, manager: Arc<DownloadManager>) {
    tauri::async_runtime::spawn(async move {
        let mut rx = manager.subscribe();
        loop {
            match rx.recv().await {
                Ok(ev) => { let _ = app.emit("download://event", ev); }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            }
        }
    });
}
```

> `DownloadEvent` 需实现 `Serialize` 才能通过 Tauri 序列化（本库事件自带 `Clone`；`Serialize` 可在 Tauri 项目内用 `#[derive(serde::Serialize)]` 包装，或后续给事件加 `serde` feature）。
