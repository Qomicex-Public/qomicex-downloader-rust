# API规范

> 生成时间：2026-08-06 23:31

# qomicex-downloader API 规范

`crate-type = ["lib"]`，异步 API，全部依赖 tokio 运行时（调用方自行创建）。

## 快速上手

```rust
use qomicex_downloader::{DownloadManager, DownloadOptions, DownloadTask, TaskState};
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let manager = DownloadManager::new(DownloadOptions::default(), 4);
    let mut rx = manager.subscribe();

    let id = manager.add(
        DownloadTask::new("https://example.com/asset.jar", PathBuf::from("./asset.jar"))
            .with_header("Authorization", "Bearer xxx")
            .with_mirrors(["https://mirror.example.com/asset.jar".to_string()]),
    );

    while let Ok(ev) = rx.recv().await {
        println!("{ev:?}");
        if matches!(manager.state(id).await, Ok(TaskState::Completed | TaskState::Failed)) {
            break;
        }
    }
    manager.shutdown().await;
}
```

## 类型

### `DownloadOptions`（全局默认，`Default` 实现）
| 字段 | 默认 | 说明 |
|------|------|------|
| `user_agent` | `qomicex-downloader/0.1.0` | 全局 UA |
| `headers` | `[]` | 全局默认请求头（任务级同名覆盖） |
| `timeout` | 60s | 单请求总超时 |
| `connect_timeout` | 15s | 连接超时 |
| `max_retries` | 5 | 分片级重试次数（耗尽后降级整文件） |
| `retry_base_delay` | 500ms | 重试退避基数（×2ⁿ + 抖动） |
| `segment_size` | 8MB | 分片大小 |
| `max_segments` | 16 | 任务内最大并发段数 |
| `split_threshold` | 10MB | 超过才分片，小文件直传 |
| `watchdog_idle_timeout` | 30s | 无数据判卡死 |
| `watchdog_slow_factor` | 0.3 | 龟速系数（低于平滑速度 ×0.3） |
| `watchdog_slow_samples` | 5 | 连续低速采样次数 |
| `split_sample_interval` | 2s | 动态拆分检查周期 |
| `progress_throttle` | 150ms | 任务级进度事件节流 |
| `global_progress_interval` | 250ms | 全局进度聚合周期 |

### `DownloadTask`（builder）
```rust
DownloadTask::new(url, dest)
    .with_header(name, value)      // 任务级头（覆盖全局）
    .with_mirrors([urls])          // 镜像 URL（探测失败/重试耗尽时轮换）
    .with_max_segments(n)
    .with_max_retries(n)
    .with_segment_size(n)
```

### `TaskState`
`Queued → Downloading → Completed / Paused ⇄ Downloading / Failed / Cancelled`

### `DownloadEvent`（三级进度，broadcast 通道容量 1024）
```rust
StateChanged { id, state, detail: Option<String> }
Progress { id, downloaded, total, speed_bps, active_segments }   // 节流 150ms
GlobalProgress { active_tasks, downloaded, total }                 // 聚合 250ms
Log { level: LogLevel, message }                                   // Debug/Info/Warn/Error
```

## `DownloadManager` API

| 方法 | 签名 | 说明 |
|------|------|------|
| `new` | `(DownloadOptions, max_concurrent: usize)` | 全局并发任务数上限 |
| `subscribe` | `() -> broadcast::Receiver<DownloadEvent>` | 订阅三级进度事件 |
| `add` | `(DownloadTask) -> TaskId` | 入队（同步返回，有空位立即启动） |
| `pause` | `async (id) -> Result<()>` | 暂停（.part 保留） |
| `resume` | `async (id) -> Result<()>` | 恢复（并发满则排队） |
| `cancel` | `async (id) -> Result<()>` | 取消（删除 .part） |
| `remove` | `async (id) -> Result<()>` | 取消并清理条目 |
| `state` | `async (id) -> Result<TaskState>` | 查询状态 |
| `list` | `async () -> Vec<(TaskId, TaskState)>` | 任务列表 |
| `shutdown` | `async ()` | 取消全部任务并等待退出 |

## 错误（`DownloadError`）

`Http` / `HttpStatus{status,url}` / `Io` / `TaskNotFound` / `Cancelled` / `Incomplete{expected,actual}` / `Exhausted` / `Other`

## 约定

- `add()` 为同步 API（短临界区 std 锁），可在任意线程调用
- 任务终态事件：`Completed` / `Failed(detail)` / `Cancelled`；暂停不终态
- `.part` 文件与目标同目录，命名 `<dest>.part`
- 线程安全：`DownloadManager` 可 `Arc` 共享跨线程


## 修订记录
| 日期 | 版本 | 修改内容 | 修改人 |
| 2026-08-06 | v1.0 | 初版创建 | AI Agent |