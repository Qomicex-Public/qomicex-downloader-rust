# qomicex-downloader

面向 Minecraft 启动器 / 桌面 / Android 应用的高性能 Rust 下载核心库（`crate-type = ["lib"]`）。

设计目标：**激进**（高并行、动态调整）+ **稳定**（重试、看门狗、断点续传、原子完成）。

## 特性

| 能力 | 说明 |
|------|------|
| 两级并发 | 全局任务并发上限（Worker 级）+ 任务内动态分段并发（段级，默认 ≤16） |
| 段大小导向切片 | >10MB 按 8-16MB 分片并行下载，小文件单请求直传 |
| 动态拆分 | 运行中周期性把剩余字节最多的段一分为二，充分利用带宽 |
| 自动重试 | 分片级指数退避 + 随机抖动；耗尽后镜像轮换、降级整文件流式重下 |
| 看门狗 | 无数据超时 / 持续龟速（低于平滑速度 ×0.3）自动重建段 |
| 断点续传 | `.part` 中间文件 + 段边界对齐，暂停后无缝续传 |
| 原子完成 | fsync + rename，杜绝半成品文件 |
| 三级进度 | 任务级 / 全局聚合 / 日志事件，节流上报，适配 Tauri IPC |
| 自定义请求头 | 全局默认 + 任务级覆盖（UA / Authorization 等） |
| 多平台 | Windows / macOS / Linux / Android（rustls 纯 Rust TLS，无 OpenSSL 依赖） |

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

## 安装

```toml
[dependencies]
qomicex-downloader = { path = "path/to/qomicex-downloader-rust" }
# 或 crates.io 发布后：
# qomicex-downloader = "0.1"
```

## API 一览

```rust
let manager = DownloadManager::new(DownloadOptions::default(), max_concurrent);

manager.add(task)          // 入队，返回 TaskId（同步）
manager.pause(id).await    // 暂停（.part 保留）
manager.resume(id).await   // 恢复（并发满则排队）
manager.cancel(id).await   // 取消（删除 .part）
manager.state(id).await    // 查询状态
manager.list().await       // 任务列表
manager.shutdown().await   // 优雅关闭
```

任务状态机：`Queued → Downloading → Completed / Paused ⇄ Downloading / Failed / Cancelled`

事件（`broadcast` 通道，容量 1024）：
- `Progress`：任务级，150ms 节流，含速度 / 活跃段数
- `GlobalProgress`：全局聚合，250ms
- `Log`：重试 / 拆分 / 看门狗 / 降级诊断
- `StateChanged`：状态变更（失败时携带错误详情）

## 文档

- [架构设计](docs/junsi-dev-docs/2-架构设计/下载核心架构.md)
- [API 规范](docs/junsi-dev-docs/3-API规范/DownloadManager-API.md)
- [Tauri 集成指南](docs/junsi-dev-docs/7-调用规范/Tauri集成指南.md)
- [Android 平台支持](docs/junsi-dev-docs/9-系统要求/Android平台支持.md)

## 开发

```powershell
cargo build                # 编译
cargo test                 # 全部测试（单元 + 集成 + doc）
cargo clippy --all-targets # lint（保持 0 警告）
cargo doc --no-deps        # 文档
```

测试覆盖：多段并发 / 小文件直传 / 无 Range 回退流式 / chunked / 分片失败重试 / 404 / 暂停续传 / 取消清理 / 队列并发上限 / 看门狗段重建 / 动态拆分 / 全局进度 / 镜像回退 / 自定义请求头（基于 `tests/common/mod.rs` 的 mock HTTP 服务器，支持 Range / flaky / stall / throttle 注入）。

## 平台注意

- **Windows**：只读句柄 `sync_all` 返回 ACCESS_DENIED（finalize 必须用读写句柄）；rename 目标存在需先删
- **Android**：纯 Rust 依赖栈，需 NDK 交叉编译（详见 Android 平台支持文档）

## 路线图

- [ ] 智能镜像选择：DNS 解析 → IP 粒度 EMA 测速 → 最优节点直连（需自定义 hyper connector）
- [ ] SHA-256 完整性校验
- [ ] 限速 / 调度策略

## License

[MIT](LICENSE)
