//! # qomicex-downloader
//!
//! 面向 Minecraft 启动器/桌面应用的高性能下载核心库。
//!
//! ## 特性
//!
//! - **两级并发**：全局任务并发上限（worker 级）+ 任务内动态分段并发（段级）
//! - **段大小导向切片**：> `split_threshold` 的文件按 8-16MB 分片并行下载，小文件单请求直传
//! - **动态段拆分**：运行中监测速度，连续提速自动拆分最慢段（上限 `max_segments`）
//! - **自动重试**：分片级指数退避 + 抖动；耗尽后降级整文件重试；镜像 URL 自动轮换
//! - **看门狗**：无数据超时 / 持续龟速自动重建段
//! - **断点续传**：`.part` 中间文件 + 段边界对齐，暂停后无缝续传
//! - **原子完成**：fsync + rename，杜绝半成品文件
//! - **三级进度**：任务级 / 全局聚合 / 日志事件，节流上报，适配 Tauri IPC
//! - **连接层调优**：HTTP/2 adaptive window + 大帧 + TCP keepalive + 连接池上限，
//!   HTTP/3（QUIC）可选启用
//!
//! ## HTTP/3（可选）
//!
//! HTTP/3 走 reqwest 的 **unstable** 特性，需两步启用（默认关闭）：
//!
//! 1. 开启 Cargo feature：`cargo build --features http3`
//! 2. 编译期传 unstable 标志：`RUSTFLAGS="--cfg reqwest_unstable"`
//!
//! 运行时再通过 `DownloadOptions::enable_http3 = true` 指定本实例用 HTTP/3-only
//! 连接（失败自动回退 HTTP/2）。因涉及 quinn/QUIC 重依赖，不建议在 Android
//! 交叉编译等跨平台场景默认开启。
//!
//! ## 平台兼容
//!
//! 纯 Rust 依赖栈（rustls TLS，无 OpenSSL/native-tls），支持 Windows / macOS / Linux /
//! Android（`aarch64-linux-android` 等 target，需 NDK 交叉编译）。
//!
//! ## 快速上手
//!
//! ```
//! use qomicex_downloader::{DownloadManager, DownloadOptions, DownloadTask};
//! use std::path::PathBuf;
//!
//! # fn main() { /* 完整示例见 tests/ */ }
//! ```
//!
//! ```no_run
//! use qomicex_downloader::{DownloadManager, DownloadOptions, DownloadTask, TaskState};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() {
//!     let manager = DownloadManager::new(DownloadOptions::default(), 4);
//!     let mut rx = manager.subscribe();
//!
//!     let id = manager.add(DownloadTask::new(
//!         "https://example.com/asset.jar",
//!         PathBuf::from("./downloads/asset.jar"),
//!     ));
//!
//!     while let Ok(ev) = rx.recv().await {
//!         // 消费三级进度事件：Progress / GlobalProgress / Log
//!         println!("{:?}", ev);
//!         if matches!(manager.state(id).await, Ok(TaskState::Completed | TaskState::Failed)) {
//!             break;
//!         }
//!     }
//! }
//! ```

pub mod error;
pub mod jni;
pub mod manager;
pub mod task;

mod engine;
mod segment;

pub use error::DownloadError;
pub use manager::{DownloadManager, DownloadProgress};
pub use task::{
    DownloadEvent, DownloadOptions, DownloadTask, LogLevel, TaskId, TaskState,
};
