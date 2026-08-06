use std::io;

use thiserror::Error;

/// 下载核心错误类型。
#[derive(Debug, Error)]
pub enum DownloadError {
    /// HTTP 请求层错误（连接失败、超时、TLS 等）。
    #[error("HTTP 请求失败: {0}")]
    Http(#[from] reqwest::Error),

    /// 服务器返回了非成功状态码。
    #[error("服务器返回错误状态 {status} (url: {url})")]
    HttpStatus { status: u16, url: String },

    /// 文件系统错误。
    #[error("IO 错误: {0}")]
    Io(#[from] io::Error),

    /// 任务 ID 不存在。
    #[error("任务不存在: {0}")]
    TaskNotFound(u64),

    /// 下载被取消（暂停/取消/关闭时由取消令牌触发）。
    #[error("下载已取消")]
    Cancelled,

    /// 下载数据不完整（连接中断且重试后仍不完整）。
    #[error("下载数据不完整: 期望 {expected} 字节, 实际 {actual} 字节")]
    Incomplete { expected: u64, actual: u64 },

    /// SHA-256 校验失败（自动重下一次后仍失败）。
    #[error("SHA-256 校验失败: 期望 {expected}, 实际 {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    /// 所有重试/镜像均已耗尽。
    #[error("所有重试均已耗尽: {0}")]
    Exhausted(String),

    /// 其他错误（状态机非法转移等）。
    #[error("{0}")]
    Other(String),
}
