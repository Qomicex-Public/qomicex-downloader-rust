use std::path::PathBuf;
use std::time::Duration;

/// 任务 ID。
pub type TaskId = u64;

/// 任务状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskState {
    /// 已加入队列，等待并发名额。
    Queued,
    /// 下载中。
    Downloading,
    /// 已暂停（.part 保留，可续传）。
    Paused,
    /// 已完成（.part 已原子重命名为目标文件）。
    Completed,
    /// 失败（重试/镜像耗尽）。
    Failed,
    /// 已取消（.part 已删除）。
    Cancelled,
}

/// 日志级别（用于 `DownloadEvent::Log`）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// 三级进度事件之一。
#[derive(Clone, Debug)]
pub enum DownloadEvent {
    /// 任务状态变更。
    StateChanged {
        id: TaskId,
        state: TaskState,
        detail: Option<String>,
    },
    /// 任务级进度（已节流，默认 150ms 合并）。
    Progress {
        id: TaskId,
        downloaded: u64,
        total: u64,
        /// 字节/秒（节流窗口内平均）。
        speed_bps: u64,
        active_segments: u32,
    },
    /// 全局聚合进度（manager 周期性汇总所有任务）。
    GlobalProgress {
        active_tasks: u32,
        downloaded: u64,
        total: u64,
    },
    /// 日志事件（重试、看门狗重建、降级等）。
    Log { level: LogLevel, message: String },
}

/// 全局默认配置，由 `DownloadManager::new` 传入。
#[derive(Clone, Debug)]
pub struct DownloadOptions {
    /// 全局默认 User-Agent。
    pub user_agent: String,
    /// 全局默认请求头（任务级同名头可覆盖）。
    pub headers: Vec<(String, String)>,
    /// 单次请求总超时。
    pub timeout: Duration,
    /// 连接建立超时。
    pub connect_timeout: Duration,
    /// 分片级最大重试次数（耗尽后降级整文件重试）。
    pub max_retries: u32,
    /// 重试基础退避（实际为 base × 2^n + 随机抖动）。
    pub retry_base_delay: Duration,
    /// 分片大小（总大小超过 `split_threshold` 时按此分片）。
    pub segment_size: u64,
    /// 任务内最大并发段数（动态拆分的上限）。
    pub max_segments: u32,
    /// 超过该大小才分片；小文件单请求直传。
    pub split_threshold: u64,
    /// 看门狗：段内无任何数据到达超过该时长 → 重建段。
    pub watchdog_idle_timeout: Duration,
    /// 看门狗：当前速度持续低于平滑速度 × 该系数 → 重建段。
    pub watchdog_slow_factor: f64,
    /// 看门狗：连续多少次低速采样触发重建。
    pub watchdog_slow_samples: u32,
    /// 动态拆分：速度采样周期。
    pub split_sample_interval: Duration,
    /// 进度事件节流周期。
    pub progress_throttle: Duration,
    /// 全局进度聚合周期。
    pub global_progress_interval: Duration,
    /// 启用 HTTP/3（QUIC）优先连接。仅当编译期启用了 `http3` Cargo feature 时生效；
    /// 未启用该 feature 时此开关被忽略并回退 HTTP/2。默认 false。
    pub enable_http3: bool,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            user_agent: "qomicex-downloader/0.1.0".to_string(),
            headers: Vec::new(),
            timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(15),
            max_retries: 5,
            retry_base_delay: Duration::from_millis(500),
            segment_size: 8 * 1024 * 1024,
            max_segments: 16,
            split_threshold: 10 * 1024 * 1024,
            watchdog_idle_timeout: Duration::from_secs(30),
            watchdog_slow_factor: 0.3,
            watchdog_slow_samples: 5,
            split_sample_interval: Duration::from_secs(2),
            progress_throttle: Duration::from_millis(150),
            global_progress_interval: Duration::from_millis(250),
            enable_http3: false,
        }
    }
}

/// 单个下载任务。`url` 与 `dest` 为必填，其余字段可选覆盖全局默认。
#[derive(Clone, Debug)]
pub struct DownloadTask {
    /// 下载源 URL。
    pub url: String,
    /// 目标文件路径（最终文件名；中间态写入 `dest` 同级 `.part` 文件）。
    pub dest: PathBuf,
    /// 任务级请求头，同名覆盖全局默认。
    pub headers: Vec<(String, String)>,
    /// 镜像 URL 列表（重试耗尽后按顺序轮换；二期完整镜像测速在此扩展）。
    pub mirror_urls: Vec<String>,
    /// 覆盖 `DownloadOptions::max_segments`。
    pub max_segments: Option<u32>,
    /// 覆盖 `DownloadOptions::max_retries`。
    pub max_retries: Option<u32>,
    /// 覆盖 `DownloadOptions::segment_size`。
    pub segment_size: Option<u64>,
    /// 可选 SHA-256 校验和（下载完成后验证，不匹配自动重下一次）。
    pub sha256: Option<[u8; 32]>,
    /// 内部任务 ID（由 manager 分配）。
    pub(crate) id: TaskId,
}

impl DownloadTask {
    /// 创建任务（其余字段走全局默认）。
    pub fn new(url: impl Into<String>, dest: impl Into<PathBuf>) -> Self {
        Self {
            url: url.into(),
            dest: dest.into(),
            headers: Vec::new(),
            mirror_urls: Vec::new(),
            max_segments: None,
            max_retries: None,
            segment_size: None,
            sha256: None,
            id: 0,
        }
    }

    /// 追加任务级请求头。
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// 设置镜像 URL 列表。
    pub fn with_mirrors(mut self, urls: impl IntoIterator<Item = String>) -> Self {
        self.mirror_urls = urls.into_iter().collect();
        self
    }

    /// 覆盖最大并发段数。
    pub fn with_max_segments(mut self, n: u32) -> Self {
        self.max_segments = Some(n);
        self
    }

    /// 覆盖分片级最大重试次数。
    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = Some(n);
        self
    }

    /// 覆盖分片大小。
    pub fn with_segment_size(mut self, n: u64) -> Self {
        self.segment_size = Some(n);
        self
    }

    /// 设置 SHA-256 校验和（原始字节）。
    pub fn with_sha256(mut self, digest: [u8; 32]) -> Self {
        self.sha256 = Some(digest);
        self
    }

    /// 设置 SHA-256 校验和（64 位十六进制字符串，无效则忽略）。
    pub fn with_sha256_hex(mut self, hex: &str) -> Self {
        self.sha256 = parse_hex_sha256(hex);
        self
    }

    /// `.part` 中间文件路径（与目标同目录，文件名追加 `.part`）。
    pub(crate) fn part_path(&self) -> PathBuf {
        let name = self
            .dest
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "download".to_string());
        self.dest.with_file_name(format!("{name}.part"))
    }
}

/// 解析 64 字符十六进制 SHA-256（无效返回 None）。
pub(crate) fn parse_hex_sha256(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}
