use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use futures_util::stream::StreamExt;
use rand::Rng;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::error::DownloadError;
use crate::segment::{build_segments, Segment};
use crate::task::{DownloadEvent, DownloadOptions, LogLevel};

/// 运行期统计（manager 聚合全局进度时读取）。
pub(crate) struct DownloadStats {
    pub downloaded: AtomicU64,
    pub total: AtomicU64,
    pub active_segments: AtomicU32,
}

/// 每段速度采样（用于动态拆分选最慢段）。
pub(crate) type SpeedMap = Arc<Mutex<HashMap<u32, (Instant, u64)>>>;

/// 任务运行上下文（由 manager 构造）。
pub(crate) struct RunContext {
    pub task: crate::task::DownloadTask,
    pub cancel: CancellationToken,
    pub events: Sender<DownloadEvent>,
    pub headers: reqwest::header::HeaderMap,
    /// 主 URL + 镜像，按顺序轮换。
    pub urls: StdMutex<Vec<String>>,
    pub options: Arc<DownloadOptions>,
    pub stats: Arc<DownloadStats>,
    pub speeds: SpeedMap,
}

impl RunContext {
    pub(crate) fn current_url(&self) -> String {
        self.urls.lock().unwrap()[0].clone()
    }

    /// 轮换到下一个镜像，返回新 URL（无镜像返回 None）。
    pub(crate) fn mirror_url(&self) -> Option<String> {
        let mut urls = self.urls.lock().unwrap();
        if urls.len() < 2 {
            return None;
        }
        let first = urls.remove(0);
        urls.push(first);
        Some(urls[0].clone())
    }

    pub(crate) fn log(&self, level: LogLevel, message: String) {
        let _ = self.events.send(DownloadEvent::Log { level, message });
    }
}

pub(crate) struct Engine {
    client: reqwest::Client,
}

struct ProbeResult {
    total: Option<u64>,
    range_ok: bool,
}

/// 段失败类别。
enum SegFailure {
    Cancelled,
    Exhausted(String),
}

type SegmentOutcome = (u32, Result<(), SegFailure>);

#[derive(Debug)]
enum SegError {
    Cancelled,
    Retryable(String),
}

impl Engine {
    pub(crate) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub(crate) async fn run(&self, ctx: &Arc<RunContext>) -> Result<(), DownloadError> {
        let mut attempts = 0;
        loop {
            let result = self.run_once(ctx).await;
            if let Err(DownloadError::ChecksumMismatch { .. }) = result {
                // 校验失败的文件内容不可信：始终清理 .part（重下前 + 最终失败）
                let part = ctx.task.part_path();
                let _ = tokio::fs::remove_file(&part).await;
                if attempts == 0 {
                    attempts += 1;
                    ctx.log(
                        LogLevel::Warn,
                        "SHA-256 校验失败，删除 .part 自动重下".to_string(),
                    );
                    continue;
                }
            }
            return result;
        }
    }

    async fn run_once(&self, ctx: &Arc<RunContext>) -> Result<(), DownloadError> {
        let part = ctx.task.part_path();
        let probe = self.probe(ctx).await?;
        ctx.log(
            LogLevel::Info,
            format!("探测完成: total={:?}, range={}", probe.total, probe.range_ok),
        );
        match probe.total {
            Some(t) if t > 0 => {
                ctx.stats.total.store(t, Ordering::Relaxed);
                if probe.range_ok {
                    return self.run_ranged(ctx, &part, t).await;
                }
                self.run_streamed(ctx, &part).await
            }
            _ => {
                ctx.stats.total.store(0, Ordering::Relaxed);
                self.run_streamed(ctx, &part).await
            }
        }
    }

    // ---------------- 探测 ----------------

    async fn probe(&self, ctx: &Arc<RunContext>) -> Result<ProbeResult, DownloadError> {
        let mut last_err: Option<DownloadError> = None;
        let url_count = ctx.urls.lock().unwrap().len();
        for _ in 0..url_count {
            let url = ctx.current_url();
            match self.probe_one(ctx, &url).await {
                Ok(p) => return Ok(p),
                Err(e) => {
                    ctx.log(LogLevel::Warn, format!("探测 {url} 失败: {e}"));
                    last_err = Some(e);
                    if ctx.mirror_url().is_none() {
                        break;
                    }
                }
            }
        }
        Err(last_err.unwrap_or(DownloadError::Exhausted(
            "无可用 URL".into(),
        )))
    }

    async fn probe_one(
        &self,
        ctx: &Arc<RunContext>,
        url: &str,
    ) -> Result<ProbeResult, DownloadError> {
        let timeout = ctx.options.timeout;
        let head = self
            .client
            .head(url)
            .headers(ctx.headers.clone())
            .timeout(timeout)
            .send()
            .await;
        if let Ok(resp) = head {
            let status = resp.status();
            if status.is_success() {
                return Ok(ProbeResult {
                    total: content_length(resp.headers()),
                    range_ok: accept_ranges_bytes(resp.headers()),
                });
            }
            if status.is_server_error() {
                return Err(DownloadError::HttpStatus {
                    status: status.as_u16(),
                    url: url.to_string(),
                });
            }
        }
        let resp = self
            .client
            .get(url)
            .headers(ctx.headers.clone())
            .header(reqwest::header::RANGE, "bytes=0-0")
            .timeout(timeout)
            .send()
            .await?;
        let status = resp.status();
        if status == reqwest::StatusCode::PARTIAL_CONTENT {
            Ok(ProbeResult {
                total: content_range_total(resp.headers()),
                range_ok: true,
            })
        } else if status.is_success() {
            Ok(ProbeResult {
                total: content_length(resp.headers()),
                range_ok: false,
            })
        } else {
            Err(DownloadError::HttpStatus {
                status: status.as_u16(),
                url: url.to_string(),
            })
        }
    }

    // ---------------- Range 多段路径 ----------------

    async fn run_ranged(
        &self,
        ctx: &Arc<RunContext>,
        part: &Path,
        total: u64,
    ) -> Result<(), DownloadError> {
        let opts = &ctx.options;
        let mut segments: HashMap<u32, Segment> = build_segments(
            total,
            opts.segment_size,
            opts.max_segments,
            opts.split_threshold,
        )
        .into_iter()
        .map(|s| (s.index, s))
        .collect();

        if let Ok(meta) = tokio::fs::metadata(part).await {
            let part_size = meta.len();
            if part_size > total {
                tokio::fs::remove_file(part).await?;
            } else if part_size > 0 {
                for s in segments.values_mut() {
                    s.align_to_part(part_size);
                }
                ctx.log(
                    LogLevel::Info,
                    format!("断点续传: 已存在 .part {part_size} 字节"),
                );
            }
        }
        if let Some(parent) = part.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| DownloadError::Other(format!("创建目录 {parent:?} 失败: {e}")))?;
        }

        let mut set: JoinSet<SegmentOutcome> = JoinSet::new();
        let mut seg_handles: HashMap<u32, AbortHandle> = HashMap::new();
        let seg_counter = Arc::new(AtomicU32::new(10_000));
        let max_segments = ctx.task.max_segments.unwrap_or(opts.max_segments) as usize;

        let reporter = spawn_progress_reporter(ctx);

        let mut degraded = false;
        loop {
            // 启动待运行段（受并发上限约束）
            if !degraded {
                let mut pending: Vec<Segment> = segments
                    .values()
                    .filter(|s| !s.finished() && !seg_handles.contains_key(&s.index))
                    .cloned()
                    .collect();
                pending.sort_by_key(|s| s.index);
                for seg in pending {
                    if seg_handles.len() >= max_segments {
                        break;
                    }
                    let idx = seg.index;
                    ctx.stats.active_segments.fetch_add(1, Ordering::Relaxed);
                    let ah = set.spawn(run_segment(
                        self.client.clone(),
                        ctx.clone(),
                        seg,
                    ));
                    seg_handles.insert(idx, ah);
                }
            }

            let all_finished = segments.values().all(|s| s.finished());
            if all_finished && seg_handles.is_empty() {
                break;
            }
            if seg_handles.is_empty() && !all_finished {
                // 某段永久失败且无活跃段 → 降级整文件重试
                degraded = true;
                ctx.log(LogLevel::Warn, "分片重试耗尽，降级整文件重试".to_string());
                break;
            }

            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    for h in seg_handles.values() { h.abort(); }
                    reporter.abort();
                    return Err(DownloadError::Cancelled);
                }
                out = set.join_next() => {
                    match out {
                        Some(Ok((idx, result))) => {
                            ctx.stats.active_segments.fetch_sub(1, Ordering::Relaxed);
                            seg_handles.remove(&idx);
                            match result {
                                Ok(()) => { segments.remove(&idx); }
                                Err(SegFailure::Cancelled) => {}
                                Err(SegFailure::Exhausted(reason)) => {
                                    ctx.log(LogLevel::Error, format!("段 {idx} 重试耗尽: {reason}"));
                                    degraded = true;
                                }
                            }
                        }
                        Some(Err(je)) => {
                            if je.is_cancelled() {
                                continue; // 被动态拆分 abort，忽略
                            }
                            ctx.log(LogLevel::Error, format!("段任务异常: {je}"));
                            degraded = true;
                        }
                        None => {}
                    }
                }
                _ = sleep(opts.split_sample_interval) => {
                    if degraded { continue; }
                    if seg_handles.len() < max_segments {
                        try_split(ctx, &mut segments, &mut seg_handles, seg_counter.clone()).await;
                    }
                }
            }
        }
        reporter.abort();

        if degraded {
            return self.download_full_fallback(ctx, part, total).await;
        }

        let actual = tokio::fs::metadata(part).await?.len();
        if actual != total {
            return Err(DownloadError::Incomplete { expected: total, actual });
        }
        finalize_verified(part, &ctx.task.dest, ctx.task.sha256).await?;
        Ok(())
    }

    /// 整文件降级：分片耗尽后删除 .part，转流式顺序重下（服务器忽略 Range 时段请求必然失败）。
    async fn download_full_fallback(
        &self,
        ctx: &Arc<RunContext>,
        part: &Path,
        _total: u64,
    ) -> Result<(), DownloadError> {
        let _ = tokio::fs::remove_file(part).await;
        self.run_streamed(ctx, part).await
    }

    // ---------------- 流式路径（服务器不支持 Range） ----------------

    async fn run_streamed(
        &self,
        ctx: &Arc<RunContext>,
        part: &Path,
    ) -> Result<(), DownloadError> {
        if part.exists() {
            tokio::fs::remove_file(part).await?;
        }
        if let Some(parent) = part.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        ctx.log(LogLevel::Info, format!("流式路径开始: {part:?}"));

        let opts = &ctx.options;
        let max_retries = ctx.task.max_retries.unwrap_or(opts.max_retries);
        let mut attempt: u32 = 0;
        let mut url = ctx.current_url();
        loop {
            if ctx.cancel.is_cancelled() {
                return Err(DownloadError::Cancelled);
            }
            // 每次尝试前清空 .part（流式无断点续传，防止残留叠加）
            let _ = tokio::fs::remove_file(part).await;
            match try_streamed_once(self.client.clone(), ctx, part, &url).await {
                Ok(()) => return finalize_verified(part, &ctx.task.dest, ctx.task.sha256).await,
                Err(DownloadError::Cancelled) => return Err(DownloadError::Cancelled),
                Err(e) => {
                    attempt += 1;
                    if attempt > max_retries {
                        match ctx.mirror_url() {
                            Some(next) => {
                                attempt = 0;
                                url = next;
                                ctx.log(LogLevel::Warn, format!("流式重试耗尽，切换镜像 {url}"));
                                continue;
                            }
                            None => return Err(e),
                        }
                    }
                    let delay = backoff(opts, attempt);
                    ctx.log(LogLevel::Warn, format!("流式下载第 {attempt} 次重试（{e}）"));
                    tokio::select! {
                        _ = ctx.cancel.cancelled() => return Err(DownloadError::Cancelled),
                        _ = sleep(delay) => {}
                    }
                }
            }
        }
    }
}

// ================= 单段执行 =================

async fn run_segment(
    client: reqwest::Client,
    ctx: Arc<RunContext>,
    mut seg: Segment,
) -> SegmentOutcome {
    let idx = seg.index;
    let max_retries = ctx.task.max_retries.unwrap_or(ctx.options.max_retries);
    let mut attempt: u32 = 0;
    let mut attempt_url = ctx.current_url();
    loop {
        if ctx.cancel.is_cancelled() {
            return (idx, Err(SegFailure::Cancelled));
        }
        match try_segment_once(&client, &ctx, &mut seg, &attempt_url).await {
            Ok(()) => return (idx, Ok(())),
            Err(SegError::Cancelled) => return (idx, Err(SegFailure::Cancelled)),
            Err(SegError::Retryable(reason)) => {
                attempt += 1;
                if attempt > max_retries {
                    match ctx.mirror_url() {
                        Some(next) => {
                            attempt = 0;
                            attempt_url = next;
                            ctx.log(LogLevel::Warn, format!("段 {idx} 重试耗尽，切换镜像 {attempt_url}"));
                            continue;
                        }
                        None => return (idx, Err(SegFailure::Exhausted(reason))),
                    }
                }
                let delay = backoff(&ctx.options, attempt);
                ctx.log(LogLevel::Warn, format!("段 {idx} 第 {attempt} 次重试（{reason}）"));
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return (idx, Err(SegFailure::Cancelled)),
                    _ = sleep(delay) => {}
                }
            }
        }
    }
}

/// 单段单次尝试：Range 请求 + 流式写盘 + 看门狗。
async fn try_segment_once(
    client: &reqwest::Client,
    ctx: &Arc<RunContext>,
    seg: &mut Segment,
    url: &str,
) -> Result<(), SegError> {
    let from = seg.start + seg.downloaded;
    if from > seg.end {
        return Ok(());
    }
    let range = format!("bytes={from}-{}", seg.end);
    let resp = client
        .get(url)
        .headers(ctx.headers.clone())
        .header(reqwest::header::RANGE, range)
        .timeout(ctx.options.timeout)
        .send()
        .await
        .map_err(|e| SegError::Retryable(e.to_string()))?;
    let status = resp.status();
    if status == reqwest::StatusCode::OK {
        // 200 = 全文件响应：仅当段本身覆盖整个文件且从头开始时合法（服务器对整文件 Range 返回 200）
        let total = ctx.stats.total.load(Ordering::Relaxed);
        let whole_file_single = seg.start == 0 && seg.end + 1 == total && seg.downloaded == 0;
        if !whole_file_single {
            return Err(SegError::Retryable("服务器忽略 Range 请求".into()));
        }
    }
    if !status.is_success() {
        return Err(SegError::Retryable(format!("HTTP {status} ({url})")));
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false) // 断点续传：不截断已有 .part
        .open(&ctx.task.part_path())
        .await
        .map_err(|e| SegError::Retryable(format!("打开 .part {:?} 失败: {e}", ctx.task.part_path())))?;
    file.seek(tokio::io::SeekFrom::Start(from))
        .await
        .map_err(|e| SegError::Retryable(e.to_string()))?;

    let mut stream = resp.bytes_stream();
    let idle_timeout = ctx.options.watchdog_idle_timeout;
    let slow_factor = ctx.options.watchdog_slow_factor;
    let slow_samples = ctx.options.watchdog_slow_samples;
    let mut ema: f64 = 0.0;
    let mut slow_streak: u32 = 0;
    let mut last_activity = Instant::now();
    let mut prev_tick_bytes = seg.downloaded;

    loop {
        tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(SegError::Cancelled),
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        file.write_all(&bytes).await
                            .map_err(|e| SegError::Retryable(e.to_string()))?;
                        seg.downloaded += bytes.len() as u64;
                        ctx.stats.downloaded.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        last_activity = Instant::now();
                        let mut sp = ctx.speeds.lock().await;
                        sp.insert(seg.index, (Instant::now(), seg.downloaded));
                    }
                    Some(Err(e)) => {
                        return Err(SegError::Retryable(format!("网络中断: {e}")));
                    }
                    None => {
                        file.sync_data().await
                            .map_err(|e| SegError::Retryable(e.to_string()))?;
                        if seg.finished() {
                            return Ok(());
                        }
                        return Err(SegError::Retryable(format!(
                            "流提前结束: 已收 {} / 期望 {}",
                            seg.downloaded,
                            seg.len()
                        )));
                    }
                }
            }
            _ = sleep(Duration::from_secs(1)) => {
                if last_activity.elapsed() > idle_timeout {
                    return Err(SegError::Retryable("看门狗: 无数据超过阈值".into()));
                }
                let inst = (seg.downloaded.saturating_sub(prev_tick_bytes)) as f64;
                prev_tick_bytes = seg.downloaded;
                if ema == 0.0 {
                    ema = inst;
                } else {
                    ema = ema * 0.8 + inst * 0.2;
                }
                if ema > 1024.0 && inst < ema * slow_factor {
                    slow_streak += 1;
                    if slow_streak >= slow_samples {
                        return Err(SegError::Retryable(
                            "看门狗: 速度持续低于平滑速度".into(),
                        ));
                    }
                } else {
                    slow_streak = 0;
                }
            }
        }
    }
}

// ================= 流式单次尝试 =================

async fn try_streamed_once(
    client: reqwest::Client,
    ctx: &Arc<RunContext>,
    part: &Path,
    url: &str,
) -> Result<(), DownloadError> {
    let resp = client
        .get(url)
        .headers(ctx.headers.clone())
        .timeout(ctx.options.timeout)
        .send()
        .await?;
    let status = resp.status();
    ctx.log(LogLevel::Debug, format!("流式响应: {status}"));
    if !status.is_success() {
        return Err(DownloadError::HttpStatus {
            status: status.as_u16(),
            url: url.to_string(),
        });
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false) // 循环内每次尝试前删除文件，避免残留叠加
        .open(part)
        .await?;
    let mut stream = resp.bytes_stream();
    let idle_timeout = ctx.options.watchdog_idle_timeout;
    let mut last_activity = Instant::now();
    loop {
        tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(DownloadError::Cancelled),
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        file.write_all(&bytes).await?;
                        ctx.stats.downloaded.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        last_activity = Instant::now();
                    }
                    Some(Err(e)) => return Err(DownloadError::Http(e)),
                    None => {
                        file.sync_all().await?;
                        // 流式 EOF 校验：total 已知时必须字节数一致（chunked 截断兜底）
                        let total = ctx.stats.total.load(Ordering::Relaxed);
                        let downloaded = ctx.stats.downloaded.load(Ordering::Relaxed);
                        if total > 0 && downloaded != total {
                            return Err(DownloadError::Incomplete { expected: total, actual: downloaded });
                        }
                        return Ok(());
                    }
                }
            }
            _ = sleep(Duration::from_secs(1)) => {
                if last_activity.elapsed() > idle_timeout {
                    return Err(DownloadError::Exhausted(
                        "看门狗: 流式下载无数据超过阈值".into(),
                    ));
                }
            }
        }
    }
}

// ================= 动态拆分 =================

/// 动态拆分：周期性检查，把剩余字节最多的活跃段一分为二（并行度提升）。
/// 下限 64KB 避免拆分碎片；段任务被 abort 后已写字节保留，新段从拆分点继续。
async fn try_split(
    ctx: &Arc<RunContext>,
    segments: &mut HashMap<u32, Segment>,
    seg_handles: &mut HashMap<u32, AbortHandle>,
    seg_counter: Arc<AtomicU32>,
) {
    const MIN_SPLIT_REMAINING: u64 = 64 * 1024;
    let target = segments
        .values()
        .filter(|s| seg_handles.contains_key(&s.index) && !s.finished())
        .max_by_key(|s| s.len().saturating_sub(s.downloaded))
        .cloned();
    let Some(seg) = target else {
        return;
    };
    if seg.len().saturating_sub(seg.downloaded) < MIN_SPLIT_REMAINING {
        return;
    }
    segments.remove(&seg.index);
    if let Some(h) = seg_handles.remove(&seg.index) {
        h.abort();
    }
    let next = seg_counter.fetch_add(1, Ordering::Relaxed);
    let (a, b) = seg.split(next);
    segments.insert(a.index, a);
    segments.insert(b.index, b);
    ctx.log(
        LogLevel::Info,
        format!("动态拆分: 段 {} → 2 段", seg.index),
    );
}

// ================= 进度上报 =================

fn spawn_progress_reporter(ctx: &Arc<RunContext>) -> JoinHandle<()> {
    let ctx = ctx.clone();
    tokio::spawn(async move {
        let throttle = ctx.options.progress_throttle;
        let mut last = (Instant::now(), 0u64);
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => break,
                _ = sleep(throttle) => {}
            }
            let downloaded = ctx.stats.downloaded.load(Ordering::Relaxed);
            let elapsed = last.0.elapsed().as_secs_f64();
            let speed_bps = if elapsed > 0.0 {
                ((downloaded.saturating_sub(last.1)) as f64 / elapsed).round() as u64
            } else {
                0
            };
            let _ = ctx.events.send(DownloadEvent::Progress {
                id: ctx.task.id,
                downloaded,
                total: ctx.stats.total.load(Ordering::Relaxed),
                speed_bps,
                active_segments: ctx.stats.active_segments.load(Ordering::Relaxed),
            });
            last = (Instant::now(), downloaded);
        }
    })
}

// ================= 辅助 =================

fn backoff(opts: &DownloadOptions, attempt: u32) -> Duration {
    let base = opts.retry_base_delay.as_millis().max(1) as u64;
    let exp = base * (1u64 << attempt.min(6));
    Duration::from_millis(exp + rand::rng().random_range(0..=250))
}

fn content_length(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

fn accept_ranges_bytes(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().contains("bytes"))
        .unwrap_or(false)
}

fn content_range_total(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let v = headers.get(reqwest::header::CONTENT_RANGE)?.to_str().ok()?;
    let total = v.rsplit('/').next()?;
    total.parse().ok()
}

/// 原子完成：fsync + rename（目标已存在时先删，兼容 Windows）。
/// 注意：Windows 上只读句柄调用 sync_all 返回 ACCESS_DENIED，必须用写句柄。
async fn finalize_part(part: &Path, dest: &Path) -> Result<(), DownloadError> {
    let f = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(part)
        .await
        .map_err(|e| DownloadError::Other(format!("finalize 打开 {part:?} 失败: {e}")))?;
    f.sync_all().await.map_err(|e| {
        DownloadError::Other(format!("finalize fsync {part:?} 失败: {e}"))
    })?;
    drop(f);
    match tokio::fs::rename(part, dest).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = tokio::fs::remove_file(dest).await;
            tokio::fs::rename(part, dest).await.map_err(|e2| {
                DownloadError::Other(format!(
                    "rename {part:?} -> {dest:?} 失败: {e} / {e2}"
                ))
            })
        }
    }
}

/// finalize + 可选 SHA-256 校验（校验失败返回 `ChecksumMismatch`，由 run() 自动重下一次）。
async fn finalize_verified(
    part: &Path,
    dest: &Path,
    sha256: Option<[u8; 32]>,
) -> Result<(), DownloadError> {
    if let Some(expected) = sha256 {
        let actual = sha256_file(part).await?;
        if actual != expected {
            return Err(DownloadError::ChecksumMismatch {
                expected: hex_encode(&expected),
                actual: hex_encode(&actual),
            });
        }
    }
    finalize_part(part, dest).await
}

/// 流式计算文件 SHA-256（分块读取，内存友好）。
async fn sha256_file(path: &Path) -> Result<[u8; 32], DownloadError> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
