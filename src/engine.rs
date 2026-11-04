// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use futures_util::stream::StreamExt;
use rand::Rng;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::broadcast::Sender;
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

/// 任务运行上下文（由 manager 构造）。
pub(crate) struct RunContext {
    /// 全局段许可（跨任务共享），限制聚合连接数（issue #5）。
    pub segment_sem: Arc<tokio::sync::Semaphore>,
    /// 全局限速（None = 不限速），跨任务共享。
    pub limiter: Option<Arc<crate::rate_limit::RateLimiter>>,
    pub task: crate::task::DownloadTask,
    pub cancel: CancellationToken,
    pub events: Sender<DownloadEvent>,
    pub headers: reqwest::header::HeaderMap,
    /// 主 URL + 镜像，按顺序轮换。
    pub urls: StdMutex<Vec<String>>,
    /// 探测阶段跟随重定向后落地的真实 URL（本次尝试有效）。
    ///
    /// 某些下载源是纯重定向器：对普通 GET 返回 302，但对带 `Range` 的请求直接报错
    /// （CurseForge 的 edge.forgecdn.net → mediafilez.forgecdn.net 就是这样，
    /// 带 Range 会拿到 CloudFront 的 404）。探测走的是 HEAD/重定向，看到的是落地
    /// 主机的 `Accept-Ranges`，于是判定支持分段；若随后仍拿原始 URL 发 Range 请求，
    /// 每一段都会失败、重试耗尽、最后降级整文件——表现为进度长时间卡 0% 再瞬间 100%。
    /// 所以这里把探测解析到的 URL 记下来，真正传输时用它。
    pub resolved_url: StdMutex<Option<String>>,
    pub options: Arc<DownloadOptions>,
    pub stats: Arc<DownloadStats>,
    /// HTTP/2 客户端（始终可用，回退目标）。
    pub h2: reqwest::Client,
    /// HTTP/1.1 并行连接客户端（始终可用）：每个文件独立 TCP 连接，用于「按连接限速」
    /// 的 CDN（如 Modrinth）。分段传输（run_ranged）一律走它；流式路径按 [`use_h1`] 决定。
    pub h1: reqwest::Client,
    /// HTTP/3 客户端（仅在编译期 feature + 运行时开关同时启用时存在）。
    pub h3: Option<reqwest::Client>,
    /// 当前是否使用 HTTP/1.1 并行（本任务已按来源路由定案，不动态翻转）。
    pub use_h1: AtomicBool,
    /// 当前是否使用 HTTP/3 优先。开启后在首个连接类失败时原子翻转为 false 回退 HTTP/2。
    pub use_h3: AtomicBool,
    /// 按 (scheme, host, port) 缓存「是否支持 Range」，带 TTL 与连续失败计数。
    /// 跨任务共享，避免同主机大量小文件逐文件 HEAD 探测。
    pub host_probe: Arc<std::sync::RwLock<HashMap<String, RangeHealth>>>,
    /// 已知不可用/挂起的宿主（host → 发现时刻）。跨任务共享：一个源在某文件上
    /// 一个字节都没给出来，后续文件直接把它排到最后，不必每个文件都再等一轮
    /// 看门狗。TTL 到期自动恢复尝试，避免源只是短暂抖动就被永久拉黑。
    pub dead_hosts: Arc<StdMutex<HashMap<String, Instant>>>,
    /// IP 级健康表（下载器内部实现，启动器不感知）。代理生效时为 None，
    /// 此时只保留域名级拉黑。
    pub ip_health: Option<Arc<crate::dns::IpHealth>>,
    /// 本次尝试是否已经因「文件够大且支持 Range」切过分片。
    ///
    /// 防止 `run_ranged` 降级整文件 → `run_streamed` 再嗅探 → 又切回分片
    /// 的死循环。每次 `run_once` 重置。
    pub ranged_attempted: AtomicBool,
    /// 本任务最近失败原因（环形，最多 8 条）。任务终态时由 manager 附到
    /// `StateChanged.detail`，避免广播事件被 Lagged 吞掉后用户只看到「下载失败」。
    pub recent_errors: StdMutex<VecDeque<String>>,
    /// 本任务各段最近一次 1s 采样的瞬时速度（bps）。L1 软重启以同批活跃段
    /// 中位数为基准，而不是该段自身历史峰值。
    pub segment_speeds: StdMutex<HashMap<u32, (f64, Instant)>>,
}

/// 死源拉黑时长。短于常见 CDN 抖动恢复时间，又足够让同一批大量小文件跳过它。
pub(crate) const DEAD_HOST_TTL: Duration = Duration::from_secs(120);

/// 单主机的 Range 支持判定。
///
/// 旧实现只按 host 缓存一个 bool，有两个坑（issue #8）：键不含 scheme/端口会串线；
/// 单条路径异常（某种资源实际不支持 Range）会把整台主机永久判为不支持，后续大文件
/// 被迫单连接流式。现在：
/// - 键 = scheme://host:port；
/// - 连续 RANGE_PROBE_MISSES 次「谎报 Range」才否定整台主机；
/// - HOST_PROBE_TTL 后重新判定，避免临时故障长期生效。
#[derive(Clone, Copy, Debug)]
pub(crate) struct RangeHealth {
    pub ok: bool,
    pub misses: u8,
    pub checked_at: Instant,
}

pub(crate) const HOST_PROBE_TTL: Duration = Duration::from_secs(600);
const RANGE_PROBE_MISSES: u8 = 2;

/// 目标路径的规范化键：绝对路径；Windows 上再折叠大小写（同卷大小写不敏感，
/// 两个仅大小写不同的 dest 会写同一个文件）。
pub(crate) fn normalize_dest(dest: &Path) -> std::path::PathBuf {
    let abs = std::path::absolute(dest).unwrap_or_else(|_| dest.to_path_buf());
    #[cfg(windows)]
    let abs = std::path::PathBuf::from(abs.to_string_lossy().to_lowercase());
    abs
}

/// Range 判定的缓存键：scheme + host + port（默认端口按 scheme 补齐）。
pub(crate) fn probe_key(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some(v) => v,
        None => return url.to_string(),
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
            (h, p.to_string())
        }
        _ => (
            authority,
            if scheme.eq_ignore_ascii_case("https") {
                "443".to_string()
            } else {
                "80".to_string()
            },
        ),
    };
    format!("{scheme}://{host}:{port}")
}

impl RunContext {
    /// 拉黑宿主（本次任务在该源上一个字节都没拿到）。
    pub(crate) fn mark_host_dead(&self, host: &str) {
        self.dead_hosts
            .lock()
            .unwrap()
            .insert(host.to_string(), Instant::now());
    }

    /// 宿主成功交付过数据 → 解除拉黑。
    pub(crate) fn mark_host_alive(&self, host: &str) {
        self.dead_hosts.lock().unwrap().remove(host);
    }

    /// 本次尝试零字节 → 判定该源不可用。
    ///
    /// **优先只拉黑这个 IP**：同一域名（BMCLAPI、ForgeCDN）背后是多 CDN，
    /// 屏蔽域名会连带废掉可用节点。只有拿不到对端 IP（连接未建立 / 未启用
    /// IP 级）时才退回域名级拉黑。
    pub(crate) fn mark_source_dead(&self, url: &str) {
        let host = host_of(url);
        if let Some(h) = &self.ip_health {
            if h.mark_failure(&host) {
                return;
            }
        }
        self.mark_host_dead(&host);
    }

    /// 该 URL 所在主机的 Range 判定（TTL 内有效）。
    pub(crate) fn cached_range_ok(&self, url: &str) -> Option<bool> {
        let key = probe_key(url);
        let guard = self.host_probe.read().unwrap();
        guard
            .get(&key)
            .filter(|h| h.checked_at.elapsed() < HOST_PROBE_TTL)
            .map(|h| h.ok)
    }

    /// 记录一次「谎报 Range」（声称支持、实际回整包）。连续命中才否定整台主机。
    pub(crate) fn record_range_miss(&self, url: &str) {
        let key = probe_key(url);
        let mut guard = self.host_probe.write().unwrap();
        let e = guard.entry(key).or_insert(RangeHealth {
            ok: true,
            misses: 0,
            checked_at: Instant::now(),
        });
        e.misses = e.misses.saturating_add(1);
        e.checked_at = Instant::now();
        if e.misses >= RANGE_PROBE_MISSES {
            e.ok = false;
        }
    }

    /// 记录一次正常判定。已被连续失败否定的主机不会被单次响应头翻回 true。
    pub(crate) fn record_range_ok(&self, url: &str, ok: bool) {
        let key = probe_key(url);
        let mut guard = self.host_probe.write().unwrap();
        if guard.get(&key).is_some_and(|h| !h.ok) {
            return;
        }
        guard.insert(
            key,
            RangeHealth {
                ok,
                misses: 0,
                checked_at: Instant::now(),
            },
        );
    }

    /// 记录本次连接的对端地址：确认该 IP 可用，解除其拉黑。
    pub(crate) fn note_remote(&self, url: &str, addr: Option<std::net::SocketAddr>) {
        if let (Some(h), Some(a)) = (&self.ip_health, addr) {
            h.mark_ok(&host_of(url), a.ip());
        }
    }

    /// 本次尝试应当使用的 URL：优先用探测解析后的真实地址。
    pub(crate) fn current_url(&self) -> String {
        if let Some(u) = self.resolved_url.lock().unwrap().as_ref() {
            return u.clone();
        }
        self.urls.lock().unwrap()[0].clone()
    }

    /// 记录探测解析出的真实 URL；与当前镜像相同则不记。
    pub(crate) fn set_resolved_url(&self, url: &str) {
        let mut slot = self.resolved_url.lock().unwrap();
        if self.urls.lock().unwrap().first().map(String::as_str) == Some(url) {
            *slot = None;
        } else {
            *slot = Some(url.to_string());
        }
    }

    pub(crate) fn clear_resolved_url(&self) {
        *self.resolved_url.lock().unwrap() = None;
    }

    /// 是否还有别的镜像可换。
    pub(crate) fn has_other_url(&self) -> bool {
        self.urls.lock().unwrap().len() > 1
    }

    /// 轮换到下一个镜像，返回新 URL（无镜像返回 None）。
    pub(crate) fn mirror_url(&self) -> Option<String> {
        // 换镜像后旧的解析结果失效
        self.clear_resolved_url();
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

    /// 带任务前缀的日志：多文件并发时日志可归因到具体任务。
    pub(crate) fn log_task(&self, level: LogLevel, message: String) {
        self.log(level, format!("[task {}] {message}", self.task.id));
    }

    /// 记录一条最近失败原因（环形，最多 8 条）。
    pub(crate) fn record_error(&self, message: String) {
        let mut errors = self.recent_errors.lock().unwrap();
        if errors.len() >= 8 {
            errors.pop_front();
        }
        errors.push_back(message);
    }

    /// 取最近失败原因（按时间序）。
    pub(crate) fn recent_errors(&self) -> Vec<String> {
        self.recent_errors.lock().unwrap().iter().cloned().collect()
    }

    /// 记录某段最近一次 1s 采样的瞬时速度。
    pub(crate) fn note_segment_speed(&self, index: u32, bps: f64) {
        self.segment_speeds
            .lock()
            .unwrap()
            .insert(index, (bps, Instant::now()));
    }

    /// 同批活跃段速度中位数（排除自己；只统计最近 5s 内有采样的段）。
    /// 只有一个活跃段时返回 None：没有横向基准时不做 L1 软重启，只保留 idle 超时。
    pub(crate) fn peer_median_speed(&self, exclude: u32) -> Option<f64> {
        let mut speeds: Vec<f64> = self
            .segment_speeds
            .lock()
            .unwrap()
            .iter()
            .filter(|(index, (_, at))| **index != exclude && at.elapsed() < Duration::from_secs(5))
            .map(|(_, (bps, _))| *bps)
            .collect();
        if speeds.is_empty() {
            return None;
        }
        speeds.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(speeds[speeds.len() / 2])
    }

    /// 当前协议选择：HTTP/3 开启且存在 h3 client 时用它，否则用 HTTP/2。
    fn h3_active(&self) -> bool {
        self.use_h3.load(Ordering::Relaxed) && self.h3.is_some()
    }

    /// 获取当前应使用的 client 引用（不触发回退判断）。
    fn client(&self) -> &reqwest::Client {
        if self.use_h1.load(Ordering::Relaxed) {
            return &self.h1;
        }
        if self.h3_active() {
            self.h3.as_ref().unwrap()
        } else {
            &self.h2
        }
    }
}

/// 一个可重试/可回退的原子请求：以 HTTP/3 优先发送，若因连接层失败（服务器
/// 不支持 HTTP/3、QUIC 握手失败等）自动切换到 HTTP/2 重试一次，并持久翻转
/// 本任务协议为 HTTP/2（后续请求直接走 H2）。
/// `build` 负责基于传入的 client 构造完整的 RequestBuilder。
async fn send_with_fallback<F, Fut>(
    ctx: &Arc<RunContext>,
    build: F,
) -> Result<reqwest::Response, reqwest::Error>
where
    F: Fn(&reqwest::Client) -> Fut,
    Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
{
    let was_h3 = ctx.h3_active();
    match build(ctx.client()).await {
        Ok(resp) => Ok(resp),
        Err(e) if was_h3 && e.is_connect() && ctx.options.http3_fallback => {
            // HTTP/3 不可达 → 持久回退 HTTP/2 并重试一次
            ctx.use_h3.store(false, Ordering::Relaxed);
            ctx.log(
                LogLevel::Warn,
                format!(
                    "HTTP/3 连接失败，运行时回退 HTTP/2: {}",
                    describe_reqwest_error(&e)
                ),
            );
            build(&ctx.h2).await
        }
        Err(e) => Err(e),
    }
}

pub(crate) struct Engine;

/// 段失败类别。
enum SegFailure {
    Cancelled,
    /// 真错误：重试轮次耗尽（保留 `.part`，由上层决定是否让用户重试/续传）。
    Exhausted(String),
    /// 服务器忽略 Range：必须改走整文件流式（唯一会删除 `.part` 的降级）。
    RangeIgnored(String),
    /// 服务器对 Range 的响应不可信（Content-Range 缺失/与请求不符）：同样改走整文件流式。
    RangeInvalid(String),
}

type SegmentOutcome = (u32, Result<(), SegFailure>);

#[derive(Debug)]
enum SegError {
    Cancelled,
    /// 真错误：扣 `max_retries` 预算。
    Retryable(String),
    /// L1 软重启：慢速但有进展，不扣重试预算，重新入队/重开连接。
    Rebuild(String),
    /// 非瞬态错误（4xx，除 408/429）：重试同一源没有意义，立即换镜像。
    Fatal(String),
    /// 服务器对带 Range 的请求返回全文件 200：非瞬态故障，重试/换段均无意义，
    /// 立即放弃分片路径转整文件流式（实测 dpdns.org 镜像：HEAD 宣称
    /// Accept-Ranges: bytes，GET+Range 却回 200，旧逻辑每段空转 5 次重试）。
    RangeIgnored,
    /// 服务器对 Range 的响应头不可信（Content-Range 缺失/与请求不符）：
    /// 立即放弃分片路径转整文件流式，避免写坏相邻段。
    RangeInvalid(String),
}

impl Engine {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) async fn run(&self, ctx: &Arc<RunContext>) -> Result<(), DownloadError> {
        let mut attempts = 0;
        loop {
            let result = self.run_once(ctx).await;
            if let Err(DownloadError::ChecksumMismatch { .. }) = result {
                // 校验失败的文件内容不可信：始终清理 .part（重下前 + 最终失败）
                let part = ctx.task.part_path();
                let _ = tokio::fs::remove_file(&part).await;
                let _ = tokio::fs::remove_file(crate::part_meta::meta_path_for(&part)).await;
                if attempts == 0 {
                    attempts += 1;
                    // 内容不符多半是刚落地的源给了坏字节（镜像 200 + 错误 body、错误页贴 200），
                    // 在同一源上重试只会再拿到同样的字节，换源才可能命中正确内容。
                    match ctx.mirror_url() {
                        Some(next) => ctx.log_task(
                            LogLevel::Warn,
                            format!("内容校验失败，删除 .part 并切换镜像重下: {next}"),
                        ),
                        None => ctx.log_task(
                            LogLevel::Warn,
                            "内容校验失败，删除 .part 自动重下".to_string(),
                        ),
                    }
                    continue;
                }
            }
            return result;
        }
    }

    async fn run_once(&self, ctx: &Arc<RunContext>) -> Result<(), DownloadError> {
        // 已知大小且本地已有同内容文件：直接复制，不发起任何网络请求（issue #11）。
        if let Some(size) = ctx.task.expected_size {
            if try_local_reuse(ctx, size).await? {
                return Ok(());
            }
        }
        let part = ctx.task.part_path();
        // 每次尝试都重新解析：上一轮的落地地址可能已失效，且换镜像后必须重新探测。
        ctx.clear_resolved_url();
        ctx.ranged_attempted.store(false, Ordering::Relaxed);

        // 不再单独发 HEAD：直接 GET，从**响应头**决定走流式还是分片。
        // 旧实现每个文件固定「HEAD + GET」两个请求（实测每文件请求数 = 2.00），
        // 小文件批量下载时等于把请求数翻倍；这里把判定合并进下载请求本身，
        // 文件不需要知道大小也能直接开始收字节。
        match self.run_streamed(ctx, &part).await {
            Err(DownloadError::NeedsRanged(total)) => {
                // 已经看过响应头（含重定向落地地址），放弃这段流、改走分片。
                ctx.ranged_attempted.store(true, Ordering::Relaxed);
                if try_local_reuse(ctx, total).await? {
                    return Ok(());
                }
                self.run_ranged(ctx, &part, total).await
            }
            other => other,
        }
    }

    // ---------------- Range 多段路径 ----------------

    async fn run_ranged(
        &self,
        ctx: &Arc<RunContext>,
        part: &Path,
        total: u64,
    ) -> Result<(), DownloadError> {
        // 写盘前预检可用空间（issue #9）：不足时立即失败，避免下到一半才发现。
        ensure_disk_space(part, total)?;
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

        // 续传以块级完成位图为准（issue #2）：.part 的文件大小只是「最高水位」，
        // 多段并行时中间空洞无法从大小推断。meta 缺失、身份不符（url / total /
        // sha256 / 块大小）或 .part 超出总长时，一律丢弃两者从头下载。
        let meta_path = crate::part_meta::meta_path_for(part);
        let sha_hex = ctx.task.sha256.as_ref().map(|d| hex_encode(d));
        let mut part_meta = match crate::part_meta::PartMeta::load(&meta_path).await {
            Some(m)
                if m.matches(
                    &ctx.task.url,
                    total,
                    sha_hex.as_deref(),
                    crate::part_meta::DEFAULT_BLOCK_SIZE,
                ) =>
            {
                m
            }
            _ => {
                let _ = tokio::fs::remove_file(part).await;
                let _ = tokio::fs::remove_file(&meta_path).await;
                crate::part_meta::PartMeta::new(
                    &ctx.task.url,
                    total,
                    sha_hex,
                    crate::part_meta::DEFAULT_BLOCK_SIZE,
                )
            }
        };

        if part.is_file() {
            // 段起点可能不在块边界：resumable_bytes 保守返回 0（块内不完整就重下，
            // 最多浪费 1 MiB/段），绝不跳过未确认的数据。
            for s in segments.values_mut() {
                let got = part_meta.resumable_bytes(s.start, s.len());
                s.downloaded.store(got, Ordering::Relaxed);
            }
            let resumed: u64 = segments
                .values()
                .map(|s| s.downloaded.load(Ordering::Relaxed))
                .sum();
            ctx.stats.downloaded.store(resumed, Ordering::Relaxed);
            if resumed > 0 {
                ctx.log(
                    LogLevel::Info,
                    format!("断点续传: 位图已确认 {resumed} 字节"),
                );
            }
        } else {
            ctx.stats.downloaded.store(0, Ordering::Relaxed);
        }
        part_meta.save(&meta_path).await.ok();
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

        // L1 软重启不扣重试预算；L2 真错误才扣。段级「轮次」用于单段耗尽后
        // 再给一整轮 max_retries，避免一个坏段把整个文件拖去单连接流式。
        let mut rounds: HashMap<u32, u32> = HashMap::new();
        let mut retry_at: HashMap<u32, Instant> = HashMap::new();
        let mut terminal_failures: HashMap<u32, String> = HashMap::new();
        let mut range_ignored: Option<String> = None;
        let mut terminal_error: Option<String> = None;
        // issue #6：拆分只有在确实提速时才继续。每次拆分记录当时速率，下一拍比较；
        // 连续两拍看不出提升就停止拆分（连接数不是瓶颈时继续拆只增加开销与竞争）。
        let mut split_enabled = true;
        let mut split_baseline: Option<f64> = None;
        let mut split_stalls: u32 = 0;
        let mut smooth_speed = 0.0f64;
        let mut last_sample = (Instant::now(), ctx.stats.downloaded.load(Ordering::Relaxed));
        loop {
            // 启动待运行段（受并发上限约束）。拿不到全局段许可时**不启动**，
            // 但绝不把它当成段失败：许可排队只是并发上限，不是下载错误。
            if range_ignored.is_none() {
                let now = Instant::now();
                let mut pending: Vec<Segment> = segments
                    .values()
                    .filter(|s| {
                        !s.finished()
                            && !seg_handles.contains_key(&s.index)
                            && !terminal_failures.contains_key(&s.index)
                            && retry_at.get(&s.index).is_none_or(|at| *at <= now)
                    })
                    .cloned()
                    .collect();
                pending.sort_by_key(|s| s.index);
                for seg in pending {
                    if seg_handles.len() >= max_segments {
                        break;
                    }
                    // 全局段许可：拿不到就本轮不启动，等下轮（段结束释放 permit）。
                    let Ok(permit) = ctx.segment_sem.clone().try_acquire_owned() else {
                        break;
                    };
                    let idx = seg.index;
                    ctx.stats.active_segments.fetch_add(1, Ordering::Relaxed);
                    let ctx_task = ctx.clone();
                    let ah = set.spawn(async move {
                        let _permit = permit;
                        run_segment(ctx_task, seg).await
                    });
                    seg_handles.insert(idx, ah);
                }
            }

            let all_finished = segments.values().all(|s| s.finished());
            if all_finished && seg_handles.is_empty() {
                break;
            }
            if seg_handles.is_empty() && !all_finished {
                if range_ignored.is_some() {
                    break;
                }
                // 还有等待全局许可或重试冷却的段：继续等，不判失败、不删 .part。
                // 这里必须主动 sleep：JoinSet 为空时 join_next() 会立刻返回 None，
                // 直接进 select 会变成忙循环。
                let waiting = segments.values().any(|s| {
                    !s.finished()
                        && !terminal_failures.contains_key(&s.index)
                        && !seg_handles.contains_key(&s.index)
                });
                if !waiting {
                    let mut reasons: Vec<String> = terminal_failures.values().cloned().collect();
                    reasons.sort();
                    terminal_error = Some(reasons.join("; "));
                    break;
                }
                tokio::select! {
                    _ = ctx.cancel.cancelled() => {
                        reporter.abort();
                        refresh_bitmap(&mut part_meta, &segments);
                        part_meta.save(&meta_path).await.ok();
                        return Err(DownloadError::Cancelled);
                    }
                    _ = sleep(Duration::from_millis(200)) => {}
                }
                continue;
            }

            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    for h in seg_handles.values() { h.abort(); }
                    reporter.abort();
                    // 保留已完成区块信息，恢复时只补空洞。
                    refresh_bitmap(&mut part_meta, &segments);
                    part_meta.save(&meta_path).await.ok();
                    return Err(DownloadError::Cancelled);
                }
                out = set.join_next() => {
                    match out {
                        Some(Ok((idx, result))) => {
                            ctx.stats.active_segments.fetch_sub(1, Ordering::Relaxed);
                            seg_handles.remove(&idx);
                            match result {
                                // 完成段保留：位图重算需要所有段的水位（删掉会让已完成
                                // 区块重新变回「未覆盖」）。
                                Ok(()) => {
                                    rounds.remove(&idx);
                                    retry_at.remove(&idx);
                                }
                                Err(SegFailure::Cancelled) => {}
                                Err(SegFailure::RangeIgnored(reason)) => {
                                    ctx.log_task(
                                        LogLevel::Warn,
                                        format!("段 {idx}: 服务器忽略 Range（{reason}），改整文件流式"),
                                    );
                                    range_ignored = Some(reason);
                                }
                                Err(SegFailure::RangeInvalid(reason)) => {
                                    ctx.log_task(
                                        LogLevel::Warn,
                                        format!("段 {idx}: Range 响应不可信（{reason}），改整文件流式"),
                                    );
                                    range_ignored = Some(reason);
                                }
                                Err(SegFailure::Exhausted(reason)) => {
                                    let round = rounds.entry(idx).or_insert(0);
                                    *round += 1;
                                    let limit = ctx.options.segment_rounds.max(1);
                                    ctx.record_error(format!("seg {idx}: {reason}"));
                                    if *round < limit {
                                        // 单段耗尽不等于整文件失败：保留已写进度，
                                        // 冷却后重新入队，再给一整轮重试预算。
                                        retry_at.insert(
                                            idx,
                                            Instant::now() + Duration::from_secs(1),
                                        );
                                        ctx.log_task(
                                            LogLevel::Warn,
                                            format!(
                                                "段 {idx} 第 {round}/{limit} 轮重试耗尽，保留进度稍后重试: {reason}"
                                            ),
                                        );
                                    } else {
                                        terminal_failures.insert(idx, reason.clone());
                                        ctx.log_task(
                                            LogLevel::Error,
                                            format!("段 {idx} 全部 {limit} 轮重试耗尽: {reason}"),
                                        );
                                    }
                                }
                            }
                        }
                        Some(Err(je)) => {
                            ctx.stats.active_segments.fetch_sub(1, Ordering::Relaxed);
                            if je.is_cancelled() {
                                continue; // 被动态拆分 abort，忽略
                            }
                            // 段任务 panic 属于实现缺陷：保留 .part，带明确原因失败，
                            // 不再静默降级整文件把已下进度删掉。
                            ctx.log_task(LogLevel::Error, format!("段任务异常: {je}"));
                            terminal_error = Some(format!("段任务异常: {je}"));
                            break;
                        }
                        None => {}
                    }
                }
                _ = sleep(opts.split_sample_interval) => {
                    // 周期性把位图落到 sidecar：崩溃最多重下最后一个采样窗口的数据。
                    refresh_bitmap(&mut part_meta, &segments);
                    part_meta.save(&meta_path).await.ok();

                    let now_dl = ctx.stats.downloaded.load(Ordering::Relaxed);
                    let elapsed = last_sample.0.elapsed().as_secs_f64();
                    if elapsed > 0.0 {
                        let inst = now_dl.saturating_sub(last_sample.1) as f64 / elapsed;
                        smooth_speed = if smooth_speed == 0.0 {
                            inst
                        } else {
                            smooth_speed * 0.7 + inst * 0.3
                        };
                    }
                    last_sample = (Instant::now(), now_dl);

                    // 上一拍拆分后的效果评估。
                    if let Some(base) = split_baseline.take() {
                        if smooth_speed > base * 1.1 {
                            split_stalls = 0;
                        } else {
                            split_stalls += 1;
                            if split_stalls >= 2 && split_enabled {
                                split_enabled = false;
                                ctx.log_task(
                                    LogLevel::Info,
                                    "拆分连续无提速，停止继续拆分（issue #6）".to_string(),
                                );
                            }
                        }
                    }
                    if split_enabled
                        && seg_handles.len() < max_segments
                        && ctx.segment_sem.available_permits() > 0
                    {
                        let before = seg_handles.len();
                        try_split(ctx, &mut segments, &mut seg_handles, seg_counter.clone()).await;
                        if seg_handles.len() > before {
                            split_baseline = Some(smooth_speed.max(1.0));
                        }
                    }
                }
            }
        }
        reporter.abort();

        // 服务器忽略 Range 是唯一需要「整文件流式」的降级：分片请求必然全失败，
        // 删 .part 重下是合理的。其余失败一律保留 .part 供续传。
        if let Some(reason) = range_ignored {
            ctx.log_task(
                LogLevel::Warn,
                format!("服务器忽略 Range，改整文件流式下载: {reason}"),
            );
            return self.download_full_fallback(ctx, part, total).await;
        }
        if let Some(err) = terminal_error {
            refresh_bitmap(&mut part_meta, &segments);
            part_meta.save(&meta_path).await.ok();
            return Err(DownloadError::Exhausted(format!(
                "分片下载失败（.part 已保留，可重试续传）: {err}"
            )));
        }

        // 完成判定：所有块都确认写完，且文件长度等于总长（issue #2）。只比长度
        // 无法发现中间空洞。
        refresh_bitmap(&mut part_meta, &segments);
        let actual = tokio::fs::metadata(part).await?.len();
        if actual != total || !part_meta.all_complete() {
            let done: u64 = segments
                .values()
                .map(|s| s.downloaded.load(Ordering::Relaxed))
                .sum();
            return Err(DownloadError::Incomplete {
                expected: total,
                actual: done.min(total),
            });
        }
        part_meta.save(&meta_path).await.ok();
        finalize_verified(
            part,
            &ctx.task.dest,
            ctx.task.sha256,
            ctx.task.sha1,
            ctx.task.expected_size,
        )
        .await?;
        let _ = tokio::fs::remove_file(&meta_path).await;
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
        let _ = tokio::fs::remove_file(crate::part_meta::meta_path_for(part)).await;
        self.run_streamed(ctx, part).await
    }

    // ---------------- 流式路径（服务器不支持 Range） ----------------

    async fn run_streamed(&self, ctx: &Arc<RunContext>, part: &Path) -> Result<(), DownloadError> {
        // 注意：这里**不再**删除已有的 .part。run_once 现在总是先走一次流式请求做
        // 响应头嗅探（判断是否该转分片），若在入口就删文件，分片路径的断点续传会在
        // 决策之前被清空。真正的清理由重试循环内每次尝试前的 `remove_file` 负责。
        if let Some(parent) = part.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        ctx.log_task(
            LogLevel::Info,
            format!("流式路径开始: {part:?} url={}", ctx.current_url()),
        );

        // 流式路径同样定期上报进度（否则下载中心进度会卡在 0% 直到完成）。
        let reporter = spawn_progress_reporter(ctx);
        let result = async {
            let opts = &ctx.options;
            let max_retries = ctx.task.max_retries.unwrap_or(opts.max_retries);
            let mut attempt: u32 = 0;
            let mut url = ctx.current_url();
            loop {
                if ctx.cancel.is_cancelled() {
                    return Err(DownloadError::Cancelled);
                }
                // 每次尝试前清空 .part（流式无断点续传，防止残留叠加）；
                // 计数同步归零——.part 从头下，累计值不清会让进度超 100%
                // 每次尝试前清空 .part 的工作交给 `try_streamed_once`——必须放在
                // 响应头嗅探**之后**，否则 run_once 的流式嗅探会在决定走分片之前
                // 把断点续传的 .part 删掉（会让多段续传彻底失效）。
                ctx.stats.downloaded.store(0, Ordering::Relaxed);
                match try_streamed_once(ctx, part, &url).await {
                    Ok(()) => {
                        return finalize_verified(
                            part,
                            &ctx.task.dest,
                            ctx.task.sha256,
                            ctx.task.sha1,
                            ctx.task.expected_size,
                        )
                        .await
                    }
                    // 响应头表明该走分片：交给 run_once 处理，不算失败、不计重试。
                    Err(e @ DownloadError::NeedsRanged(_)) => return Err(e),
                    Err(DownloadError::Cancelled) => return Err(DownloadError::Cancelled),
                    Err(e) => {
                        // 快速失败：本次尝试一个字节都没收到 → 这个源是死的（挂起/截断），
                        // 先把该宿主拉黑并立刻换源，而不是在同一源上把重试预算烧完。
                        // 旧实现要耗尽 max_retries × idle 才轮换：默认 5×30s，实测单文件
                        // 约 5 分钟才用上可用镜像。
                        let got_bytes = ctx.stats.downloaded.load(Ordering::Relaxed) > 0;
                        ctx.record_error(format!("stream url={url} attempt={attempt}: {e}"));
                        // 4xx（除 408/429）：重试同一源没有意义，直接换镜像。
                        let fatal_status = match &e {
                            DownloadError::HttpStatus { status, .. } => {
                                !is_retryable_status(*status)
                            }
                            _ => false,
                        };
                        if fatal_status && ctx.has_other_url() {
                            if let Some(next) = ctx.mirror_url() {
                                attempt = 0;
                                url = next;
                                ctx.log_task(
                                    LogLevel::Warn,
                                    format!("非瞬态错误（{e}），立即切换镜像 {url}"),
                                );
                                continue;
                            }
                        }
                        // 流式路径每次尝试都从头写，换源没有续传代价：已经收到过字节
                        // 但看门狗再次超时时也换源，避免在同一坏节点上耗完整个重试预算。
                        let stalled = matches!(e, DownloadError::Exhausted(_));
                        if stalled && ctx.has_other_url() {
                            if let Some(next) = ctx.mirror_url() {
                                attempt = 0;
                                url = next;
                                ctx.log_task(LogLevel::Warn, format!("连续无进展，切换镜像 {url}"));
                                continue;
                            }
                        }
                        if !got_bytes {
                            ctx.mark_source_dead(&url);
                        }
                        if !got_bytes && ctx.has_other_url() {
                            if let Some(next) = ctx.mirror_url() {
                                attempt = 0;
                                url = next;
                                ctx.log_task(
                                    LogLevel::Warn,
                                    format!("源 {url} 未返回数据，立即切换镜像"),
                                );
                                continue;
                            }
                        }
                        attempt += 1;
                        if attempt > max_retries {
                            match ctx.mirror_url() {
                                Some(next) => {
                                    attempt = 0;
                                    url = next;
                                    ctx.log_task(
                                        LogLevel::Warn,
                                        format!("流式重试耗尽，切换镜像 {url}"),
                                    );
                                    continue;
                                }
                                None => {
                                    return Err(DownloadError::Other(format!(
                                        "{e}（url={url}，已收 {}）",
                                        human_bytes(ctx.stats.downloaded.load(Ordering::Relaxed))
                                    )))
                                }
                            }
                        }
                        let delay = backoff(opts, attempt);
                        ctx.log_task(
                            LogLevel::Warn,
                            format!("流式下载第 {attempt} 次重试（{e}）"),
                        );
                        tokio::select! {
                            _ = ctx.cancel.cancelled() => return Err(DownloadError::Cancelled),
                            _ = sleep(delay) => {}
                        }
                    }
                }
            }
        }
        .await;
        reporter.abort();
        result
    }
}

// ================= 单段执行 =================

async fn run_segment(ctx: Arc<RunContext>, mut seg: Segment) -> SegmentOutcome {
    let idx = seg.index;
    let max_retries = ctx.task.max_retries.unwrap_or(ctx.options.max_retries);
    let mut attempt: u32 = 0;
    let mut attempt_url = ctx.current_url();
    // L1 软重启预算：与 max_retries 完全独立，超限后不再杀段，只保留 idle 超时。
    let mut rebuilds: u32 = 0;
    let mut allow_rebuild = true;
    loop {
        if ctx.cancel.is_cancelled() {
            return (idx, Err(SegFailure::Cancelled));
        }
        match try_segment_once(&ctx, &mut seg, &attempt_url, allow_rebuild).await {
            Ok(()) => return (idx, Ok(())),
            Err(SegError::Cancelled) => return (idx, Err(SegFailure::Cancelled)),
            Err(SegError::Rebuild(snapshot)) => {
                let limit = ctx.options.watchdog_rebuild_limit.max(1);
                if rebuilds >= limit {
                    // 超限后不再杀段：慢就慢，只要还在出字节就让它传完。
                    allow_rebuild = false;
                    ctx.log_task(
                        LogLevel::Warn,
                        format!(
                            "[watchdog] type=L1_rebuild_exhausted seg={idx} rebuild={rebuilds}/{limit} {snapshot}"
                        ),
                    );
                    continue;
                }
                rebuilds += 1;
                ctx.log_task(
                    LogLevel::Warn,
                    format!(
                        "[watchdog] type=L1_rebuild seg={idx} rebuild={rebuilds}/{limit} {snapshot}"
                    ),
                );
                // 冷却后重开连接；已写字节保留，从当前 offset 续传。
                let cooldown = ctx.options.watchdog_rebuild_cooldown;
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return (idx, Err(SegFailure::Cancelled)),
                    _ = sleep(cooldown) => {}
                }
                continue;
            }
            Err(SegError::Fatal(reason)) => {
                ctx.record_error(format!("seg {idx} url={attempt_url}: {reason}"));
                // 4xx（非 408/429）：该源这条路径不存在/无权限，重试同一源只是空转。
                if let Some(next) = ctx.mirror_url() {
                    attempt = 0;
                    attempt_url = next;
                    ctx.log_task(
                        LogLevel::Warn,
                        format!("段 {idx}: 非瞬态错误（{reason}），立即切换镜像 {attempt_url}"),
                    );
                    continue;
                }
                return (idx, Err(SegFailure::Exhausted(reason)));
            }
            Err(SegError::RangeIgnored) => {
                // ponytail: 不轮换镜像重试分片——镜像对 Range 的支持未知，直接整文件流式最稳
                ctx.log_task(
                    LogLevel::Warn,
                    format!("段 {idx}: 服务器忽略 Range 请求，转整文件下载"),
                );
                return (
                    idx,
                    Err(SegFailure::RangeIgnored("服务器忽略 Range 请求".into())),
                );
            }
            Err(SegError::RangeInvalid(reason)) => {
                // 响应头与请求区间不符：写进去会污染相邻段，立即转整文件流式。
                ctx.log_task(
                    LogLevel::Warn,
                    format!("段 {idx}: Range 响应不可信（{reason}），转整文件下载"),
                );
                return (idx, Err(SegFailure::RangeInvalid(reason)));
            }
            Err(SegError::Retryable(reason)) => {
                ctx.record_error(format!(
                    "seg {idx} url={attempt_url} attempt={attempt}: {reason}"
                ));
                // 快速失败：这一段在本源上一个字节都没拿到 → 源是死的，立刻换源，
                // 不在同一源上耗尽重试预算。
                let progressed = seg.downloaded.load(Ordering::Relaxed) > 0;
                if !progressed {
                    ctx.mark_source_dead(&attempt_url);
                    if ctx.has_other_url() {
                        if let Some(next) = ctx.mirror_url() {
                            attempt = 0;
                            attempt_url = next;
                            ctx.log_task(
                                LogLevel::Warn,
                                format!("段 {idx}: 源无响应，立即切换镜像 {attempt_url}"),
                            );
                            continue;
                        }
                    }
                }
                attempt += 1;
                if attempt > max_retries {
                    match ctx.mirror_url() {
                        Some(next) => {
                            attempt = 0;
                            attempt_url = next;
                            ctx.log_task(
                                LogLevel::Warn,
                                format!("段 {idx} 重试耗尽（{reason}），切换镜像 {attempt_url}"),
                            );
                            continue;
                        }
                        None => return (idx, Err(SegFailure::Exhausted(reason))),
                    }
                }
                let delay = backoff(&ctx.options, attempt);
                ctx.log_task(
                    LogLevel::Warn,
                    format!("段 {idx} 第 {attempt} 次重试（{reason}）"),
                );
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
    ctx: &Arc<RunContext>,
    seg: &mut Segment,
    url: &str,
    allow_rebuild: bool,
) -> Result<(), SegError> {
    let from = seg.start + seg.downloaded.load(Ordering::Relaxed);
    if from > seg.end {
        return Ok(());
    }
    let attempt_started = Instant::now();
    let attempt_start_bytes = seg.downloaded.load(Ordering::Relaxed);
    let range = format!("bytes={from}-{}", seg.end);
    // aria2 式独立连接：分段请求一律走 HTTP/1.1 客户端，hyper 连接池为并发段自动
    // 建立多条 TCP——HTTP/2 多路复用会把所有段压进同一条流，高 RTT / 按连接限速
    // 的 CDN 上「16 段并行」实际吞吐 = 单流上限（实测镜像源单流 125KB/s vs
    // aria2 16 连接 13MB/s）。
    // 超时语义：仅对「等待响应头」设 options.timeout（TTFB 保护）；响应体不限时，
    // 由看门狗负责断流/龟速检测。reqwest 的 per-request total timeout 从建连
    // 一直计时到 body 读完，慢源上 8MB 分段 >60s 必被杀掉重试、永无完成可能。
    let send = ctx
        .h1
        .get(url)
        .headers(ctx.headers.clone())
        .header(reqwest::header::RANGE, range.clone())
        .send();
    let resp = match tokio::time::timeout(ctx.options.timeout, send).await {
        Ok(r) => r.map_err(|e| SegError::Retryable(describe_reqwest_error(&e)))?,
        Err(_) => {
            return Err(SegError::Retryable(format!(
                "等待响应头超时 {}s（{url}，range={range}）",
                ctx.options.timeout.as_secs()
            )))
        }
    };
    let status = resp.status();
    ctx.log_task(
        LogLevel::Debug,
        format!(
            "段 {} 响应: {status} url={url} range={range} content_range={:?} content_length={:?}",
            seg.index,
            resp.headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok()),
            content_length(resp.headers()),
        ),
    );
    if status == reqwest::StatusCode::OK {
        // 200 = 全文件响应：仅当段本身覆盖整个文件且从头开始时合法（服务器对整文件 Range 返回 200）
        let total = ctx.stats.total.load(Ordering::Relaxed);
        let whole_file_single =
            seg.start == 0 && seg.end + 1 == total && seg.downloaded.load(Ordering::Relaxed) == 0;
        if !whole_file_single {
            // 探测缓存翻转为不支持 Range：同主机后续文件直接走流式，不再撞同一堵墙
            ctx.record_range_miss(url);
            return Err(SegError::RangeIgnored);
        }
    }
    if !status.is_success() {
        let code = status.as_u16();
        if code == 429 {
            // 429：按 Retry-After 停一下再重试，避免持续触发风控（上限 30s）。
            if let Some(secs) = retry_after_secs(resp.headers()) {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(SegError::Cancelled),
                    _ = sleep(Duration::from_secs(secs.min(30))) => {}
                }
            }
        }
        let msg = format!("HTTP {status} ({url})");
        if is_retryable_status(code) {
            return Err(SegError::Retryable(msg));
        }
        return Err(SegError::Fatal(msg));
    }
    // 连接已建立：记录对端 IP，确认该节点可用（IP 级黑名单据此解除）。
    ctx.note_remote(url, resp.remote_addr());

    // 分段响应必须与请求区间一致（issue #3）：异常 CDN 或中间层可能回整包/超长
    // body，越过 seg.end 就会覆盖相邻段。206 必须带合法 Content-Range 且落在本次
    // 请求区间内；总长与已知值不一致同样视为该源不可信。
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        let total_known = ctx.stats.total.load(Ordering::Relaxed);
        match parse_content_range(resp.headers()) {
            Some((start, end, total)) => {
                let bad_total = matches!(total, Some(t) if total_known > 0 && t != total_known);
                if start != from || end > seg.end || bad_total {
                    return Err(SegError::RangeInvalid(format!(
                        "Content-Range 与请求不符: 收到 {start}-{end}/{total:?}，请求 {from}-{}/{}",
                        seg.end, total_known
                    )));
                }
            }
            None => {
                return Err(SegError::RangeInvalid(
                    "206 响应缺少合法 Content-Range".into(),
                ))
            }
        }
    }

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false) // 断点续传：不截断已有 .part
        .open(&ctx.task.part_path())
        .await
        .map_err(|e| {
            SegError::Retryable(format!("打开 .part {:?} 失败: {e}", ctx.task.part_path()))
        })?;
    file.seek(tokio::io::SeekFrom::Start(from))
        .await
        .map_err(|e| SegError::Retryable(e.to_string()))?;

    let mut stream = resp.bytes_stream();
    let idle_timeout = ctx.options.watchdog_idle_timeout;
    let first_byte_timeout = ctx.options.watchdog_first_byte_timeout;
    // 固定周期 tick：不能每轮 select 新建 `sleep(1s)`——数据持续到达时
    // 另一分支总是先就绪，sleep 会被反复丢弃，看门狗永远不采样（旧实现的隐性 bug）。
    let mut watchdog_tick = tokio::time::interval(Duration::from_secs(1));
    watchdog_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    watchdog_tick.tick().await;
    let mut smooth: f64 = 0.0;
    let mut slow_since: Option<Instant> = None;
    let mut last_activity = Instant::now();
    let mut prev_tick_bytes = attempt_start_bytes;

    loop {
        tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(SegError::Cancelled),
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        // 绝不越过段尾写：多出来的字节属于相邻段，写进去就是静默损坏。
                        let remaining =
                            seg.len().saturating_sub(seg.downloaded.load(Ordering::Relaxed));
                        if remaining == 0 {
                            return Err(SegError::Retryable("响应体超出段范围".into()));
                        }
                        let take = (bytes.len() as u64).min(remaining) as usize;
                        file.write_all(&bytes[..take]).await
                            .map_err(|e| SegError::Retryable(format!("写 .part 失败: {e}")))?;
                        seg.downloaded.fetch_add(take as u64, Ordering::Relaxed);
                        ctx.stats.downloaded.fetch_add(take as u64, Ordering::Relaxed);
                        if let Some(l) = &ctx.limiter {
                            l.acquire(take as u64).await;
                        }
                        if take < bytes.len() {
                            return Err(SegError::Retryable(format!(
                                "响应体超出段范围: 段剩余 {remaining} 字节，收到 {} 字节",
                                bytes.len()
                            )));
                        }
                        last_activity = Instant::now();
                    }
                    Some(Err(e)) => {
                        return Err(SegError::Retryable(format!(
                            "网络中断: {}",
                            describe_reqwest_error(&e)
                        )));
                    }
                    None => {
                        file.sync_data().await
                            .map_err(|e| SegError::Retryable(format!("sync .part 失败: {e}")))?;
                        if seg.finished() {
                            ctx.mark_host_alive(&host_of(url));
                            return Ok(());
                        }
                        let got = seg.downloaded.load(Ordering::Relaxed);
                        return Err(SegError::Retryable(format!(
                            "流提前结束: 已收 {} / 期望 {}（url={url}）",
                            got,
                            seg.len()
                        )));
                    }
                }
            }
            _ = watchdog_tick.tick() => {
                let now = Instant::now();
                let got = seg.downloaded.load(Ordering::Relaxed);
                let received_this_attempt = got.saturating_sub(attempt_start_bytes);
                let idle = now.duration_since(last_activity);
                let inst = got.saturating_sub(prev_tick_bytes) as f64;
                prev_tick_bytes = got;
                smooth = if smooth == 0.0 {
                    inst
                } else {
                    smooth * 0.8 + inst * 0.2
                };
                ctx.note_segment_speed(seg.index, inst);

                let total = ctx.stats.total.load(Ordering::Relaxed);
                let downloaded = ctx.stats.downloaded.load(Ordering::Relaxed);
                let remaining = total.saturating_sub(downloaded);
                let active = ctx.stats.active_segments.load(Ordering::Relaxed);
                let peer = ctx.peer_median_speed(seg.index);
                let snapshot = watchdog_snapshot(
                    seg, url, from, got, inst, smooth, peer, active, remaining,
                );

                // L2 硬失败：首字节迟迟不来 / 已出过字节但断流超过 idle。
                if received_this_attempt == 0 {
                    if idle >= first_byte_timeout {
                        return Err(SegError::Retryable(format!(
                            "看门狗: 等待响应体首字节超时 {}s（{snapshot}）",
                            first_byte_timeout.as_secs()
                        )));
                    }
                } else if idle >= idle_timeout {
                    return Err(SegError::Retryable(format!(
                        "看门狗: 无数据超过 {}s（{snapshot}）",
                        idle_timeout.as_secs()
                    )));
                }

                // L1 软重启：只在末端阶段、且只相对「同批活跃段中位数」判断。
                // 全程高并发阶段不因速度比例杀段；超限后由 run_segment 关闭该逻辑。
                if allow_rebuild {
                    let max_segments = ctx.task.max_segments.unwrap_or(ctx.options.max_segments);
                    let end_stage = active < max_segments
                        || remaining
                            < ctx
                                .options
                                .segment_size
                                .saturating_mul(ctx.options.watchdog_end_stage_factor);
                    if attempt_started.elapsed() >= ctx.options.watchdog_rebuild_min_runtime
                        && got >= ctx.options.watchdog_rebuild_min_bytes
                        && end_stage
                    {
                        match peer {
                            Some(peer_bps)
                                if peer_bps > 1024.0
                                    && smooth
                                        < peer_bps * ctx.options.watchdog_rebuild_ratio =>
                            {
                                let since = *slow_since.get_or_insert(now);
                                if now.duration_since(since)
                                    >= ctx.options.watchdog_rebuild_slow_duration
                                {
                                    return Err(SegError::Rebuild(snapshot));
                                }
                            }
                            _ => slow_since = None,
                        }
                    } else {
                        slow_since = None;
                    }
                }
            }
        }
    }
}

// ================= 流式单次尝试 =================

async fn try_streamed_once(
    ctx: &Arc<RunContext>,
    part: &Path,
    url: &str,
) -> Result<(), DownloadError> {
    // 超时语义与分段路径一致：仅保护「等待响应头」（TTFB），body 不限总时长、
    // 由看门狗 idle_timeout 检测断流——total timeout 会让慢源上的大文件永远
    // 无法在 60s 内传完，被反复杀掉重下。
    let resp = match tokio::time::timeout(
        ctx.options.timeout,
        send_with_fallback(ctx, |client| {
            client.get(url).headers(ctx.headers.clone()).send()
        }),
    )
    .await
    {
        Ok(r) => r.map_err(|e| {
            DownloadError::Other(format!(
                "等待响应头失败（{url}）: {}",
                describe_reqwest_error(&e)
            ))
        })?,
        Err(_) => {
            return Err(DownloadError::Exhausted(format!(
                "等待响应头超时 {}s（{url}）",
                ctx.options.timeout.as_secs()
            )));
        }
    };
    let status = resp.status();
    ctx.log_task(LogLevel::Debug, format!("流式响应: {status} url={url}"));
    if !status.is_success() {
        let code = status.as_u16();
        if code == 429 {
            if let Some(secs) = retry_after_secs(resp.headers()) {
                tokio::select! {
                    _ = ctx.cancel.cancelled() => return Err(DownloadError::Cancelled),
                    _ = sleep(Duration::from_secs(secs.min(30))) => {}
                }
            }
        }
        return Err(DownloadError::HttpStatus {
            status: code,
            url: url.to_string(),
        });
    }
    ctx.note_remote(url, resp.remote_addr());

    // 用响应头完成原来 HEAD 探测承担的两件事：
    //   1. 记录跟随重定向后的真实地址（纯重定向器对带 Range 的请求会报错，
    //      分段必须用落地地址，见 `resolved_url` 字段注释）；
    //   2. 缓存该宿主是否支持 Range，供后续同主机文件复用判定。
    ctx.set_resolved_url(resp.url().as_str());
    let cached_range = ctx.cached_range_ok(url);
    let header_range_ok = accept_ranges_bytes(resp.headers());
    // 只在没有既有结论时写入：`try_segment_once` 发现「谎报 Range」会把缓存翻成
    // false，若这里再按响应头写回 true，同主机每个文件都会重新撞一次分片。
    if cached_range.is_none() {
        ctx.record_range_ok(url, header_range_ok);
    }
    let range_ok = cached_range.unwrap_or(header_range_ok);
    let total = content_length(resp.headers());
    if let Some(t) = total {
        ctx.stats.total.store(t, Ordering::Relaxed);
    }

    // 文件够大且源支持 Range → 放弃这段流改走分片（多连接并行）。
    // 仅在本次尝试还没进入过分片时触发一次，避免与 `run_ranged` 的降级互相弹跳。
    if range_ok
        && !ctx.ranged_attempted.load(Ordering::Relaxed)
        && total.is_some_and(|t| t > ctx.options.split_threshold)
    {
        ctx.log_task(
            LogLevel::Info,
            format!(
                "响应头判定为大文件（{} 字节）且支持 Range，改走分片: {url}",
                total.unwrap_or(0)
            ),
        );
        return Err(DownloadError::NeedsRanged(total.unwrap_or(0)));
    }

    // 流式重试/首次真正开始写之前清空 .part（嗅探阶段已提前返回，不会走到这里）。
    // 块位图随之失效，一并删除，避免下次误用。
    let _ = tokio::fs::remove_file(part).await;
    let _ = tokio::fs::remove_file(crate::part_meta::meta_path_for(part)).await;
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false) // 上一次尝试的残留已在上方删除
        .open(part)
        .await?;
    let mut stream = resp.bytes_stream();
    let idle_timeout = ctx.options.watchdog_idle_timeout;
    let first_byte_timeout = ctx.options.watchdog_first_byte_timeout;
    let attempt_started = Instant::now();
    let mut got_any = false;
    let mut last_activity = Instant::now();
    let mut watchdog_tick = tokio::time::interval(Duration::from_secs(1));
    watchdog_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    watchdog_tick.tick().await;
    loop {
        tokio::select! {
            _ = ctx.cancel.cancelled() => return Err(DownloadError::Cancelled),
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        got_any = true;
                        file.write_all(&bytes).await?;
                        ctx.stats.downloaded.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        if let Some(l) = &ctx.limiter {
                            l.acquire(bytes.len() as u64).await;
                        }
                        last_activity = Instant::now();
                    }
                    Some(Err(e)) => {
                        return Err(DownloadError::Other(format!(
                            "流式网络中断（{url}）: {}",
                            describe_reqwest_error(&e)
                        )));
                    }
                    None => {
                        file.sync_all().await?;
                        let total = ctx.stats.total.load(Ordering::Relaxed);
                        let downloaded = ctx.stats.downloaded.load(Ordering::Relaxed);
                        // 空响应体：服务器回 200 却一个字节都没给。镜像节点挂掉/重定向到
                        // 坏节点时就是这样（实测 modrinth.lenmei233.dpdns.org：200 + 空 body）。
                        // 无 Content-Length 时 `total` 为 0，下面的字节数比对覆盖不到，
                        // 必须显式判定——否则 0 字节文件被静默 rename 成正式文件，
                        // 直到上层解析才报错（如 zip 的 "Could not find EOCD"）。
                        if downloaded == 0 {
                            return Err(DownloadError::EmptyBody {
                                url: url.to_string(),
                            });
                        }
                        // 流式 EOF 校验：total 已知时必须字节数一致（chunked 截断兜底）
                        if total > 0 && downloaded != total {
                            return Err(DownloadError::Incomplete { expected: total, actual: downloaded });
                        }
                        // 该源确实交付了完整数据：解除可能存在的拉黑。
                        ctx.mark_host_alive(&host_of(url));
                        return Ok(());
                    }
                }
            }
            _ = watchdog_tick.tick() => {
                // 流式路径没有分段冗余：只保留 idle 急救，绝不因「慢」中止。
                if got_any {
                    if last_activity.elapsed() >= idle_timeout {
                        return Err(DownloadError::Exhausted(format!(
                            "看门狗: 流式下载无数据超过 {}s（url={url}）",
                            idle_timeout.as_secs()
                        )));
                    }
                } else if attempt_started.elapsed() >= first_byte_timeout {
                    return Err(DownloadError::Exhausted(format!(
                        "看门狗: 流式响应体首字节超时 {}s（url={url}）",
                        first_byte_timeout.as_secs()
                    )));
                }
            }
        }
    }
}

/// 依据各段「已写入水位」重算完成位图。
///
/// 段之间互不重叠（拆分只会沿边界切分），因此「块内各段覆盖长度之和 == 块长」
/// 就等价于该块已完整写入；这个判定是幂等的，重试/拆分都不会重复计数。
fn refresh_bitmap(meta: &mut crate::part_meta::PartMeta, segments: &HashMap<u32, Segment>) {
    for block in 0..meta.used_blocks() {
        let (b0, b1) = meta.block_range(block);
        let covered: u64 = segments
            .values()
            .map(|s| {
                let ws = s.start;
                let we = (s.start + s.downloaded.load(Ordering::Relaxed)).min(s.end + 1);
                if we <= b0 || ws >= b1 {
                    0
                } else {
                    we.min(b1) - ws.max(b0)
                }
            })
            .sum();
        meta.set_complete(block, covered == b1 - b0);
    }
}
/// 本地复用：按摘要找到同内容文件后复制到 `.part`，再走最终校验/rename（issue #11）。
///
/// 复制后仍调用 `finalize_verified`：命中只说明大小与摘要一致，最终校验不能省。
async fn try_local_reuse(ctx: &Arc<RunContext>, size: u64) -> Result<bool, DownloadError> {
    if ctx.task.reuse_dirs.is_empty() || (ctx.task.sha256.is_none() && ctx.task.sha1.is_none()) {
        return Ok(false);
    }
    let dirs = ctx.task.reuse_dirs.clone();
    let sha256 = ctx.task.sha256;
    let sha1 = ctx.task.sha1;
    let found = tokio::task::spawn_blocking(move || {
        crate::local_reuse::find_match(&dirs, size, sha256, sha1)
    })
    .await
    .ok()
    .flatten();
    let Some(src) = found else {
        return Ok(false);
    };
    let part = ctx.task.part_path();
    if let Some(parent) = part.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(&src, &part).await?;
    ctx.stats.total.store(size, Ordering::Relaxed);
    ctx.stats.downloaded.store(size, Ordering::Relaxed);
    finalize_verified(
        &part,
        &ctx.task.dest,
        ctx.task.sha256,
        ctx.task.sha1,
        ctx.task.expected_size,
    )
    .await?;
    ctx.log(
        LogLevel::Info,
        format!("本地复用: {} -> {}", src.display(), ctx.task.dest.display()),
    );
    Ok(true)
}
/// 目标盘空间预检（issue #9）。
///
/// 峰值占用 = 尚未 rename 的 `.part`（total）+ 最终文件（total），按 1.1 倍留余量。
/// 预检是尽力而为：拿不到可用空间（路径不存在等）时不阻断下载。
fn ensure_disk_space(part: &Path, total: u64) -> Result<(), DownloadError> {
    let dir = part.parent().unwrap_or_else(|| Path::new("."));
    let Ok(available) = fs2::available_space(dir) else {
        return Ok(());
    };
    let required = total.saturating_add(total / 10);
    if available < required {
        return Err(DownloadError::InsufficientSpace {
            required,
            available,
        });
    }
    Ok(())
}
// ================= 动态拆分 =================

/// 动态拆分：周期性检查，把剩余字节最多的活跃段一分为二（并行度提升）。
/// 段任务被 abort 后已写字节保留，新段从拆分点继续。
/// 下限 2MB（对齐 aria2 `--min-split-size` 思路）：拆分要付出「abort 在途连接 +
/// 新建 TCP/TLS 握手 + abort 瞬间在途数据重下」的代价，剩余不足兆级的段拆了
/// 只会更慢——实测 64KB 下限时尾段被反复拆碎，末段吞吐从 ~10MB/s 掉到 <1MB/s。
async fn try_split(
    ctx: &Arc<RunContext>,
    segments: &mut HashMap<u32, Segment>,
    seg_handles: &mut HashMap<u32, AbortHandle>,
    seg_counter: Arc<AtomicU32>,
) {
    const MIN_SPLIT_REMAINING: u64 = 2 * 1024 * 1024;
    let target = segments
        .values()
        .filter(|s| seg_handles.contains_key(&s.index) && !s.finished())
        .max_by_key(|s| s.len().saturating_sub(s.downloaded.load(Ordering::Relaxed)))
        .cloned();
    let Some(seg) = target else {
        return;
    };
    if seg
        .len()
        .saturating_sub(seg.downloaded.load(Ordering::Relaxed))
        < MIN_SPLIT_REMAINING
    {
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
    ctx.log(LogLevel::Info, format!("动态拆分: 段 {} → 2 段", seg.index));
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

/// 从 URL 提取主机名（host），用于按主机缓存探测结果。
pub(crate) fn host_of(url: &str) -> String {
    url.split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or(url)
        .to_string()
}

fn backoff(opts: &DownloadOptions, attempt: u32) -> Duration {
    let base = opts.retry_base_delay.as_millis().max(1) as u64;
    let exp = base * (1u64 << attempt.min(6));
    Duration::from_millis(exp + rand::rng().random_range(0..=250))
}

/// 看门狗快照：把「真死 / 误杀 / 整体限速」区分开所需的全部字段。
#[allow(clippy::too_many_arguments)]
fn watchdog_snapshot(
    seg: &Segment,
    url: &str,
    from: u64,
    got: u64,
    inst: f64,
    smooth: f64,
    peer: Option<f64>,
    active: u32,
    remaining: u64,
) -> String {
    let peer_text = peer.map(human_bps).unwrap_or_else(|| "n/a".to_string());
    let ratio = peer.map_or(1.0, |p| if p > 0.0 { smooth / p } else { 1.0 });
    format!(
        "instant={} smooth={} peer_median={peer_text} ratio={ratio:.2} bytes={}/{} seg={}-{} from={} remaining={} active_segs={active} url={url}",
        human_bps(inst),
        human_bps(smooth),
        human_bytes(got),
        human_bytes(seg.len()),
        seg.start,
        seg.end,
        from,
        human_bytes(remaining),
    )
}

fn human_bps(bps: f64) -> String {
    if !bps.is_finite() || bps <= 0.0 {
        return "0B/s".to_string();
    }
    format!("{}/s", human_bytes(bps as u64))
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

/// reqwest 的 Display 通常只有「error sending request for url (...)」，
/// 真正的 DNS/TLS/连接重置原因在 source 链里。把整条链拼出来，用户日志才有信息量。
fn describe_reqwest_error(error: &reqwest::Error) -> String {
    use std::error::Error as _;
    let mut out = error.to_string();
    let mut source = error.source();
    let mut depth = 0;
    while let Some(cause) = source {
        if depth >= 4 {
            break;
        }
        out.push_str(" <- ");
        out.push_str(&cause.to_string());
        source = cause.source();
        depth += 1;
    }
    out
}

fn content_length(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

/// 该状态码是否值得在同一源上重试。
///
/// 4xx 里只有 408（请求超时）与 429（限流）属于瞬态；其余是路径不存在/无权限，
/// 重试同一源只会空转（issue #7）。5xx 与网络错误按瞬态处理，走指数退避。
fn is_retryable_status(code: u16) -> bool {
    !(400..500).contains(&code) || code == 408 || code == 429
}

/// 解析 Retry-After（仅支持秒数形式；HTTP-date 形式忽略）。
fn retry_after_secs(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// 解析 `Content-Range: bytes start-end/total`（total 允许为 `*`）。
fn parse_content_range(headers: &reqwest::header::HeaderMap) -> Option<(u64, u64, Option<u64>)> {
    let v = headers.get(reqwest::header::CONTENT_RANGE)?.to_str().ok()?;
    let rest = v.trim().strip_prefix("bytes")?.trim();
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((
        start.trim().parse().ok()?,
        end.trim().parse().ok()?,
        total.trim().parse().ok(),
    ))
}

fn accept_ranges_bytes(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get(reqwest::header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().contains("bytes"))
        .unwrap_or(false)
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
    f.sync_all()
        .await
        .map_err(|e| DownloadError::Other(format!("finalize fsync {part:?} 失败: {e}")))?;
    drop(f);
    match tokio::fs::rename(part, dest).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = tokio::fs::remove_file(dest).await;
            tokio::fs::rename(part, dest).await.map_err(|e2| {
                DownloadError::Other(format!("rename {part:?} -> {dest:?} 失败: {e} / {e2}"))
            })
        }
    }
}

/// finalize + 内容校验（SHA-256 / SHA-1 / 期望大小）。
///
/// 摘要优先于大小：有摘要时大小不符必然也表现为摘要不符，再比一次大小只是重复判定。
/// 反过来只有大小可用时也必须校验——镜像挂掉时最常见的坏字节就是截断的 body 或
/// 一段错误页，它们都能通过 HTTP 层（200 + 完整 Content-Length）。
///
/// 校验失败返回 `ChecksumMismatch`，由 run() 清理 `.part`、换源并重下一次。
async fn finalize_verified(
    part: &Path,
    dest: &Path,
    sha256: Option<[u8; 32]>,
    sha1: Option<[u8; 20]>,
    expected_size: Option<u64>,
) -> Result<(), DownloadError> {
    if let Some(expected) = sha256 {
        let actual = sha256_file(part).await?;
        if actual != expected {
            return Err(DownloadError::ChecksumMismatch {
                expected: hex_encode(&expected),
                actual: hex_encode(&actual),
            });
        }
    } else if let Some(expected) = sha1 {
        let actual = sha1_file(part).await?;
        if actual != expected {
            return Err(DownloadError::ChecksumMismatch {
                expected: hex_encode(&expected),
                actual: hex_encode(&actual),
            });
        }
    } else if let Some(expected) = expected_size {
        let actual = tokio::fs::metadata(part).await?.len();
        if actual != expected {
            return Err(DownloadError::ChecksumMismatch {
                expected: format!("{expected} 字节"),
                actual: format!("{actual} 字节"),
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

/// 流式计算文件 SHA-1（Minecraft 仓库与整合包清单用它）。
async fn sha1_file(path: &Path) -> Result<[u8; 20], DownloadError> {
    use sha1::{Digest, Sha1};
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha1::new();
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

#[cfg(test)]
mod tests {
    use super::{
        ensure_disk_space, is_retryable_status, normalize_dest, parse_content_range, probe_key,
        retry_after_secs,
    };
    use crate::DownloadError;
    use std::path::Path;

    #[test]
    fn probe_key_includes_scheme_and_port() {
        assert_eq!(probe_key("https://a.example/x"), "https://a.example:443");
        assert_eq!(
            probe_key("http://a.example:8080/x"),
            "http://a.example:8080"
        );
        // 同 host 不同 scheme/端口不再共用缓存（issue #8）
        assert_ne!(
            probe_key("https://a.example/x"),
            probe_key("http://a.example/x")
        );
    }

    #[test]
    fn parse_content_range_reads_start_end_total() {
        let mut h = reqwest::header::HeaderMap::new();
        h.insert(
            reqwest::header::CONTENT_RANGE,
            "bytes 100-199/1000".parse().unwrap(),
        );
        assert_eq!(parse_content_range(&h), Some((100, 199, Some(1000))));
        h.insert(
            reqwest::header::CONTENT_RANGE,
            "bytes 0-9/*".parse().unwrap(),
        );
        assert_eq!(parse_content_range(&h), Some((0, 9, None)));
        h.remove(reqwest::header::CONTENT_RANGE);
        assert_eq!(parse_content_range(&h), None);
    }

    #[test]
    fn retryable_status_fast_fails_on_4xx_except_408_429() {
        // issue #7：路径不存在/无权限重试同一源没有意义
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(403));
        assert!(!is_retryable_status(404));
        assert!(!is_retryable_status(410));
        assert!(is_retryable_status(408));
        assert!(is_retryable_status(429));
        // 5xx 与 3xx 仍按瞬态处理
        assert!(is_retryable_status(500));
        assert!(is_retryable_status(503));
    }

    #[test]
    fn retry_after_parses_seconds_only() {
        let mut h = reqwest::header::HeaderMap::new();
        assert_eq!(retry_after_secs(&h), None);
        h.insert(reqwest::header::RETRY_AFTER, "12".parse().unwrap());
        assert_eq!(retry_after_secs(&h), Some(12));
        // HTTP-date 形式不解析（返回 None，调用方按默认退避处理）
        h.insert(
            reqwest::header::RETRY_AFTER,
            "Wed, 21 Oct 2026 07:28:00 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after_secs(&h), None);
    }

    #[test]
    fn disk_precheck_rejects_impossible_size() {
        let part = std::env::temp_dir().join("qml-space-check.part");
        match ensure_disk_space(&part, u64::MAX) {
            Err(DownloadError::InsufficientSpace {
                required,
                available,
            }) => {
                assert!(required > 0);
                assert!(available < required);
            }
            other => panic!("应提前判定空间不足，实际: {other:?}"),
        }
    }

    #[test]
    fn normalize_dest_folds_case_on_windows() {
        let a = normalize_dest(Path::new("C:/Games/MC/A.jar"));
        let b = normalize_dest(Path::new("C:/Games/MC/a.jar"));
        #[cfg(windows)]
        assert_eq!(a, b, "Windows 上仅大小写不同的目标必须视为同一路径");
        #[cfg(not(windows))]
        assert_ne!(a, b);
    }
}
