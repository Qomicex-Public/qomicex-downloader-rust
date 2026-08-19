use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;

use tokio::sync::{broadcast, Mutex, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::{DownloadStats, Engine, RunContext};
use crate::error::DownloadError;
use crate::task::{DownloadEvent, DownloadOptions, DownloadTask, TaskId, TaskState};

/// 任务条目：状态、取消令牌、worker 句柄与进度快照。
struct Entry {
    task: DownloadTask,
    state: StdRwLock<TaskState>,
    cancel: Mutex<CancellationToken>,
    handle: StdMutex<Option<JoinHandle<()>>>,
    stats: Arc<DownloadStats>,
}

/// 任务进度快照（从原子统计字段读取，供轮询/桥接层使用）。
#[derive(Clone, Copy, Debug, Default)]
pub struct DownloadProgress {
    /// 已下载字节数。
    pub downloaded: u64,
    /// 总字节数（未知为 0）。
    pub total: u64,
    /// 当前活跃段数。
    pub active_segments: u32,
}

/// 全局下载管理器：队列 + 两级并发控制 + 三级进度事件。
pub struct DownloadManager {
    inner: Arc<Inner>,
}

struct Inner {
    options: Arc<DownloadOptions>,
    events: broadcast::Sender<DownloadEvent>,
    queue: StdMutex<VecDeque<TaskId>>,
    tasks: StdRwLock<HashMap<TaskId, Arc<Entry>>>,
    next_id: AtomicU64,
    semaphore: Arc<Semaphore>,
    engine: Engine,
    /// HTTP/2 客户端（常备）。
    h2: reqwest::Client,
    /// HTTP/3 客户端（可选）。
    h3: Option<reqwest::Client>,
    /// 队列派发唤醒通知。
    dispatch: Arc<Notify>,
    /// 按宿主的探测结果缓存（host → 支持 Range？）。同一 CDN 主机的大量小文件
    /// 只探测一次，后续任务直接下载，避免每个文件一次 HEAD（Java 启动器同款行为）。
    host_probe: Arc<StdRwLock<HashMap<String, bool>>>,
    /// 全局进度聚合任务。
    aggregator: StdMutex<Option<JoinHandle<()>>>,
    /// 队列派发任务。
    dispatcher: StdMutex<Option<JoinHandle<()>>>,
}

impl DownloadManager {
    /// 创建管理器。`max_concurrent` 为全局并发任务数上限（worker 级）。
    pub fn new(options: DownloadOptions, max_concurrent: usize) -> Self {
        let (events, _) = broadcast::channel(1024);
        let options = Arc::new(options);
        let (h2, h3) = build_clients(&options, max_concurrent);
        let inner = Arc::new(Inner {
            options,
            events,
            queue: StdMutex::new(VecDeque::new()),
            tasks: StdRwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1))),
            engine: Engine::new(),
            h2,
            h3,
            dispatch: Arc::new(Notify::new()),
            host_probe: Arc::new(StdRwLock::new(HashMap::new())),
            aggregator: StdMutex::new(None),
            dispatcher: StdMutex::new(None),
        });
        let manager = Self { inner: inner.clone() };
        manager.start_aggregator();
        manager.start_dispatcher();
        manager
    }

    /// 订阅事件流（三级进度：任务级/全局/日志）。
    pub fn subscribe(&self) -> broadcast::Receiver<DownloadEvent> {
        self.inner.events.subscribe()
    }

    /// 加入队列并返回任务 ID（并发有空位立即启动，否则排队）。
    pub fn add(&self, mut task: DownloadTask) -> TaskId {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        task.id = id;
        let entry = Arc::new(Entry {
            task,
            state: StdRwLock::new(TaskState::Queued),
            cancel: Mutex::new(CancellationToken::new()),
            handle: StdMutex::new(None),
            stats: Arc::new(DownloadStats {
                downloaded: AtomicU64::new(0),
                total: AtomicU64::new(0),
                active_segments: AtomicU32::new(0),
            }),
        });
        self.inner.tasks.write().unwrap().insert(id, entry);
        let _ = self.inner.events.send(DownloadEvent::StateChanged {
            id,
            state: TaskState::Queued,
            detail: None,
        });
        self.inner.queue.lock().unwrap().push_back(id);
        self.inner.dispatch.notify_one();
        id
    }

    /// 暂停下载中任务（`.part` 保留，可 `resume` 续传）。
    pub async fn pause(&self, id: TaskId) -> Result<(), DownloadError> {
        let entry = self.entry(id)?;
        let cur = *entry.state.read().unwrap();
        match cur {
            TaskState::Completed | TaskState::Cancelled => {
                return Err(DownloadError::Other(format!(
                    "任务已终结（{cur:?}），无法暂停"
                )));
            }
            TaskState::Queued => {
                // 排队中：直接出队即可（worker 未启动，无需取消）
                self.inner.queue.lock().unwrap().retain(|x| *x != id);
            }
            _ => {
                entry.cancel.lock().await.cancel();
            }
        }
        *entry.state.write().unwrap() = TaskState::Paused;
        let handle = entry.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
        let _ = self.inner.events.send(DownloadEvent::StateChanged {
            id,
            state: TaskState::Paused,
            detail: None,
        });
        Ok(())
    }

    /// 恢复暂停任务（若并发满则排队）。
    pub async fn resume(&self, id: TaskId) -> Result<(), DownloadError> {
        let entry = self.entry(id)?;
        {
            let mut st = entry.state.write().unwrap();
            if *st != TaskState::Paused {
                return Err(DownloadError::Other(format!(
                    "任务状态为 {:?}，不可恢复",
                    *st
                )));
            }
            *st = TaskState::Queued;
        }
        *entry.cancel.lock().await = CancellationToken::new();
        self.inner.queue.lock().unwrap().push_back(id);
        self.inner.dispatch.notify_one();
        Ok(())
    }

    /// 重试失败/取消的任务（`.part` 保留则断点续传，否则从头下载）。
    pub async fn retry(&self, id: TaskId) -> Result<(), DownloadError> {
        let entry = self.entry(id)?;
        {
            let mut st = entry.state.write().unwrap();
            match *st {
                TaskState::Failed | TaskState::Cancelled => {}
                TaskState::Downloading | TaskState::Queued => {
                    return Err(DownloadError::Other("任务进行中，无法重试".into()));
                }
                TaskState::Completed => {
                    return Err(DownloadError::Other("任务已完成，无需重试".into()));
                }
                TaskState::Paused => {
                    return Err(DownloadError::Other("任务已暂停，请用 resume".into()));
                }
            }
            *st = TaskState::Queued;
        }
        *entry.cancel.lock().await = CancellationToken::new();
        self.inner.queue.lock().unwrap().push_back(id);
        self.inner.dispatch.notify_one();
        Ok(())
    }

    /// 取消任务（删除 `.part`，不可恢复）。
    pub async fn cancel(&self, id: TaskId) -> Result<(), DownloadError> {
        let entry = self.entry(id)?;
        {
            let mut st = entry.state.write().unwrap();
            *st = TaskState::Cancelled;
        }
        self.inner.queue.lock().unwrap().retain(|x| *x != id);
        entry.cancel.lock().await.cancel();
        let handle = entry.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
        let _ = tokio::fs::remove_file(entry.task.part_path()).await;
        let _ = self.inner.events.send(DownloadEvent::StateChanged {
            id,
            state: TaskState::Cancelled,
            detail: None,
        });
        Ok(())
    }

    /// 移除任务（取消并清理）。
    pub async fn remove(&self, id: TaskId) -> Result<(), DownloadError> {
        self.cancel(id).await?;
        self.inner.tasks.write().unwrap().remove(&id);
        Ok(())
    }

    /// 查询任务状态。
    pub async fn state(&self, id: TaskId) -> Result<TaskState, DownloadError> {
        Ok(*self.entry(id)?.state.read().unwrap())
    }

    /// 查询任务进度快照（已下载/总量/活跃段数）。
    pub async fn progress(&self, id: TaskId) -> Result<DownloadProgress, DownloadError> {
        let entry = self.entry(id)?;
        Ok(DownloadProgress {
            downloaded: entry.stats.downloaded.load(Ordering::Relaxed),
            total: entry.stats.total.load(Ordering::Relaxed),
            active_segments: entry.stats.active_segments.load(Ordering::Relaxed),
        })
    }

    /// 任务列表（ID + 状态）。
    pub async fn list(&self) -> Vec<(TaskId, TaskState)> {
        let tasks = self.inner.tasks.read().unwrap();
        let mut out: Vec<(TaskId, TaskState)> = tasks
            .iter()
            .map(|(id, e)| (*id, *e.state.read().unwrap()))
            .collect();
        out.sort_by_key(|(id, _)| *id);
        out
    }

    /// 关闭：取消全部任务并等待 worker 退出。
    pub async fn shutdown(&self) {
        let entries: Vec<Arc<Entry>> = self
            .inner
            .tasks
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for e in &entries {
            e.cancel.lock().await.cancel();
        }
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        for e in &entries {
            if let Some(h) = e.handle.lock().unwrap().take() {
                handles.push(h);
            }
        }
        for h in handles {
            let _ = h.await;
        }
        if let Some(agg) = self.inner.aggregator.lock().unwrap().take() {
            agg.abort();
        }
        if let Some(disp) = self.inner.dispatcher.lock().unwrap().take() {
            disp.abort();
        }
    }

    // ---------------- 内部 ----------------

    fn entry(&self, id: TaskId) -> Result<Arc<Entry>, DownloadError> {
        self.inner
            .tasks
            .read()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(DownloadError::TaskNotFound(id))
    }

    /// 全局进度聚合循环。
    fn start_aggregator(&self) {
        let inner = self.inner.clone();
        let interval = inner.options.global_progress_interval;
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let tasks = inner.tasks.read().unwrap();
                let mut downloaded = 0u64;
                let mut total = 0u64;
                let mut active = 0u32;
                for e in tasks.values() {
                    downloaded += e.stats.downloaded.load(Ordering::Relaxed);
                    total += e.stats.total.load(Ordering::Relaxed);
                    if *e.state.read().unwrap() == TaskState::Downloading {
                        active += 1;
                    }
                }
                drop(tasks);
                let _ = inner.events.send(DownloadEvent::GlobalProgress {
                    active_tasks: active,
                    downloaded,
                    total,
                });
            }
        });
        *self.inner.aggregator.lock().unwrap() = Some(handle);
    }

    /// 队列派发循环：尽量消耗队列，名额不足时挂起等待通知。
    fn start_dispatcher(&self) {
        let inner = self.inner.clone();
        let handle = tokio::spawn(dispatch_loop(inner));
        *self.inner.dispatcher.lock().unwrap() = Some(handle);
    }
}

/// 构建 HTTP 客户端对：(HTTP/2 常备, 可选 HTTP/3)。
/// HTTP/2 调优默认开启；`enable_http3` 且编译期启用 `http3` feature 时额外
/// 构建 HTTP/3-only 客户端用于运行时优先连接（失败自动回退 HTTP/2）。
fn build_clients(
    options: &DownloadOptions,
    max_concurrent: usize,
) -> (reqwest::Client, Option<reqwest::Client>) {
    let h2 = reqwest::Client::builder()
        .timeout(options.timeout)
        .connect_timeout(options.connect_timeout)
        .pool_idle_timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(max_concurrent.clamp(1, 32))
        .user_agent(&options.user_agent)
        .http2_adaptive_window(true)
        // 大帧需合法（上限 2^24-1=16_777_215）。激进 h2 配置是本方案实测最优
        //（5-7MB/s）；默认 h2/h1/更高并发反而更慢，勿再改动。
        .http2_max_frame_size((16 * 1024 * 1024) - 1)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .build()
        .expect("构建 HTTP 客户端失败");

    #[cfg(feature = "http3")]
    if options.enable_http3 {
        if let Ok(c) = reqwest::Client::builder()
            .timeout(options.timeout)
            .connect_timeout(options.connect_timeout)
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(max_concurrent.clamp(1, 32))
            .user_agent(&options.user_agent)
            .http3_prior_knowledge()
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()
        {
            return (h2, Some(c));
        }
    }

    (h2, None)
}

/// 派发循环：耗尽队列或信号量后挂起，等待 add/resume/worker 完成通知。
async fn dispatch_loop(inner: Arc<Inner>) {
    loop {
        loop {
            let id = match inner.queue.lock().unwrap().pop_front() {
                Some(id) => id,
                None => break,
            };
            let entry = match inner.tasks.read().unwrap().get(&id).cloned() {
                Some(e) => e,
                None => continue,
            };
            let permit = match inner.semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    inner.queue.lock().unwrap().push_front(id);
                    break;
                }
            };
            let run_inner = inner.clone();
            let run_entry = entry.clone();
            let handle = tokio::spawn(async move {
                worker(&run_inner, &run_entry, permit).await;
            });
            *entry.handle.lock().unwrap() = Some(handle);
        }
        inner.dispatch.notified().await;
    }
}

/// 单任务 worker：状态机 + engine 执行 + 收尾 + 唤醒派发。
async fn worker(inner: &Arc<Inner>, entry: &Arc<Entry>, permit: tokio::sync::OwnedSemaphorePermit) {
    let id = entry.task.id;
    {
        let mut st = entry.state.write().unwrap();
        *st = TaskState::Downloading;
    }
    let _ = inner.events.send(DownloadEvent::StateChanged {
        id,
        state: TaskState::Downloading,
        detail: None,
    });

    // 合并全局默认 + 任务级请求头（任务级覆盖）
    let mut headers = reqwest::header::HeaderMap::new();
    for (k, v) in &inner.options.headers {
        if let (Ok(k), Ok(v)) = (
            k.parse::<reqwest::header::HeaderName>(),
            v.parse::<reqwest::header::HeaderValue>(),
        ) {
            headers.insert(k, v);
        }
    }
    for (k, v) in &entry.task.headers {
        if let (Ok(k), Ok(v)) = (
            k.parse::<reqwest::header::HeaderName>(),
            v.parse::<reqwest::header::HeaderValue>(),
        ) {
            headers.insert(k, v);
        }
    }
    let mut urls = Vec::with_capacity(entry.task.mirror_urls.len() + 1);
    urls.push(entry.task.url.clone());
    urls.extend(entry.task.mirror_urls.clone());

    let ctx = Arc::new(RunContext {
        task: entry.task.clone(),
        cancel: entry.cancel.lock().await.clone(),
        events: inner.events.clone(),
        headers,
        urls: StdMutex::new(urls),
        resolved_url: StdMutex::new(None),
        options: inner.options.clone(),
        stats: entry.stats.clone(),
        speeds: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        h2: inner.h2.clone(),
        h3: inner.h3.clone(),
        use_h3: AtomicBool::new(inner.h3.is_some()),
        host_probe: inner.host_probe.clone(),
    });

    let result = inner.engine.run(&ctx).await;
    // 任务失败：清掉该主机的探测缓存，下次重试仍会重新探测（避免缓存盖住临时故障）；
    // 成功则保留缓存，同主机的后续大量小文件直接下载、不再逐文件探测。
    if result.is_err() {
        let host = crate::engine::host_of(&entry.task.url);
        inner.host_probe.write().unwrap().remove(&host);
    }
    let (final_state, detail) = {
        let mut st = entry.state.write().unwrap();
        match result {
            Ok(()) => {
                *st = TaskState::Completed;
                (TaskState::Completed, None)
            }
            Err(DownloadError::Cancelled) => (*st, None),
            Err(e) => {
                *st = TaskState::Failed;
                (TaskState::Failed, Some(e.to_string()))
            }
        }
    };
    let _ = inner.events.send(DownloadEvent::StateChanged {
        id,
        state: final_state,
        detail,
    });
    drop(permit);
    // 唤醒派发循环接手队列中的下一个任务
    inner.dispatch.notify_one();
}
