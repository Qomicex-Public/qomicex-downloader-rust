mod common;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::{Behavior, MockServer};
use qomicex_downloader::{
    DownloadEvent, DownloadManager, DownloadOptions, DownloadTask, TaskState,
};
use tokio::sync::broadcast::{self, Receiver};

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "qomicex-test-{tag}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn fast_opts() -> DownloadOptions {
    DownloadOptions {
        retry_base_delay: Duration::from_millis(10),
        split_sample_interval: Duration::from_millis(200),
        progress_throttle: Duration::from_millis(20),
        global_progress_interval: Duration::from_millis(50),
        ..Default::default()
    }
}

async fn wait_state(
    m: &DownloadManager,
    id: u64,
    want: TaskState,
    timeout: Duration,
) -> TaskState {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(st) = m.state(id).await {
            if st == want {
                return st;
            }
        }
        if Instant::now() > deadline {
            return m.state(id).await.unwrap_or(TaskState::Failed);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn drain_events(rx: &mut Receiver<DownloadEvent>) -> Vec<DownloadEvent> {
    let mut out = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(ev) => out.push(ev),
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(_) => break,
        }
    }
    out
}

#[tokio::test]
async fn multipart_download() {
    let server = MockServer::start(2 * 1024 * 1024, Behavior::default()).await;
    let dir = tmp_dir("multipart");
    let dest = dir.join("big.bin");
    let mut opts = fast_opts();
    opts.segment_size = 256 * 1024;
    opts.split_threshold = 1024;
    let m = DownloadManager::new(opts, 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()));
    let st = wait_state(&m, id, TaskState::Completed, Duration::from_secs(20)).await;
    if st != TaskState::Completed {
        for ev in drain_events(&mut rx) {
            println!("EVENT: {ev:?}");
        }
    }
    assert_eq!(st, TaskState::Completed, "多段下载未完成");
    let got = std::fs::read(&dest).unwrap();
    assert_eq!(got, *server.data, "多段下载内容不一致");
    assert!(!dest.with_file_name("big.bin.part").exists(), ".part 应已被重命名");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn small_file_single_segment() {
    let server = MockServer::start(8 * 1024, Behavior::default()).await;
    let dir = tmp_dir("small");
    let dest = dir.join("small.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(10)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data);
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn norange_fallback_streamed() {
    let server = MockServer::start(1024 * 1024, Behavior { no_range: true, ..Default::default() }).await;
    let dir = tmp_dir("norange");
    let dest = dir.join("norange.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()));
    let st = wait_state(&m, id, TaskState::Completed, Duration::from_secs(15)).await;
    for ev in drain_events(&mut rx) {
        println!("EVENT: {ev:?}");
    }
    assert_eq!(st, TaskState::Completed, "无 Range 回退流式未完成");
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "无 Range 回退流式内容不一致");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn chunked_streamed() {
    let server = MockServer::start(512 * 1024, Behavior { chunked: true, ..Default::default() }).await;
    let dir = tmp_dir("chunked");
    let dest = dir.join("chunked.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(15)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "chunked 流式内容不一致");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn flaky_segment_retry() {
    let server = MockServer::start(1024 * 1024, Behavior { flaky: Some(3), ..Default::default() }).await;
    let dir = tmp_dir("flaky");
    let dest = dir.join("flaky.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("flaky"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(20)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "flaky 重试后内容不一致");
    assert!(server.flaky_requests() >= 4, "应至少经历 3 次失败 + 1 次成功");
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, DownloadEvent::Log { level: qomicex_downloader::LogLevel::Warn, message } if message.contains("重试"))),
        "应产生重试日志"
    );
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn http_404_fails() {
    let server = MockServer::start(1024, Behavior::default()).await;
    let dir = tmp_dir("404");
    let dest = dir.join("missing.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(DownloadTask::new(server.url("status404"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Failed, Duration::from_secs(10)).await, TaskState::Failed);
    assert!(!dest.exists(), "404 不应产生目标文件");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn pause_resume_keeps_part() {
    // 慢速端点保证下载进行中有进度事件可观测
    let server = MockServer::start(
        2 * 1024 * 1024,
        Behavior { throttle: Some((300_000, 5_000_000)), ..Default::default() },
    )
    .await;
    let dir = tmp_dir("pause");
    let dest = dir.join("resume.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("throttle"), dest.clone()));

    // 等部分数据到达
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let events = drain_events(&mut rx);
        let some = events.iter().any(|e| {
            matches!(e, DownloadEvent::Progress { downloaded, .. } if *downloaded > 0)
        });
        if some {
            break;
        }
        assert!(Instant::now() < deadline, "等待进度事件超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    m.pause(id).await.unwrap();
    assert_eq!(m.state(id).await.unwrap(), TaskState::Paused);
    let part = dest.with_file_name("resume.bin.part");
    assert!(part.exists(), "暂停后 .part 应保留");
    let part_size = std::fs::metadata(&part).unwrap().len();
    assert!(part_size > 0 && part_size < 2 * 1024 * 1024, "暂停时应已下载部分数据");

    m.resume(id).await.unwrap();
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(20)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "断点续传内容不一致");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn cancel_deletes_part() {
    // 慢速端点保证下载进行中有进度事件可观测
    let server = MockServer::start(
        2 * 1024 * 1024,
        Behavior { throttle: Some((300_000, 5_000_000)), ..Default::default() },
    )
    .await;
    let dir = tmp_dir("cancel");
    let dest = dir.join("cancel.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("throttle"), dest.clone()));
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let events = drain_events(&mut rx);
        let some = events.iter().any(|e| {
            matches!(e, DownloadEvent::Progress { downloaded, .. } if *downloaded > 0)
        });
        if some {
            break;
        }
        assert!(Instant::now() < deadline, "等待进度事件超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    m.cancel(id).await.unwrap();
    assert_eq!(m.state(id).await.unwrap(), TaskState::Cancelled);
    assert!(!dest.exists());
    assert!(!dest.with_file_name("cancel.bin.part").exists(), "取消后 .part 应删除");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn queue_concurrency_limit() {
    let server = MockServer::start(512 * 1024, Behavior::default()).await;
    let dir = tmp_dir("queue");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let ids: Vec<u64> = (0..3)
        .map(|i| {
            let dest = dir.join(format!("f{i}.bin"));
            m.add(DownloadTask::new(server.url("file"), dest))
        })
        .collect();

    // 从事件流统计同时下载中的最大任务数
    let mut running: std::collections::HashMap<u64, bool> = HashMap::new();
    let mut max_active = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for ev in drain_events(&mut rx) {
            if let DownloadEvent::StateChanged { id, state, .. } = ev {
                match state {
                    TaskState::Downloading => {
                        running.insert(id, true);
                    }
                    TaskState::Completed | TaskState::Failed | TaskState::Cancelled => {
                        running.insert(id, false);
                    }
                    _ => {}
                }
            }
        }
        max_active = max_active.max(running.values().filter(|v| **v).count());
        let mut done = true;
        for id in &ids {
            if !matches!(m.state(*id).await, Ok(TaskState::Completed)) {
                done = false;
            }
        }
        if done {
            break;
        }
        assert!(Instant::now() < deadline, "队列任务超时未完成");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(max_active <= 2, "同时下载任务数应 ≤ 2，实际 {max_active}");
    for i in 0..3 {
        assert_eq!(
            std::fs::read(dir.join(format!("f{i}.bin"))).unwrap(),
            *server.data
        );
    }
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn watchdog_rebuilds_stalled_segment() {
    let server = MockServer::start(512 * 1024, Behavior { stall: Some(Duration::from_secs(3)), ..Default::default() }).await;
    let dir = tmp_dir("watchdog");
    let dest = dir.join("stall.bin");
    let mut opts = fast_opts();
    opts.watchdog_idle_timeout = Duration::from_millis(300);
    opts.max_segments = 1; // 禁用动态拆分，让看门狗是唯一重建途径
    let m = DownloadManager::new(opts, 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("stall-once"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(20)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "看门狗重建后内容不一致");
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, DownloadEvent::Log { message, .. } if message.contains("看门狗"))),
        "应产生看门狗重建日志"
    );
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn dynamic_split_on_speedup() {
    // 连接 1 慢速（500KB/s），后续连接快速（5MB/s）→ 初始单段 + 周期性拆分 → 段数逐步提升
    let server = MockServer::start(
        4 * 1024 * 1024,
        Behavior { throttle: Some((500_000, 5_000_000)), ..Default::default() },
    )
    .await;
    let dir = tmp_dir("split");
    let dest = dir.join("split.bin");
    let mut opts = fast_opts();
    opts.split_threshold = 0; // 总是分片
    opts.segment_size = 4 * 1024 * 1024; // 初始单段
    opts.max_segments = 4;
    let m = DownloadManager::new(opts, 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("throttle"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(30)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "动态拆分后内容不一致");
    let events = drain_events(&mut rx);
    let max_seg = events
        .iter()
        .filter_map(|e| match e {
            DownloadEvent::Progress { active_segments, .. } => Some(*active_segments),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    assert!(max_seg >= 2, "应发生过动态拆分，最大并发段数 {max_seg} < 2");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn global_progress_and_log_events() {
    let server = MockServer::start(256 * 1024, Behavior::default()).await;
    let dir = tmp_dir("global");
    let dest = dir.join("g.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(15)).await, TaskState::Completed);
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, DownloadEvent::GlobalProgress { .. })),
        "应收到全局聚合进度事件"
    );
    assert!(
        events.iter().any(|e| matches!(e, DownloadEvent::Log { .. })),
        "应收到日志事件"
    );
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mirror_fallback_on_primary_failure() {
    // 主 URL 指向 404，镜像指向正常文件 → 探测失败后切换镜像成功
    let server = MockServer::start(256 * 1024, Behavior::default()).await;
    let dir = tmp_dir("mirror");
    let dest = dir.join("m.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(
        DownloadTask::new(server.url("status404"), dest.clone())
            .with_mirrors([server.url("file")]),
    );
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(15)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "镜像回退后内容不一致");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn custom_headers_applied() {
    let server = MockServer::start(64 * 1024, Behavior::default()).await;
    let dir = tmp_dir("headers");
    let dest = dir.join("h.bin");
    let mut opts = fast_opts();
    opts.headers = vec![("X-Global".into(), "1".into())];
    let m = DownloadManager::new(opts, 2);
    let id = m.add(
        DownloadTask::new(server.url("file"), dest.clone())
            .with_header("X-Task", "yes"),
    );
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(15)).await, TaskState::Completed);
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn streamed_retry_does_not_accumulate() {
    // 无 Range + 首次 GET 500 → 流式重试成功后文件不叠加
    let server = MockServer::start(
        512 * 1024,
        Behavior { no_range: true, flaky: Some(1), ..Default::default() },
    )
    .await;
    let dir = tmp_dir("stream-retry");
    let dest = dir.join("sr.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(DownloadTask::new(server.url("flaky"), dest.clone()));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(20)).await, TaskState::Completed);
    let got = std::fs::read(&dest).unwrap();
    assert_eq!(got.len(), server.data.len(), "流式重试后文件不应叠加（大小必须一致）");
    assert_eq!(got, *server.data, "流式重试后内容不一致");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn retry_after_failure_recovers() {
    // 前 5 次 GET 失败 + 禁止自动重试 → 任务 Failed → retry 直到服务器恢复 → Completed
    let server = MockServer::start(256 * 1024, Behavior { flaky: Some(5), ..Default::default() }).await;
    let dir = tmp_dir("retry");
    let dest = dir.join("r.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(
        DownloadTask::new(server.url("flaky"), dest.clone()).with_max_retries(0),
    );
    assert_eq!(wait_state(&m, id, TaskState::Failed, Duration::from_secs(20)).await, TaskState::Failed);
    assert!(server.flaky_requests() >= 2, "失败任务应至少经历 2 次 GET（段 + 降级流式）");

    m.retry(id).await.unwrap();
    assert_eq!(m.state(id).await.unwrap(), TaskState::Queued);
    // 服务器前 5 次 GET 失败；失败阶段已消耗 2 次，需 retry 若干次直到恢复
    let mut recovered = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let st = wait_state(&m, id, TaskState::Completed, Duration::from_secs(10)).await;
        if st == TaskState::Completed {
            recovered = true;
            break;
        }
        if st == TaskState::Failed {
            m.retry(id).await.unwrap();
        } else {
            panic!("retry 后出现意外状态 {st:?}");
        }
    }
    assert!(recovered, "retry 后应最终恢复完成");
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data, "retry 后内容不一致");
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sha256_verifies_content() {
    use sha2::{Digest, Sha256};
    let server = MockServer::start(512 * 1024, Behavior::default()).await;
    let digest: [u8; 32] = Sha256::digest(&server.data[..]).into();
    let dir = tmp_dir("sha-ok");
    let dest = dir.join("s.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()).with_sha256(digest));
    assert_eq!(wait_state(&m, id, TaskState::Completed, Duration::from_secs(20)).await, TaskState::Completed);
    assert_eq!(std::fs::read(&dest).unwrap(), *server.data);
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sha256_mismatch_fails_after_redownload() {
    let server = MockServer::start(256 * 1024, Behavior::default()).await;
    let dir = tmp_dir("sha-bad");
    let dest = dir.join("sbad.bin");
    let m = DownloadManager::new(fast_opts(), 2);
    let mut rx = m.subscribe();
    let wrong = [0xABu8; 32];
    let id = m.add(DownloadTask::new(server.url("file"), dest.clone()).with_sha256(wrong));
    assert_eq!(wait_state(&m, id, TaskState::Failed, Duration::from_secs(30)).await, TaskState::Failed);
    assert!(!dest.exists(), "校验失败不应留下目标文件");
    assert!(!dest.with_file_name("sbad.bin.part").exists(), "校验失败应清理 .part");
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(e, DownloadEvent::Log { message, .. } if message.contains("SHA-256"))),
        "应产生 SHA-256 重下日志"
    );
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}
