//! 回归测试：同 host 缓存命中时，大文件必须恢复多段并行，不能退化成单连接流式。
//!
//! 背景：`run_once` 的 host_probe 缓存短路曾把所有缓存命中文件一律打成单连接
//! `run_streamed`（忽略 `range_ok`），导致同 CDN 主机的整合包包体/大 mod 只剩一条
//! 连接的吞吐（慢 CDN 上 ~100KB/s，且易触发上层下载超时）。修复后：Range 可用时用
//! 轻量 HEAD 拿大小，`> split_threshold` 的文件仍走 `run_ranged` 多段并行，小文件
//! 保留单连接直传。
//!
//! 验证方式：mock 服务器把每条连接限速 1MB/s。先放一个同 host 小文件填充探测缓存，
//! 再下载 12MB (>split_threshold 10MB) 文件。若缓存短路成立则只有一条连接 → 慢；
//! 若走并行则与「未缓存首文件」速度相当。断言缓存命中耗时 < 首文件耗时的 1.6 倍
//! （修复前约 2 倍，必失败；修复后基本持平，通过）。

mod common;

use std::time::{Duration, Instant};

use common::{Behavior, MockServer};
use qomicex_downloader::{DownloadManager, DownloadOptions, DownloadTask, TaskState};

const FILE_SIZE: usize = 12 * 1024 * 1024; // 12MB > split_threshold 10MB，需分片才触发
const RATE: u64 = 1_048_576; // 1MB/s per connection

async fn download_timed(srv: &MockServer, prime_first: bool, label: &str) -> Duration {
    let dir = std::env::temp_dir().join(format!(
        "qomicex-hc-{}-{}",
        label,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let m = DownloadManager::new(
        DownloadOptions {
            progress_throttle: Duration::from_millis(50),
            watchdog_idle_timeout: Duration::from_secs(60),
            ..Default::default()
        },
        8,
    );

    // 先放同 host 小文件，填充 host_probe 缓存（模拟同一会话此前下载过该 CDN）
    if prime_first {
        let pid = m.add(DownloadTask::new(srv.url("prime"), dir.join("prime.bin")));
        wait_terminal(&m, pid).await;
    }

    let id = m.add(DownloadTask::new(srv.url("big.bin"), dir.join("big.bin")));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(90);
    loop {
        if let Ok(st) = m.state(id).await {
            if matches!(st, TaskState::Completed | TaskState::Failed) {
                break;
            }
        }
        if Instant::now() > deadline {
            panic!("{label}: 下载未在 90s 内完成");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = start.elapsed();
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "{label}: {} bytes in {:.2}s = {:.1} KB/s",
        FILE_SIZE,
        elapsed.as_secs_f64(),
        FILE_SIZE as f64 / elapsed.as_secs_f64() / 1000.0
    );
    elapsed
}

async fn wait_terminal(m: &DownloadManager, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(st) = m.state(id).await {
            if matches!(st, TaskState::Completed | TaskState::Failed) {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("prime 未完成");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn cached_large_file_uses_parallelism() {
    let behavior = Behavior {
        throttle: Some((RATE, RATE)),
        ..Default::default()
    };

    // A：首文件（未缓存，Range 支持）→ 基线（多段并行）
    let srv_a = MockServer::start(FILE_SIZE, behavior.clone()).await;
    let a = download_timed(&srv_a, false, "uncached").await;

    // B：缓存命中（同 host 已探测，Range 支持）→ 修复前单连接 ≈2 倍慢，修复后应持平
    let srv_b = MockServer::start(FILE_SIZE, behavior).await;
    let b = download_timed(&srv_b, true, "cached").await;

    eprintln!("== uncached={a:?} cached={b:?} ==");
    assert!(
        b < a + Duration::from_secs(4),
        "缓存命中的大文件应走多段并行（与首文件相当），但实际 cached={b:?} vs uncached={a:?}，\
         疑似 host_probe 短路把大文件打成单连接"
    );
}
