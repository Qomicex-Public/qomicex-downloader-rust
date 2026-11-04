// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

//! 回归：全局段许可被别的任务占满时，新任务必须等待许可，不得降级整文件流式。
//!
//! 生产默认 `download_threads=64` + `max_total_segments=64`：64 个任务同时启动时，
//! 只有前几个能拿到段许可。旧实现把「拿不到许可」误判成段永久失败，删除 `.part`
//! 改走单连接流式（无断点续传、每次重试从 0 开始），是线上高并发下载失败的主因。
//! 本用例把该场景缩小到 2 任务 / 1 许可。

mod common;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::{Behavior, MockServer};
use qomicex_downloader::{
    DownloadEvent, DownloadManager, DownloadOptions, DownloadTask, TaskState,
};

fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("qomicex-test-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn opts() -> DownloadOptions {
    DownloadOptions {
        retry_base_delay: Duration::from_millis(10),
        split_sample_interval: Duration::from_millis(200),
        progress_throttle: Duration::from_millis(20),
        global_progress_interval: Duration::from_millis(50),
        segment_size: 256 * 1024,
        split_threshold: 1024,
        max_segments: 4,
        max_total_segments: 1,
        max_retries: 1,
        ..Default::default()
    }
}

async fn wait_terminal(m: &DownloadManager, id: u64, timeout: Duration) -> TaskState {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(st) = m.state(id).await {
            if matches!(
                st,
                TaskState::Completed | TaskState::Failed | TaskState::Cancelled
            ) {
                return st;
            }
        }
        if Instant::now() > deadline {
            return m.state(id).await.unwrap_or(TaskState::Failed);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn starved_segment_semaphore_must_not_degrade_to_streamed() {
    let server = MockServer::start(
        2 * 1024 * 1024,
        Behavior {
            throttle: Some((200_000, 200_000)),
            ..Default::default()
        },
    )
    .await;
    let dir = tmp_dir("sem-starve");
    let m = DownloadManager::new(opts(), 2);
    let mut rx = m.subscribe();
    let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let logs_task = {
        let logs = logs.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(DownloadEvent::Log { message, .. }) => {
                        logs.lock().unwrap().push(message);
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        })
    };

    let id1 = m.add(DownloadTask::new(server.url("file"), dir.join("a.bin")));
    let id2 = m.add(DownloadTask::new(server.url("file"), dir.join("b.bin")));

    let st1 = wait_terminal(&m, id1, Duration::from_secs(60)).await;
    let st2 = wait_terminal(&m, id2, Duration::from_secs(60)).await;
    logs_task.abort();
    let logs = logs.lock().unwrap().clone();
    let range_requests = server.range_requests();
    m.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(st1, TaskState::Completed);
    assert_eq!(st2, TaskState::Completed);
    assert!(
        range_requests >= 4,
        "两个任务都应走分片路径，实测 range 请求 {range_requests}"
    );
    assert!(
        !logs.iter().any(|m| m.contains("降级整文件重试")),
        "段许可排队不应被判为段失败并降级整文件；日志: {logs:#?}"
    );
}
