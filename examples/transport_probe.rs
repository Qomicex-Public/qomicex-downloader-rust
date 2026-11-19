// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

//! 传输对照探针（issue #366 日志）：同一批 CurseForge 文件，分别用默认（HTTP/2
//! 多路复用）与强制 HTTP/1.1 并行跑一遍，统计首字节超时/换源次数与总耗时。
//!
//! 运行：cargo run --example transport_probe -- [并发数]

use std::time::{Duration, Instant};

use qomicex_downloader::{
    DownloadEvent, DownloadManager, DownloadOptions, DownloadTask, TaskState,
};

const URLS: &[&str] = &[
    "https://edge.forgecdn.net/files/7036/5/misctweaks-1.1.4%2b1.20.1.jar",
    "https://edge.forgecdn.net/files/9130/247/PuzzlesLib-v8.1.36-1.20.1-Forge.jar",
    "https://edge.forgecdn.net/files/6671/993/nanny-1.0.6.jar",
    "https://edge.forgecdn.net/files/7338/200/BadOptimizations-2.4.1-1.20.1.jar",
    "https://edge.forgecdn.net/files/9202/151/hybrid_delights-forge-1.20.1-1.3.3.jar",
    "https://edge.forgecdn.net/files/4598/640/deuf-1.20.1-1.3.jar",
    "https://edge.forgecdn.net/files/5490/637/ConnectorExtras-1.11.2%2b1.20.1.jar",
    "https://edge.forgecdn.net/files/7573/740/framework-forge-1.20.1-0.8.0.jar",
    "https://edge.forgecdn.net/files/7041/812/Raided-1.20.1-0.1.6.jar",
    "https://edge.forgecdn.net/files/5869/180/cristellib-1.1.6-forge.jar",
    "https://edge.forgecdn.net/files/7665/983/Mandala_Dragon_Mount_Refreshed_1.0.zip",
    "https://edge.forgecdn.net/files/7180/166/reset_controls_confirmation-forge-1.0.0-1.20.1.jar",
    "https://edge.forgecdn.net/files/5509/183/temporalapi-1.6.7.jar",
    "https://edge.forgecdn.net/files/9103/580/inmis-2.7.2-2.9.4-1.20.1.jar",
    "https://edge.forgecdn.net/files/5046/982/MagnumTorch-v8.0.2-1.20.1-Forge.jar",
    "https://edge.forgecdn.net/files/7326/870/VB-MobVariantCompat1.4.1.zip",
    "https://edge.forgecdn.net/files/4624/707/Stay_True_1.20.zip",
    "https://edge.forgecdn.net/files/4769/83/YungsBetterStrongholds-1.20-Forge-4.0.3.jar",
    "https://edge.forgecdn.net/files/9469/65/everycomp-1.20-2.9.29-forge.jar",
    "https://edge.forgecdn.net/files/6365/716/spawnersplus-4.3.3-1.20.1.jar",
    "https://edge.forgecdn.net/files/6148/844/redirected-forge-1.0.0-1.20.1.jar",
    "https://edge.forgecdn.net/files/9381/66/Ixeris-4.6.11-1.20.1-forge.jar",
    "https://edge.forgecdn.net/files/8322/142/Kiwi-1.20.1-Forge-11.10.3.jar",
    "https://edge.forgecdn.net/files/8926/736/tidal-towns-2.0.jar",
];

async fn run_phase(label: &str, http1: bool, concurrency: usize) {
    let dir = std::env::temp_dir().join(format!("qmx-probe-{label}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let opts = DownloadOptions {
        http1_parallel: http1,
        ..Default::default()
    };
    let mgr = DownloadManager::new(opts, concurrency);
    let mut rx = mgr.subscribe();
    let logger = tokio::spawn(async move {
        let mut first_byte_stalls = 0u32;
        let mut mirror_switches = 0u32;
        let mut retries = 0u32;
        let mut lagged = 0u64;
        loop {
            match rx.recv().await {
                Ok(DownloadEvent::Log { message, .. }) => {
                    if message.contains("首字节超时") {
                        first_byte_stalls += 1;
                    } else if message.contains("切换镜像") {
                        mirror_switches += 1;
                    } else if message.contains("次重试") {
                        retries += 1;
                    }
                }
                Ok(_) => {}
                // 落后不等于结束：广播缓冲区被覆盖只丢一部分日志，必须继续收，
                // 并把丢弃数量报出来，否则会把不完整计数当完整结果。
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => lagged += n,
                Err(_) => break,
            }
        }
        (first_byte_stalls, mirror_switches, retries, lagged)
    });
    let mut ids = Vec::new();
    for (i, url) in URLS.iter().enumerate() {
        ids.push(mgr.add(DownloadTask::new(*url, dir.join(format!("f{i}.jar")))));
    }
    let started = Instant::now();
    let mut done = 0usize;
    let mut failed = 0usize;
    while done + failed < ids.len() {
        tokio::time::sleep(Duration::from_millis(500)).await;
        done = 0;
        failed = 0;
        for &id in &ids {
            match mgr.state(id).await {
                Ok(TaskState::Completed) => done += 1,
                Ok(TaskState::Failed | TaskState::Cancelled) => failed += 1,
                _ => {}
            }
        }
        if started.elapsed() > Duration::from_secs(600) {
            println!("[{label}] 超时退出");
            break;
        }
    }
    let elapsed = started.elapsed();
    mgr.shutdown().await;
    // 先丢 manager（关掉事件 sender），否则 rx.recv() 永远等不到 Closed，logger.await 死等。
    drop(mgr);
    let (stalls, switches, retries, lagged) =
        match tokio::time::timeout(Duration::from_secs(5), logger).await {
            Ok(Ok(v)) => v,
            _ => (0, 0, 0, 0),
        };
    println!(
        "[{label}] http1={http1} 并发={concurrency} 完成={done} 失败={failed} 耗时={elapsed:.1?} 首字节超时={stalls} 换源={switches} 重试={retries} 事件丢弃={lagged}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::main]
async fn main() {
    let concurrency: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(24);
    run_phase("h2", false, concurrency).await;
    run_phase("h1", true, concurrency).await;
}
