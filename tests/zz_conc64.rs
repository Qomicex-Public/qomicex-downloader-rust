use std::time::{Duration, Instant};
use qomicex_downloader::{DownloadManager, DownloadOptions, DownloadTask, TaskState};

#[tokio::test]
async fn conc64_volume() {
    let urls: Vec<String> = include_str!("../../stuck_urls.txt").lines()
        .filter_map(|l| l.split('\t').nth(1).map(String::from)).collect();
    let dir = std::env::temp_dir().join(format!("qomicex-c64-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir); std::fs::create_dir_all(&dir).unwrap();
    let opts = DownloadOptions { retry_base_delay: Duration::from_millis(100), watchdog_idle_timeout: Duration::from_secs(8), ..Default::default() };
    let m = DownloadManager::new(opts, 64);
    let ids: Vec<u64> = urls.iter().enumerate().map(|(i,u)| m.add(DownloadTask::new(u.clone(), dir.join(format!("a{i}"))))).collect();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(120);
    loop {
        let mut all=true; for &id in &ids {
            match m.state(id).await { Ok(st) if matches!(st, TaskState::Completed|TaskState::Failed) => {}, _=>all=false }
        }
        if all { let l=m.list().await;
            let ok=l.iter().filter(|(_,s)|matches!(s,TaskState::Completed)).count();
            let fail=l.iter().filter(|(_,s)|matches!(s,TaskState::Failed)).count();
            eprintln!("DONE in {:?}: completed={ok} failed={fail}", start.elapsed());
            m.shutdown().await; let _=std::fs::remove_dir_all(&dir); return;
        }
        if Instant::now()>deadline {
            let l=m.list().await;
            let q=l.iter().filter(|(_,s)|matches!(s,TaskState::Queued)).count();
            let r=l.iter().filter(|(_,s)|matches!(s,TaskState::Downloading)).count();
            eprintln!("TIMEOUT at {:?}: queued={q} running={r}", start.elapsed());
            // show a sample of running/queued
            for &id in &ids { let s=m.state(id).await.ok(); if matches!(s,Some(TaskState::Downloading)|Some(TaskState::Queued)) { eprintln!("  stuck {id}: {s:?}"); } if q+r>6 { break; } }
            m.shutdown().await; let _=std::fs::remove_dir_all(&dir); panic!("timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
