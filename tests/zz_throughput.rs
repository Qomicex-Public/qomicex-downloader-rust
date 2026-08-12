use std::time::{Duration, Instant};
use qomicex_downloader::{DownloadManager, DownloadOptions, DownloadTask, TaskState};

#[tokio::test]
async fn throughput_200_small() {
    let urls: Vec<String> = include_str!("../../stuck_urls.txt").lines()
        .filter_map(|l| l.split('\t').nth(1).map(String::from)).collect();
    let dir = std::env::temp_dir().join(format!("qomicex-thpt-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let opts = DownloadOptions { retry_base_delay: Duration::from_millis(100), watchdog_idle_timeout: Duration::from_secs(8), ..Default::default() };
    let m = DownloadManager::new(opts, 8);
    let ids: Vec<u64> = urls.iter().enumerate().map(|(i,u)| m.add(DownloadTask::new(u.clone(), dir.join(format!("a{i}"))))).collect();
    let start = Instant::now();
    let deadline = start + Duration::from_secs(180);
    loop {
        let all = {
            let mut t=true; for &id in &ids {
                match m.state(id).await { Ok(st) if matches!(st, TaskState::Completed|TaskState::Failed) => {}, _=>t=false }
            } t
        };
        if all { let l=m.list().await;
            let ok=l.iter().filter(|(_,s)|matches!(s,TaskState::Completed)).count();
            let fail=l.iter().filter(|(_,s)|matches!(s,TaskState::Failed)).count();
            eprintln!("DONE in {:?}: completed={ok} failed={fail} remaining={}", start.elapsed(), ids.len()-ok-fail);
            m.shutdown().await; let _=std::fs::remove_dir_all(&dir); return;
        }
        if Instant::now()>deadline {
            let l=m.list().await;
            let q=l.iter().filter(|(_,s)|matches!(s,TaskState::Queued)).count();
            let r=l.iter().filter(|(_,s)|matches!(s,TaskState::Downloading)).count();
            eprintln!("TIMEOUT at {:?}: queued={q} running={r}", start.elapsed());
            m.shutdown().await; let _=std::fs::remove_dir_all(&dir); panic!("timeout");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
