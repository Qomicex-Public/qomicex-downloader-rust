//! 真机 A/B 测速：同一批 Modrinth 整合包 mod 文件，分别用「HTTP/2 激进配置」与
//! 「HTTP/1.1 并行连接」并发下载，对比总吞吐，用于判定 Modrinth CDN 的限速是
//! 「按连接」(→ h1 并行更好) 还是「按 IP/总带宽」(→ 两者差不多)。
//!
//! 用法（在仓库根运行，须本机能访问 api.modrinth.com / cdn.modrinth.com）：
//!
//!   1) 从版本 id 清单拉取直链并生成 mods.txt + 直接跑测速：
//!      cargo run --release --example modpack_ab -- --versions-file <ids.txt> \
//!      [--concurrency 16] [--write-mods mods.txt]
//!   2) 直接给一个 Modrinth 版本 id（拉该版本的 files[]）：
//!      cargo run --release --example modpack_ab -- <版本id> [并发数]
//!   3) 直接用现成的直链清单（每行一个 URL）：
//!      cargo run --release --example modpack_ab -- <urls.txt> [并发数]
//!
//! 输出 H2 / H1 各自的：文件数、总字节、耗时、总吞吐(KB/s)。
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use reqwest::Client;
use tokio::sync::Semaphore;

const UA: &str = "qomicex-modpack-ab/0.1 (A/B benchmark)";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("用法见 examples/modpack_ab.rs 头部注释");
        std::process::exit(2);
    }

    let mut concurrency: usize = 16;
    let mut urls: Vec<String> = Vec::new();
    let mut write_to: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--versions-file" => {
                i += 1;
                let path = args.get(i).unwrap_or_else(|| {
                    eprintln!("--versions-file 缺少路径");
                    std::process::exit(2);
                });
                let ids = std::fs::read_to_string(path)
                    .unwrap_or_else(|e| {
                        eprintln!("读取 {path} 失败: {e}");
                        std::process::exit(1);
                    })
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(String::from)
                    .collect::<Vec<_>>();
                eprintln!(
                    "从 {path} 读到 {} 个版本 id，正在解析 files[].url ...",
                    ids.len()
                );
                urls = fetch_many(&ids).await;
            }
            "--concurrency" => {
                i += 1;
                concurrency = args[i].parse().unwrap_or(16);
            }
            "--write-mods" => {
                i += 1;
                write_to = Some(args[i].clone());
            }
            other => {
                if other.ends_with(".txt") {
                    urls = std::fs::read_to_string(other)
                        .unwrap_or_else(|e| {
                            eprintln!("读取 {other} 失败: {e}");
                            std::process::exit(1);
                        })
                        .lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty() && l.starts_with("http"))
                        .map(String::from)
                        .collect();
                } else {
                    // 单个版本 id
                    urls = fetch_many(&[other.to_string()]).await;
                }
                // 兼容旧用法：下一个数字并发
                if i + 1 < args.len() {
                    if let Ok(n) = args[i + 1].parse::<usize>() {
                        concurrency = n;
                        i += 1;
                    }
                }
            }
        }
        i += 1;
    }

    if urls.is_empty() {
        eprintln!("没有可用的下载 URL");
        std::process::exit(1);
    }

    // 去重 + 写 mods.txt（如指定）
    let mut seen = HashSet::new();
    urls.retain(|u| seen.insert(u.clone()));
    if let Some(p) = write_to {
        std::fs::write(&p, urls.join("\n") + "\n").unwrap_or_else(|e| {
            eprintln!("写 {p} 失败: {e}");
            std::process::exit(1);
        });
        eprintln!("已写出 {} 个直链 -> {p}", urls.len());
    }
    eprintln!("文件数: {}  并发: {}\n", urls.len(), concurrency);

    // H2：镜像下载器 manager.rs build_clients 的激进配置（adaptive window + 大帧）
    let h2 = Client::builder()
        .user_agent(UA)
        .pool_idle_timeout(std::time::Duration::from_secs(60))
        .pool_max_idle_per_host(concurrency.min(32))
        .http2_adaptive_window(true)
        .http2_max_frame_size((16 * 1024 * 1024) - 1)
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .build()
        .expect("h2 client");
    // H1：强制 HTTP/1.1，各请求独立 TCP 连接
    let h1 = Client::builder()
        .user_agent(UA)
        .pool_idle_timeout(std::time::Duration::from_secs(60))
        .http1_only()
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .build()
        .expect("h1 client");

    let (b2, t2) = run_mode(h2, &urls, concurrency).await;
    let (b1, t1) = run_mode(h1, &urls, concurrency).await;

    println!("\n================ 结果 ================");
    print_row("H2(多路复用)", b2, t2, urls.len());
    print_row("H1(并行连接)", b1, t1, urls.len());
    if t1 > 0.0 && t2 > 0.0 {
        let ratio = t2 / t1;
        let hint = if ratio > 1.3 {
            "  → 倾向 HTTP/1.1 并行（每连接限速）"
        } else if ratio < 0.77 {
            "  → 倾向保持 HTTP/2"
        } else {
            "  → 差异不大，按主机而定"
        };
        println!("\n结论提示: H2 耗时/H1 耗时 = {:.2}x{hint}", ratio);
    }
}

/// 并发拉取多个版本 id 的 files[].url（每个版本取第一个文件）。
async fn fetch_many(ids: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut jobs = Vec::new();
    for id in ids {
        let id = id.clone();
        jobs.push(tokio::spawn(async move { fetch_version_url(&id).await }));
    }
    for j in jobs {
        if let Ok(Some(url)) = j.await {
            out.push(url);
        }
    }
    out
}

async fn fetch_version_url(version_id: &str) -> Option<String> {
    let api = format!("https://api.modrinth.com/v2/version/{version_id}");
    let raw = Client::new()
        .get(&api)
        .header("User-Agent", UA)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .bytes()
        .await
        .ok()?;
    let body: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    body["files"]
        .as_array()?
        .first()?
        .get("url")?
        .as_str()
        .map(String::from)
}

fn print_row(label: &str, bytes: u64, secs: f64, n: usize) {
    let kbs = if secs > 0.0 {
        bytes as f64 / secs / 1000.0
    } else {
        0.0
    };
    println!(
        "{:<16} {} 文件  {:>10.1} MB  耗时 {:>6.2}s  总吞吐 {:>8.1} KB/s",
        label,
        n,
        bytes as f64 / 1024.0 / 1024.0,
        secs,
        kbs,
    );
}

async fn run_mode(client: Client, urls: &[String], concurrency: usize) -> (u64, f64) {
    let sem = Arc::new(Semaphore::new(concurrency));
    let start = Instant::now();
    let mut handles = Vec::with_capacity(urls.len());
    for url in urls {
        let sem = sem.clone();
        let client = client.clone();
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            match client.get(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    r.bytes().await.map(|b| b.len() as u64).unwrap_or(0)
                }
                _ => 0,
            }
        }));
    }
    let mut total = 0u64;
    for h in handles {
        if let Ok(b) = h.await {
            total += b;
        }
    }
    (total, start.elapsed().as_secs_f64())
}
