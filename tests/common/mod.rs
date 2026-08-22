//! 测试用 mock HTTP 服务器：支持 Range、HEAD、异常注入（flaky/stall/throttle/rampup）。
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// 确定性伪随机数据（LCG）。
pub fn make_data(size: usize) -> Vec<u8> {
    let mut x: u64 = 0x1234_5678_9abc_def0;
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push((x >> 24) as u8);
    }
    out
}

/// 服务器行为配置。
#[derive(Clone, Default)]
pub struct Behavior {
    /// 忽略 Range，总是返回 200 全文件。
    pub no_range: bool,
    /// Transfer-Encoding: chunked 且无 Content-Length。
    pub chunked: bool,
    /// `/flaky` 路径前 N 次 GET 返回 500。
    pub flaky: Option<u32>,
    /// `/stall-once` 首次 GET：发 1KB 后停顿 `stall` 再继续。
    pub stall: Option<Duration>,
    /// `/throttle` 慢/快连接速率（bps），慢=连接1，快=后续。
    pub throttle: Option<(u64, u64)>,
    /// `/status404` 恒返回 404。
    pub status404: bool,
}

pub struct MockServer {
    pub addr: std::net::SocketAddr,
    pub data: Arc<Vec<u8>>,
    handle: JoinHandle<()>,
    flaky_count: Arc<AtomicU32>,
    conn_count: Arc<AtomicU32>,
}

impl MockServer {
    pub async fn start(data_size: usize, behavior: Behavior) -> Self {
        let data = Arc::new(make_data(data_size));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let flaky_count = Arc::new(AtomicU32::new(0));
        let conn_count = Arc::new(AtomicU32::new(0));
        let handle = tokio::spawn(accept_loop(
            listener,
            data.clone(),
            behavior,
            flaky_count.clone(),
            conn_count.clone(),
        ));
        Self {
            addr,
            data,
            handle,
            flaky_count,
            conn_count,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}/{}", self.addr, path.trim_start_matches('/'))
    }

    pub fn flaky_requests(&self) -> u32 {
        self.flaky_count.load(Ordering::Relaxed)
    }
}

async fn accept_loop(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    behavior: Behavior,
    flaky_count: Arc<AtomicU32>,
    conn_count: Arc<AtomicU32>,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let data = data.clone();
        let behavior = behavior.clone();
        let fc = flaky_count.clone();
        let cc = conn_count.clone();
        tokio::spawn(async move {
            let _ = handle_conn(stream, data, behavior, fc, cc).await;
        });
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    data: Arc<Vec<u8>>,
    behavior: Behavior,
    flaky_count: Arc<AtomicU32>,
    conn_count: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 2048];
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let (path, _query) = target.split_once('?').unwrap_or((target.as_str(), ""));
    let mut headers: HashMap<String, String> = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let range = headers.get("range").cloned();

    // `/redirect` 模拟纯重定向器：无 Range 时 302 到真实文件，带 Range 时直接 404。
    // 这是 CurseForge edge.forgecdn.net 的真实行为（它 302 到 mediafilez，但对
    // 带 Range 的请求返回 CloudFront 404）。探测走 HEAD/重定向会判定支持分段，
    // 因此传输阶段必须使用解析后的地址，否则每一段都会失败。
    if path == "/redirect" {
        let resp = if range.is_some() {
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        } else {
            "HTTP/1.1 302 Found\r\nLocation: /\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        };
        return write_response(&mut stream, resp.as_bytes(), &[], None).await;
    }

    if path == "/status404" || behavior.status404 {
        return write_response(
            &mut stream,
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_bytes(),
            &[],
            None,
        )
        .await;
    }
    if path == "/flaky" && method == "GET" {
        let count = flaky_count.fetch_add(1, Ordering::Relaxed);
        if count < behavior.flaky.unwrap_or(0) {
            return write_response(
                &mut stream,
                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .as_bytes(),
                &[],
                None,
            )
            .await;
        }
    }

    if method == "HEAD" {
        let total = data.len();
        let range_hdr = if behavior.no_range {
            ""
        } else {
            "Accept-Ranges: bytes\r\n"
        };
        return write_response(
            &mut stream,
            format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n{range_hdr}Connection: close\r\n\r\n")
                .as_bytes(),
            &[],
            None,
        )
        .await;
    }

    // GET 响应体分块 + 可选延迟：Vec<(块, 写前延迟)>
    let mut body_parts: Vec<(Vec<u8>, Option<Duration>)> = Vec::new();

    if behavior.chunked {
        let header = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
        for chunk in data.chunks(16 * 1024) {
            body_parts.push((format!("{:x}\r\n", chunk.len()).into_bytes(), None));
            body_parts.push((chunk.to_vec(), None));
            body_parts.push((b"\r\n".to_vec(), None));
        }
        body_parts.push((b"0\r\n\r\n".to_vec(), None));
        return write_parts(&mut stream, header, &body_parts).await;
    }

    if let Some((slow, fast)) = behavior.throttle {
        let conn = conn_count.fetch_add(1, Ordering::Relaxed);
        let rate = if conn == 0 { slow } else { fast };
        let block = 8 * 1024u64;
        let delay = Duration::from_secs_f64(block as f64 / rate as f64);
        let total = data.len() as u64;
        let (a, b) = match parse_range(range.as_deref()) {
            Some((a, b)) if !behavior.no_range => (a.min(total), b.min(total - 1)),
            _ => (0, total - 1),
        };
        if a > b {
            let header = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            return write_response(&mut stream, header.as_bytes(), &[], None).await;
        }
        let header = if a == 0 && b == total - 1 {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
            )
        } else {
            format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {a}-{b}/{total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                b - a + 1
            )
        };
        let mut offset = a as usize;
        let end = b as usize;
        while offset <= end {
            let chunk_end = (offset + block as usize - 1).min(end);
            body_parts.push((data[offset..=chunk_end].to_vec(), Some(delay)));
            offset = chunk_end + 1;
        }
        return write_parts(&mut stream, &header, &body_parts).await;
    }

    if path == "/stall-once" {
        if let Some(stall) = behavior.stall {
            // 首个请求：发 1KB 后停顿，后续请求正常
            let first = conn_count
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok();
            let total = data.len() as u64;
            let (a, b) = match parse_range(range.as_deref()) {
                Some((a, b)) if !behavior.no_range => (a.min(total), b.min(total - 1)),
                _ => (0, total - 1),
            };
            if a > b {
                let header = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                return write_response(&mut stream, header.as_bytes(), &[], None).await;
            }
            let header = if a == 0 && b == total - 1 && range.is_none() {
                format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n")
            } else {
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {a}-{b}/{total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    b - a + 1
                )
            };
            let slice = &data[a as usize..=b as usize];
            if first && slice.len() > 1024 {
                body_parts.push((slice[..1024].to_vec(), None));
                body_parts.push((slice[1024..].to_vec(), Some(stall)));
            } else {
                body_parts.push((slice.to_vec(), None));
            }
            return write_parts(&mut stream, &header, &body_parts).await;
        }
    }

    // 正常路径：Range 支持
    let total = data.len() as u64;
    let parsed_range = parse_range(range.as_deref());
    if behavior.no_range || parsed_range.is_none() {
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
        );
        body_parts.push((data.to_vec(), None));
        return write_parts(&mut stream, &header, &body_parts).await;
    }
    let (a, b) = parsed_range.unwrap();
    let a = a.min(total);
    let b = b.min(total - 1);
    if a > b {
        let header =
            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        return write_response(&mut stream, header.as_bytes(), &[], None).await;
    }
    let header = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {a}-{b}/{total}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
        b - a + 1
    );
    body_parts.push((data[a as usize..=b as usize].to_vec(), None));
    write_parts(&mut stream, &header, &body_parts).await
}

fn parse_range(range: Option<&str>) -> Option<(u64, u64)> {
    let v = range?;
    let bytes = v.strip_prefix("bytes=")?;
    let (a, b) = bytes.split_once('-')?;
    let a: u64 = a.parse().ok()?;
    let b: u64 = if b.is_empty() {
        u64::MAX
    } else {
        b.parse().ok()?
    };
    Some((a, b))
}

async fn write_response(
    stream: &mut TcpStream,
    header: &[u8],
    body: &[u8],
    delay: Option<Duration>,
) -> std::io::Result<()> {
    if let Some(d) = delay {
        tokio::time::sleep(d).await;
    }
    stream.write_all(header).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

async fn write_parts(
    stream: &mut TcpStream,
    header: &str,
    parts: &[(Vec<u8>, Option<Duration>)],
) -> std::io::Result<()> {
    stream.write_all(header.as_bytes()).await?;
    for (chunk, delay) in parts {
        if let Some(d) = delay {
            tokio::time::sleep(*d).await;
        }
        stream.write_all(chunk).await?;
    }
    stream.flush().await
}
