// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

//! 按 **IP** 而不是按域名记录下载源的健康状态。
//!
//! 同一个域名（如 `bmclapi2.bangbang93.com`、`mediafilez.forgecdn.net`）背后常是
//! 多家 CDN，解析结果随地域/时间变化：其中一部分节点很快，另一部分几乎不可用。
//! 按域名拉黑会把好节点一起废掉，因此这里：
//!
//! - 自定义 [`Resolve`] 用 `lookup_host` 取地址列表，**跳过已拉黑的 IP**；
//! - 每个 host 记录「上次返回给连接的 IP」，失败时据此归因（连接错误时拿不到
//!   `Response::remote_addr()`，只能靠这条记录）；
//! - 成功时用 `remote_addr()` 精确清除该 IP 的拉黑；
//! - 某个 host 的 IP **全部**不可用时会清空该 host 的黑名单（宁可再试一次，
//!   也不要永久废掉一个域名）。
//!
//! 已知限制：走 HTTP 代理时连接对端是代理而非源站，`remote_addr()` 失去意义，
//! 调用方应关闭本机制（见 `DownloadOptions::ip_level_blocking` 的注释）。

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// 单个 IP 的拉黑时长。短于常见 CDN 抖动恢复时间。
const IP_DEAD_TTL: Duration = Duration::from_secs(120);
/// DNS 结果缓存时长（避免每个连接都重新解析）。
const DNS_TTL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct HostState {
    addrs: Vec<IpAddr>,
    resolved_at: Option<Instant>,
    /// 轮询游标：连续失败时能换到下一个 IP，而不是一直撞同一个。
    cursor: usize,
    dead: HashMap<IpAddr, Instant>,
    /// 上一次实际返回给连接层的 IP（失败归因用）。
    last_served: Option<IpAddr>,
}

impl HostState {
    fn is_dead(&self, ip: &IpAddr) -> bool {
        self.dead
            .get(ip)
            .is_some_and(|at| at.elapsed() < IP_DEAD_TTL)
    }

    fn prune(&mut self) {
        self.dead.retain(|_, at| at.elapsed() < IP_DEAD_TTL);
    }
}

/// 跨连接共享的 IP 健康状态。
#[derive(Default)]
pub struct IpHealth {
    hosts: Mutex<HashMap<String, HostState>>,
}

impl IpHealth {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 从候选地址里挑一个可用的：跳过黑名单、轮询、全黑时自愈。
    fn pick(&self, host: &str, addrs: Vec<IpAddr>) -> Option<IpAddr> {
        if addrs.is_empty() {
            return None;
        }
        let mut map = self.hosts.lock().unwrap();
        let st = map.entry(host.to_string()).or_default();
        st.prune();
        st.addrs = addrs;
        st.resolved_at = Some(Instant::now());

        // 全黑：说明这个域名整体不可用，清空黑名单重新试，避免永久封禁。
        if st.addrs.iter().all(|ip| st.is_dead(ip)) {
            st.dead.clear();
        }

        let n = st.addrs.len();
        for off in 0..n {
            let idx = (st.cursor + off) % n;
            let ip = st.addrs[idx];
            if !st.is_dead(&ip) {
                st.cursor = (idx + 1) % n;
                st.last_served = Some(ip);
                return Some(ip);
            }
        }
        None
    }

    /// 仍在 TTL 内的解析结果（避免每个连接都重新查询 DNS）。
    fn cached_addrs(&self, host: &str) -> Option<Vec<IpAddr>> {
        let map = self.hosts.lock().unwrap();
        let st = map.get(host)?;
        let at = st.resolved_at?;
        if at.elapsed() < DNS_TTL && !st.addrs.is_empty() {
            Some(st.addrs.clone())
        } else {
            None
        }
    }

    /// 本次失败归因到 IP。返回 true 表示已按 IP 处理（调用方不必再拉黑域名）。
    pub fn mark_failure(&self, host: &str) -> bool {
        let mut map = self.hosts.lock().unwrap();
        let Some(st) = map.get_mut(host) else {
            return false;
        };
        let Some(ip) = st.last_served else {
            return false;
        };
        st.dead.insert(ip, Instant::now());
        true
    }

    /// 成功：该 IP 确认可用，移出黑名单。
    pub fn mark_ok(&self, host: &str, ip: IpAddr) {
        let mut map = self.hosts.lock().unwrap();
        if let Some(st) = map.get_mut(host) {
            st.dead.remove(&ip);
        }
    }

    /// 诊断/测试用：该 host 当前被拉黑的 IP 数。
    pub fn dead_count(&self, host: &str) -> usize {
        let map = self.hosts.lock().unwrap();
        map.get(host)
            .map(|st| st.dead.keys().filter(|ip| st.is_dead(ip)).count())
            .unwrap_or(0)
    }
}

/// 包装系统解析的 resolver：解析结果经 [`IpHealth`] 过滤，并只返回一个地址，
/// 让「返回的 IP」与「实际连接的 IP」一一对应（归因的前提）。
pub struct TrackingResolver {
    health: Arc<IpHealth>,
}

impl TrackingResolver {
    pub fn new(health: Arc<IpHealth>) -> Self {
        Self { health }
    }
}

impl Resolve for TrackingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        let health = self.health.clone();
        Box::pin(async move {
            let ips: Vec<IpAddr> = match health.cached_addrs(&host) {
                Some(v) => v,
                None => {
                    let looked: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                        .await
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?
                        .collect();
                    let mut v: Vec<IpAddr> = Vec::with_capacity(looked.len());
                    for a in looked {
                        if !v.contains(&a.ip()) {
                            v.push(a.ip());
                        }
                    }
                    v
                }
            };
            let Some(ip) = health.pick(&host, ips) else {
                return Err(Box::new(io::Error::new(
                    io::ErrorKind::AddrNotAvailable,
                    format!("{host} 无可用的解析地址"),
                ))
                    as Box<dyn std::error::Error + Send + Sync>);
            };
            let addrs: Addrs = Box::new(std::iter::once(SocketAddr::new(ip, 0)));
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn skips_dead_ip_and_keeps_rest_of_host_usable() {
        let h = IpHealth::new();
        let a = ip("1.1.1.1");
        let b = ip("2.2.2.2");
        assert_eq!(h.pick("cdn.example", vec![a, b]), Some(a));
        // 归因到刚才返回的 IP 并拉黑
        assert!(h.mark_failure("cdn.example"));
        assert_eq!(h.dead_count("cdn.example"), 1);
        // 同一域名仍可用另一个 IP —— 这正是按 IP 拉黑的意义
        assert_eq!(h.pick("cdn.example", vec![a, b]), Some(b));
    }

    #[test]
    fn success_clears_dead_ip() {
        let h = IpHealth::new();
        let a = ip("1.1.1.1");
        let b = ip("2.2.2.2");
        h.pick("h", vec![a, b]);
        h.mark_failure("h");
        assert_eq!(h.dead_count("h"), 1);
        h.mark_ok("h", a);
        assert_eq!(h.dead_count("h"), 0);
        // 游标会轮转，这里只断言「仍有可用地址」；黑名单已清空由上一行覆盖。
        assert!(h.pick("h", vec![a, b]).is_some());
    }

    #[test]
    fn all_ips_dead_self_heals_instead_of_blacklisting_host() {
        let h = IpHealth::new();
        let a = ip("1.1.1.1");
        let b = ip("2.2.2.2");
        h.pick("h", vec![a, b]);
        h.mark_failure("h");
        h.pick("h", vec![a, b]);
        h.mark_failure("h");
        assert_eq!(h.dead_count("h"), 2);
        // 全黑 → 清空并重新给一个地址，不返回「域名不可用」
        assert!(h.pick("h", vec![a, b]).is_some());
        assert_eq!(h.dead_count("h"), 0);
    }

    #[test]
    fn unknown_host_failure_is_not_attributed() {
        let h = IpHealth::new();
        assert!(
            !h.mark_failure("never-resolved"),
            "没有解析记录时不应归因到 IP"
        );
    }

    #[test]
    fn cursor_rotates_so_repeated_failures_try_other_ips() {
        let h = IpHealth::new();
        let a = ip("1.1.1.1");
        let b = ip("2.2.2.2");
        let c = ip("3.3.3.3");
        let first = h.pick("h", vec![a, b, c]).unwrap();
        h.mark_failure("h");
        let second = h.pick("h", vec![a, b, c]).unwrap();
        h.mark_failure("h");
        let third = h.pick("h", vec![a, b, c]).unwrap();
        assert_ne!(first, second);
        assert_ne!(second, third);
    }
}
