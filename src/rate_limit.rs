// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

//! 全局限速（令牌桶，issue #10）。
//!
//! 所有段、所有任务共享一个桶：令牌按 `bytes_per_sec` 速率补充，容量为 1 秒的量
//! （允许短时突发，不长期超前）。等待时最长睡 100 ms 就重新检查，保证取消/暂停
//! 不会被长睡眠拖住。

use std::time::{Duration, Instant};

use tokio::sync::Mutex;

pub(crate) struct RateLimiter {
    bytes_per_sec: f64,
    state: Mutex<State>,
}

struct State {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    pub fn new(bytes_per_sec: u64) -> Self {
        let rate = bytes_per_sec.max(1) as f64;
        Self {
            bytes_per_sec: rate,
            state: Mutex::new(State {
                tokens: rate,
                last: Instant::now(),
            }),
        }
    }

    /// 取走 `bytes` 个令牌，不足则等待补足。调用方逐块调用即可得到全局速率上限。
    pub async fn acquire(&self, bytes: u64) {
        let mut need = bytes as f64;
        loop {
            let wait = {
                let mut st = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.saturating_duration_since(st.last).as_secs_f64();
                st.last = now;
                st.tokens = (st.tokens + elapsed * self.bytes_per_sec).min(self.bytes_per_sec);
                if st.tokens >= need {
                    st.tokens -= need;
                    return;
                }
                need -= st.tokens;
                st.tokens = 0.0;
                Duration::from_secs_f64((need / self.bytes_per_sec).min(0.1))
                    .max(Duration::from_millis(5))
            };
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn waits_for_extra_tokens() {
        // 每秒 1000 字节：桶初始 1000 字节，再取 1000 必须等约 1 秒。
        let l = RateLimiter::new(1000);
        let start = Instant::now();
        l.acquire(1000).await;
        assert!(start.elapsed() < Duration::from_millis(200));
        l.acquire(1000).await;
        assert!(
            start.elapsed() >= Duration::from_millis(700),
            "第二次取令牌应等待补足，实际 {:?}",
            start.elapsed()
        );
    }
}
