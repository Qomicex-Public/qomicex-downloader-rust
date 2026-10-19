// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

//! 分片下载的块级完成位图（sidecar `<dest>.part.meta`）。
//!
//! `.part` 文件大小只是「最高水位」：多段并行时中间可能有空洞，无法从大小推断
//! 哪些段真的写完了（issue #2）。这里按固定块大小记录完成位图，续传时以位图为准，
//! 而不是文件长度。
//!
//! 设计要点：
//! - 位图与 `.part` 同目录、同生命周期；`finalize` 成功后一并删除；
//! - 载入时校验 `url / total / sha256 / block_size`，任何不一致直接丢弃（避免复用
//!   错配的 `.part` 产出错误内容）；
//! - 落盘先写 `.tmp` 再 rename，避免中途崩溃留下半截 JSON。

use std::path::{Path, PathBuf};

/// 块大小：1 MiB。太小会产生大量元数据，太大则崩溃时重下过多。
pub(crate) const DEFAULT_BLOCK_SIZE: u64 = 1024 * 1024;
const META_VERSION: u32 = 1;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PartMeta {
    pub version: u32,
    pub url: String,
    pub total: u64,
    pub sha256: Option<String>,
    pub block_size: u64,
    /// 完成位图（bit i = 第 i 块已完成），按 u64 打包。
    pub bitmap: Vec<u64>,
    pub updated_at: u64,
}

impl PartMeta {
    pub fn new(url: &str, total: u64, sha256: Option<String>, block_size: u64) -> Self {
        let bs = block_size.max(1);
        let blocks = total.div_ceil(bs) as usize;
        Self {
            version: META_VERSION,
            url: url.to_string(),
            total,
            sha256,
            block_size: bs,
            bitmap: vec![0; blocks.div_ceil(64)],
            updated_at: now_secs(),
        }
    }

    fn bit(&self, block: usize) -> bool {
        self.bitmap
            .get(block / 64)
            .is_some_and(|w| w & (1u64 << (block % 64)) != 0)
    }

    fn set_bit(&mut self, block: usize, value: bool) {
        if let Some(w) = self.bitmap.get_mut(block / 64) {
            if value {
                *w |= 1u64 << (block % 64);
            } else {
                *w &= !(1u64 << (block % 64));
            }
        }
    }

    /// 第 `block` 块在文件中的字节范围。
    pub fn block_range(&self, block: usize) -> (u64, u64) {
        let start = block as u64 * self.block_size;
        (start, (start + self.block_size).min(self.total))
    }

    /// 有效块数（最后一截可能不足 `block_size`）。
    pub fn used_blocks(&self) -> usize {
        if self.total == 0 {
            0
        } else {
            (self.total.div_ceil(self.block_size)) as usize
        }
    }

    pub fn is_complete(&self, block: usize) -> bool {
        block < self.used_blocks() && self.bit(block)
    }

    pub fn set_complete(&mut self, block: usize, value: bool) {
        if block < self.used_blocks() {
            self.set_bit(block, value);
        }
    }

    pub fn all_complete(&self) -> bool {
        if self.total == 0 {
            return true;
        }
        (0..self.used_blocks()).all(|b| self.is_complete(b))
    }

    /// 该元数据是否仍对应当前任务（URL / 总长 / 校验和 / 块大小全一致）。
    pub fn matches(&self, url: &str, total: u64, sha256: Option<&str>, block_size: u64) -> bool {
        self.version == META_VERSION
            && self.url == url
            && self.total == total
            && self.block_size == block_size.max(1)
            && self.sha256.as_deref() == sha256
    }

    /// 某段可以从哪里续传：从段首开始，连续完成的块所覆盖的字节数。
    ///
    /// 只按「整块完成」推进：块内的部分进度宁可重下（≤1 MiB/段），也不冒跳过
    /// 数据的风险。段首可能落在块中间，此时该块必不完整，返回 0。
    pub fn resumable_bytes(&self, seg_start: u64, seg_len: u64) -> u64 {
        let mut got = 0u64;
        let mut offset = seg_start;
        let seg_end = seg_start + seg_len;
        while offset < seg_end {
            let block = (offset / self.block_size) as usize;
            let (b0, b1) = self.block_range(block);
            // 段首在块中间：该块无法判定完整，停止（保守）。
            if offset != b0 || !self.is_complete(block) {
                break;
            }
            got += b1 - b0;
            offset = b1;
        }
        got.min(seg_len)
    }

    pub async fn load(path: &Path) -> Option<Self> {
        let text = tokio::fs::read_to_string(path).await.ok()?;
        serde_json::from_str::<Self>(&text).ok()
    }

    /// 原子落盘：先写 `.tmp` 再 rename（同目录，保证同卷）。
    pub async fn save(&self, path: &Path) -> std::io::Result<()> {
        let mut me = self.clone();
        me.updated_at = now_secs();
        let text = serde_json::to_string(&me)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = meta_tmp_path(path);
        tokio::fs::write(&tmp, text.as_bytes()).await?;
        tokio::fs::rename(&tmp, path).await
    }
}

/// `<part>.meta` 的同目录临时文件。
fn meta_tmp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "part.meta".to_string());
    path.with_file_name(format!("{name}.tmp"))
}

pub(crate) fn meta_path_for(part: &Path) -> PathBuf {
    let name = part
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download.part".to_string());
    part.with_file_name(format!("{name}.meta"))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(total: u64, bs: u64) -> PartMeta {
        PartMeta::new("https://x/y.jar", total, None, bs)
    }

    #[test]
    fn bitmap_roundtrip_and_completeness() {
        let mut m = meta(3 * DEFAULT_BLOCK_SIZE + 100, DEFAULT_BLOCK_SIZE);
        assert_eq!(m.used_blocks(), 4);
        assert!(!m.all_complete());
        for b in 0..m.used_blocks() {
            m.set_complete(b, true);
        }
        assert!(m.all_complete());
        m.set_complete(2, false);
        assert!(!m.all_complete());
    }

    #[test]
    fn matches_rejects_mismatched_identity() {
        let m = meta(1000, DEFAULT_BLOCK_SIZE);
        assert!(m.matches("https://x/y.jar", 1000, None, DEFAULT_BLOCK_SIZE));
        assert!(!m.matches("https://x/other.jar", 1000, None, DEFAULT_BLOCK_SIZE));
        assert!(!m.matches("https://x/y.jar", 999, None, DEFAULT_BLOCK_SIZE));
        assert!(!m.matches("https://x/y.jar", 1000, Some("ab"), DEFAULT_BLOCK_SIZE));
        assert!(!m.matches("https://x/y.jar", 1000, None, 4096));
    }

    #[test]
    fn resumable_bytes_requires_whole_blocks() {
        let mut m = meta(4 * DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
        m.set_complete(0, true);
        m.set_complete(1, true);
        // 段从 0 开始：连续 2 块完成 → 可续 2MiB
        assert_eq!(
            m.resumable_bytes(0, 4 * DEFAULT_BLOCK_SIZE),
            2 * DEFAULT_BLOCK_SIZE
        );
        // 段从中途开始（块未对齐）→ 保守返回 0
        assert_eq!(m.resumable_bytes(512, 1000), 0);
    }

    #[test]
    fn resumable_bytes_stops_at_first_gap() {
        let mut m = meta(4 * DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
        m.set_complete(0, true);
        m.set_complete(2, true); // 1 缺失
        assert_eq!(
            m.resumable_bytes(0, 4 * DEFAULT_BLOCK_SIZE),
            DEFAULT_BLOCK_SIZE
        );
    }

    #[tokio::test]
    async fn save_and_load_roundtrip() {
        let dir = std::env::temp_dir().join(format!("qml-meta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.bin.part.meta");
        let mut m = meta(2 * DEFAULT_BLOCK_SIZE, DEFAULT_BLOCK_SIZE);
        m.set_complete(0, true);
        m.save(&path).await.unwrap();
        let loaded = PartMeta::load(&path).await.expect("应能读回");
        assert!(loaded.is_complete(0));
        assert!(!loaded.is_complete(1));
        assert!(loaded.matches(
            "https://x/y.jar",
            2 * DEFAULT_BLOCK_SIZE,
            None,
            DEFAULT_BLOCK_SIZE
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
