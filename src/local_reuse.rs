// Copyright (C) 2026 Qomicex Launcher Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

//! 下载前的本地复用：按 hash 在给定目录中查找同内容文件（issue #11）。
//!
//! 只对「大小相同」的候选文件算摘要，避免为一次下载遍历哈希整个 `.minecraft`。
//! 摘要结果按路径 + 大小 + mtime 缓存，重复任务不会反复读盘。

use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

use sha1::Sha1;
use sha2::{Digest, Sha256};

/// 目录递归深度上限：够覆盖 `.minecraft/libraries/...`，又能挡住异常深的结构。
const MAX_DEPTH: u32 = 8;

#[derive(Clone, Copy)]
struct Digests {
    size: u64,
    mtime: Option<SystemTime>,
    sha256: [u8; 32],
    sha1: [u8; 20],
}

fn cache() -> &'static Mutex<HashMap<PathBuf, Digests>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, Digests>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// 在 `dirs` 中查找大小与摘要都匹配的文件。
///
/// `sha256` / `sha1` 至少给一个，否则无从判断内容。
pub(crate) fn find_match(
    dirs: &[PathBuf],
    size: u64,
    sha256: Option<[u8; 32]>,
    sha1: Option<[u8; 20]>,
) -> Option<PathBuf> {
    if sha256.is_none() && sha1.is_none() {
        return None;
    }
    for dir in dirs {
        if !dir.is_dir() {
            continue;
        }
        if let Some(hit) = scan(dir, size, sha256, sha1) {
            return Some(hit);
        }
    }
    None
}

fn scan(
    root: &Path,
    size: u64,
    sha256: Option<[u8; 32]>,
    sha1: Option<[u8; 20]>,
) -> Option<PathBuf> {
    let mut stack: Vec<(PathBuf, u32)> = vec![(root.to_path_buf(), 0)];
    while let Some((dir, depth)) = stack.pop() {
        if depth > MAX_DEPTH {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_dir() {
                stack.push((path, depth + 1));
                continue;
            }
            if meta.len() != size {
                continue;
            }
            let Some(d) = digests(&path, meta.len(), meta.modified().ok()) else {
                continue;
            };
            if sha256.is_none_or(|want| want == d.sha256) && sha1.is_none_or(|want| want == d.sha1)
            {
                return Some(path);
            }
        }
    }
    None
}

fn digests(path: &Path, size: u64, mtime: Option<SystemTime>) -> Option<Digests> {
    if let Some(hit) = cache().lock().ok()?.get(path).copied() {
        if hit.size == size && hit.mtime == mtime {
            return Some(hit);
        }
    }
    let mut file = std::fs::File::open(path).ok()?;
    let mut sha256 = Sha256::new();
    let mut sha1 = Sha1::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        sha256.update(&buf[..n]);
        sha1.update(&buf[..n]);
    }
    let d = Digests {
        size,
        mtime,
        sha256: sha256.finalize().into(),
        sha1: sha1.finalize().into(),
    };
    if let Ok(mut c) = cache().lock() {
        c.insert(path.to_path_buf(), d);
    }
    Some(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("qml-reuse-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sha256_of(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    #[test]
    fn finds_by_sha256_under_subdir() {
        let dir = tmp("hit");
        let nested = dir.join("libraries").join("a");
        std::fs::create_dir_all(&nested).unwrap();
        let data = b"same content";
        std::fs::write(nested.join("x.jar"), data).unwrap();
        let hit = find_match(
            std::slice::from_ref(&dir),
            data.len() as u64,
            Some(sha256_of(data)),
            None,
        );
        assert_eq!(hit, Some(nested.join("x.jar")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_same_size_different_content() {
        let dir = tmp("miss");
        std::fs::write(dir.join("y.jar"), b"other content").unwrap();
        let miss = find_match(
            std::slice::from_ref(&dir),
            b"other content".len() as u64,
            Some(sha256_of(b"same content ")),
            None,
        );
        assert_eq!(miss, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn requires_at_least_one_hash() {
        let dir = tmp("nohash");
        std::fs::write(dir.join("z.bin"), b"abc").unwrap();
        assert_eq!(find_match(std::slice::from_ref(&dir), 3, None, None), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
