/// 一个下载段：`[start, end]` 闭区间，`downloaded` 为相对 `start` 已写入字节数。
#[derive(Clone, Debug)]
pub(crate) struct Segment {
    pub index: u32,
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
}

impl Segment {
    pub fn new(index: u32, start: u64, end: u64) -> Self {
        Self {
            index,
            start,
            end,
            downloaded: 0,
        }
    }

    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn finished(&self) -> bool {
        self.downloaded >= self.len()
    }

    /// 断点续传对齐：按 `.part` 现有大小估算各段已下载字节。
    /// 基于写盘区间不重叠、重复写入幂等的假设，误差由后续请求覆盖修正。
    pub fn align_to_part(&mut self, part_size: u64) {
        self.downloaded = part_size.saturating_sub(self.start).min(self.len());
    }

    /// 动态拆分：沿中间切成两段。A 段继承已下载字节（截断到 A 长度），
    /// B 段从新起点重新下载（重复写区间幂等，安全）。
    pub fn split(&self, next_index: u32) -> (Segment, Segment) {
        let len = self.len();
        let half = len / 2;
        let a = Segment {
            index: self.index,
            start: self.start,
            end: self.start + half - 1,
            downloaded: self.downloaded.min(half),
        };
        let b = Segment {
            index: next_index,
            start: self.start + half,
            end: self.end,
            downloaded: self.downloaded.saturating_sub(half),
        };
        (a, b)
    }
}

/// 按"段大小导向"切片：总大小 ≤ `split_threshold` 时单段（小文件直传）；
/// 否则均分为每段 ≈ `segment_size`，段数不超过 `max_segments`。
pub(crate) fn build_segments(
    total: u64,
    segment_size: u64,
    max_segments: u32,
    split_threshold: u64,
) -> Vec<Segment> {
    if total == 0 || total <= split_threshold {
        return vec![Segment::new(0, 0, total.saturating_sub(1))];
    }
    let mut n = total.div_ceil(segment_size);
    n = n.min(max_segments as u64).max(1);
    let base = total / n;
    let rem = total % n;
    (0..n)
        .map(|i| {
            let start = i * base + i.min(rem);
            let len = base + u64::from(i < rem);
            Segment::new(i as u32, start, start + len - 1)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_file_single_segment() {
        let segs = build_segments(8 * 1024, 8 * 1024 * 1024, 16, 10 * 1024 * 1024);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].len(), 8 * 1024);
    }

    #[test]
    fn big_file_split_by_size() {
        let segs = build_segments(30 * 1024 * 1024, 8 * 1024 * 1024, 16, 10 * 1024 * 1024);
        assert_eq!(segs.len(), 4);
        let total: u64 = segs.iter().map(|s| s.len()).sum();
        assert_eq!(total, 30 * 1024 * 1024);
        for w in segs.windows(2) {
            assert_eq!(w[0].end + 1, w[1].start);
        }
    }

    #[test]
    fn split_bounds() {
        let s = Segment::new(0, 0, 99);
        let (a, b) = s.split(1);
        assert_eq!(a.len() + b.len(), 100);
        assert_eq!(a.end + 1, b.start);
        assert_eq!(a.downloaded + b.downloaded, 0);
    }

    #[test]
    fn split_preserves_downloaded() {
        let s = Segment {
            index: 0,
            start: 0,
            end: 99,
            downloaded: 60,
        };
        let (a, b) = s.split(1);
        assert_eq!(a.downloaded, 50);
        assert_eq!(b.downloaded, 10);
        assert_eq!(a.downloaded + b.downloaded, 60);
    }

    #[test]
    fn align_clamps() {
        let mut s = Segment::new(0, 100, 199);
        s.align_to_part(150);
        assert_eq!(s.downloaded, 50);
        s.align_to_part(1000);
        assert_eq!(s.downloaded, 100);
        s.align_to_part(0);
        assert_eq!(s.downloaded, 0);
    }
}
