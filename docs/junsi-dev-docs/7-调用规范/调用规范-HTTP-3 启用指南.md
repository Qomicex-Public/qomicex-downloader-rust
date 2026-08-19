# 调用规范/HTTP/3 启用指南

> 生成时间：2026-08-19

# 下游启用 HTTP/3 指南

面向**下游集成方**（Tauri 应用、桌面/移动启动器，以及任何依赖 `qomicex-downloader` 的项目），说明如何按需启用 HTTP/3，以及各档位的取舍。

> HTTP/3 走 reqwest 的 **unstable** 特性，会引入 quinn/h3（QUIC 协议栈）重依赖，并对 Android 交叉编译有额外负担。因此本库将其设计为**默认关闭、双层可选**，下游拥有完全的控制权。

## 1. 三层控制模型

| 控制粒度 | 控制方 | 默认 | 说明 |
|---|---|---|---|
| 编译期是否引入 QUIC 依赖 | 下游 Cargo feature | 关 | 不开则无 quinn，构建/体积/跨平台最轻 |
| 运行时某 Manager 是否用 HTTP/3 | 下游 `enable_http3` | `false` | 每个 Manager 实例独立开关 |
| 运行时 HTTP/3 失败自动回退 HTTP/2 | 库自动 | 开 | 服务器不支持 QUIC 时自动降级，不影响下载 |

三层叠加效果：下游可以"整项目不开"、"项目开但只让部分 Manager 用"、"开了但失败自动回退"——三档都支持。

## 2. 编译期：按需开启 feature

### 方案 A：不开（默认，最轻）
```toml
# 下游 Cargo.toml
[dependencies]
qomicex-downloader = "0.1"
```
- 零 QUIC 依赖，Android 交叉编译无需任何额外配置。
- 运行时 `enable_http3` 即使设为 `true` 也不会构建 h3 client（被 `#[cfg(feature = "http3")]` 编译期剔除），行为等同纯 HTTP/2。

### 方案 B：开启（引入 QUIC）
```toml
# 下游 Cargo.toml
[dependencies]
qomicex-downloader = { version = "0.1", features = ["http3"] }
```
同时**必须**在构建时传 unstable 标志（reqwest 硬性要求，否则编译报错）。推荐写进下游 `.cargo/config.toml`：
```toml
# 下游 .cargo/config.toml
[build]
rustflags = ["--cfg", "reqwest_unstable"]
```
这样 `cargo build` / `cargo tauri build` 自动带上，无需每次手动传。

- 注意：该 feature 只影响**本下游**及其依赖链，不影响其他项目。
- Android 场景若开启，需评估 quinn 在该平台的表现/体积；配置平台见第 4 节。

## 3. 运行时：逐 Manager 开关

即使编译期开了 feature，是否使用 HTTP/3 仍由每个 `DownloadManager` 的 `DownloadOptions::enable_http3` 决定：

```rust
use qomicex_downloader::{DownloadManager, DownloadOptions};

// 实例 A：该下载器用 HTTP/3（要求编译期已开 http3 feature）
let mut opts_h3 = DownloadOptions::default();
opts_h3.enable_http3 = true;
let m_h3 = DownloadManager::new(opts_h3, 8);

// 实例 B：同进程内另一个下载器仍走 HTTP/2
let m_h2 = DownloadManager::new(DownloadOptions::default(), 8);
```

- 同一进程可共存多个不同协议的 Manager，互不影响。
- 开启 HTTP/3 的实例在服务器不支持 QUIC 时会**自动回退 HTTP/2**（无需人为处理）。

## 4. Android / 跨平台注意

- **默认（不开 feature）**：rustls 纯 Rust TLS，Tauri CLI 自带 NDK，直接 `tauri android build` 即可。
- **开启 feature 后**：引入 quinn，需自行验证 Android 目标上的编译与体积；若追求最小体积与最稳跨平台，建议 Android 端保持关闭，仅桌面端按需开启。

## 5. 常见错误排查

| 现象 | 原因 | 解决 |
|---|---|---|
| 编译报 reqwest unstable 相关错误 | 开了 `http3` feature 但没传 `--cfg reqwest_unstable` | 配置 `.cargo/config.toml` 的 `rustflags` |
| 设了 `enable_http3=true` 但没走 H3 | 编译期 feature 未开，h3 被 `#[cfg]` 剔除 | 启用 `features = ["http3"]` |
| 服务器不支持 QUIC，下载仍成功 | 运行时自动回退 HTTP/2（预期行为） | — |
