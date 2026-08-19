# AGENTS.md

## 项目

qomicex-downloader-rust：Rust 下载核心库（`crate-type = ["lib"]`），面向 Minecraft 启动器/Tauri/Android。

## 常用命令

## 命令

```powershell
cargo build                # 编译（默认路径，不含 HTTP/3）
cargo test                 # 全部测试（单元 + 集成 + doc）
cargo test --test download_core <name>   # 单个集成测试
cargo clippy --all-targets # lint（保持 0 警告）
cargo doc --no-deps        # 文档

# HTTP/3（可选 feature，需 unstable 标志）：
RUSTFLAGS="--cfg reqwest_unstable" cargo build --features http3
RUSTFLAGS="--cfg reqwest_unstable" cargo clippy --all-targets --features http3
```

## 约定

- 每次改动后必须运行 `cargo test` 和 `cargo clippy --all-targets`，粘贴输出
- 新增/修改 API、架构 → 更新 `docs/junsi-dev-docs/`
- 新增/修改依赖或构建命令 → 更新本文件
- 关键决策 → 追加 `.memory/decisions/`（gitignored）
- 测试依赖 `tests/common/mod.rs` 的 mock HTTP 服务器（支持 Range/flaky/stall/throttle 注入），新用例复用之
- Windows 特有坑：只读句柄 `sync_all` 返回 ACCESS_DENIED（finalize 必须用读写句柄）；rename 目标存在需先删

## 依赖（2026-08-19 更新）

- 主依赖：tokio / reqwest(rustls, http2) / futures-util / thiserror / rand / tokio-util / **sha2**（SHA-256 校验）
- Cargo features：`default=[]`；`http3` 可选（挂载 `reqwest/http3`，引入 quinn/h3 QUIC 栈，需 `--cfg reqwest_unstable`，仅供非 Android 场景按需开启）
- dev 依赖：sha2（测试计算校验和）
- 依赖变更须同步更新本清单与 Cargo.toml
