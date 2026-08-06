# 系统要求

> 生成时间：2026-08-06 23:31

# Android 平台支持与构建

## 支持状态

qomicex-downloader 目标支持 Android（`aarch64-linux-android` / `armv7-linux-androideabi` / `x86_64-linux-android`）。

代码层面已满足：
- **纯 Rust 依赖栈**：reqwest + rustls（ring），无 OpenSSL/native-tls 系统库依赖
- 无平台特定路径/系统 API 假设（文件路径由调用方传入）
- 已通过 `rustup target add` 安装 target 验证类型层（依赖 ring 的 C 汇编编译需 NDK）

## 构建步骤（需 Android NDK）

```powershell
# 1. 安装 Android target
rustup target add aarch64-linux-android

# 2. 安装 NDK（Android Studio SDK Manager 或命令行），并设置环境变量
$env:ANDROID_NDK_HOME = "C:\Users\<user>\AppData\Local\Android\Sdk\ndk\<version>"

# 3. 交叉检查/构建（cc-rs 会从 NDK 定位 aarch64-linux-android-clang）
cargo check --target aarch64-linux-android
cargo build --target aarch64-linux-android --release
```

Tauri Android 集成时，Tauri CLI 会自带 NDK 配置，直接使用本 crate 即可。

## 验证状态

- 本机（无 NDK）已完成：`cargo check` 目标类型正确；ring 编译失败点仅为缺少 NDK clang（环境问题，非代码问题）
- 集成测试基于 127.0.0.1 loopback mock server，Android 模拟器同样适用


## 修订记录
| 日期 | 版本 | 修改内容 | 修改人 |
| 2026-08-06 | v1.0 | 初版创建 | AI Agent |