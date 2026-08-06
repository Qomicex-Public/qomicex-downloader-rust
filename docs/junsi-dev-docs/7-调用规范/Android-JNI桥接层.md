# Android JNI 桥接层（jni.rs）

面向 Android 的 JNI 导出层：Kotlin 通过 `System.loadLibrary("qomicex_downloader")` 加载后调用。

## 导出符号

类名约定：`com.qomicex.launcher.downloader.DownloaderBridge`（Kotlin 侧 `object`，见主仓库 `mobile/android/app/src/main/java/com/qomicex/launcher/downloader/DownloaderBridge.kt`）。

| 方法 | 签名 | 返回 |
|------|------|------|
| `add` | `(url: String, dest: String, sha256Hex: String, headersJson: String) -> String` | `{"id": N}`；失败 `{"id":-1,"error":"..."}` |
| `state` | `(id: Long) -> String` | `{"id","status","downloaded","total","activeSegments"}` |
| `list` | `() -> String` | `[{...}, ...]` |
| `pause` / `resume` / `cancel` / `retry` / `remove` | `(id: Long) -> String` | `{"ok":true}` 或 `{"ok":false,"error":"..."}` |

- 状态字符串对齐前端 `DownloadTask.status`：`queued/downloading/paused/completed/failed/cancelled`。
- `headersJson` 为 `[["k","v"],...]` 形式的 JSON 数组；任务级头与全局默认头合并（任务级覆盖）。
- 所有导出经 `std::panic::catch_unwind` 防护，JNI 层不抛异常。

## 运行时

- 单例 tokio Runtime + 单例 DownloadManager（`DownloadOptions::default()`，全局并发 4）。
- `add` 同步入队后立即返回任务 ID；下载在 tokio worker 上执行。
- 轮询模式：Kotlin 侧每 500ms 轮询 `state` 聚合进度；终态任务调用 `remove` 清理（避免任务表无限增长）。

## 构建

```bash
# 宿主编译/测试（本机无 MSVC，必须用 stable-gnu + MinGW 在 PATH）
cargo +stable-gnu test

# Android 三 ABI 交叉编译（NDK 28.2.13676358）
# CC_<target>=<ndkbin>\clang.exe
# CARGO_TARGET_<TARGET>_LINKER=<ndkbin>\<triple>21-clang.cmd
# AR_<target>=<ndkbin>\llvm-ar.cmd
cargo +stable-gnu build --target aarch64-linux-android --release
cargo +stable-gnu build --target armv7-linux-androideabi --release
cargo +stable-gnu build --target x86_64-linux-android --release
```

产物复制到主仓库 `mobile/android/app/src/main/jniLibs/<abi>/libqomicex_downloader.so` 随 APK 打包。

## 已知事项

- Minecraft 资源使用 SHA-1，Rust 侧仅支持 SHA-256 校验；实例安装场景不传 sha256Hex（空串跳过校验），由 Kotlin 侧对已存在文件做 SHA-1 比对决定是否跳过。
- 不要用 PowerShell `Set-Content` 修改本文件（曾导致 UTF-8 损坏）；`mut env` 之类警告不删 mut，避免 JNIEnv 生命周期问题。
