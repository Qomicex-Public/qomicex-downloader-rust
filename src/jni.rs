//! JNI 桥接层：供 Android (Kotlin) 调用下载核心库。
//!
//! 导出约定：`Java_<包名以_分隔>_<类名>_<方法名>`，
//! Kotlin 侧类为 `com.qomicex.launcher.downloader.DownloaderBridge`（见 mobile/android）。
//!
//! 所有方法返回 JSON 字符串；错误时返回 `{"error":"..."}`（add 失败返回 `{"id":-1,"error":"..."}`）。
//! 任务状态字符串对齐前端 DownloadTask.status：queued/downloading/paused/completed/failed/cancelled。

use std::panic::{self, AssertUnwindSafe};
use std::sync::OnceLock;

use jni::objects::{JClass, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use tokio::runtime::Runtime;

use crate::manager::DownloadManager;
use crate::task::{DownloadOptions, DownloadTask, TaskState};

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().expect("failed to create tokio runtime"))
}

fn manager() -> &'static DownloadManager {
    static MGR: OnceLock<DownloadManager> = OnceLock::new();
    MGR.get_or_init(|| DownloadManager::new(DownloadOptions::default(), 4))
}

fn state_name(s: TaskState) -> &'static str {
    match s {
        TaskState::Queued => "queued",
        TaskState::Downloading => "downloading",
        TaskState::Paused => "paused",
        TaskState::Completed => "completed",
        TaskState::Failed => "failed",
        TaskState::Cancelled => "cancelled",
    }
}

fn task_json(mgr: &DownloadManager, id: u64) -> String {
    let rt = runtime();
    let state = rt.block_on(mgr.state(id)).unwrap_or(TaskState::Failed);
    let prog = rt.block_on(mgr.progress(id)).unwrap_or_default();
    serde_json::json!({
        "id": id,
        "status": state_name(state),
        "downloaded": prog.downloaded,
        "total": prog.total,
        "activeSegments": prog.active_segments,
    })
    .to_string()
}

/// 解析 `[["name","value"],...]` 形式的请求头 JSON。
fn parse_headers(json: &str) -> Vec<(String, String)> {
    if json.trim().is_empty() {
        return Vec::new();
    }
    serde_json::from_str::<Vec<[String; 2]>>(json)
        .map(|pairs| pairs.into_iter().map(|p| (p[0].clone(), p[1].clone())).collect())
        .unwrap_or_default()
}

/// 添加下载任务。
/// Kotlin 调用：`DownloaderBridge.add(url: String, dest: String, sha256Hex: String?, headersJson: String): Long`
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_qomicex_launcher_downloader_DownloaderBridge_add(
    mut env: JNIEnv,
    _class: JClass,
    url: JString,
    dest: JString,
    sha256_hex: JString,
    headers_json: JString,
) -> jstring {
    let body = panic::catch_unwind(AssertUnwindSafe(|| {
        let url: String = env.get_string(&url).map(|v| v.into()).unwrap_or_default();
        let dest: String = env.get_string(&dest).map(|v| v.into()).unwrap_or_default();
        let sha256_hex: String = env.get_string(&sha256_hex).map(|v| v.into()).unwrap_or_default();
        let headers_json: String = env.get_string(&headers_json).map(|v| v.into()).unwrap_or_default();
        let headers = parse_headers(&headers_json);

        let mut task = DownloadTask::new(url, dest);
        if !sha256_hex.is_empty() {
            task = task.with_sha256_hex(&sha256_hex);
        }
        for (k, v) in headers {
            task = task.with_header(k, v);
        }
        let id = manager().add(task);
        serde_json::json!({ "id": id }).to_string()
    }));

    let body = match body {
        Ok(b) => b,
        Err(_) => r#"{"id":-1,"error":"add failed"}"#.to_string(),
    };
    env.new_string(&body)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// 查询任务状态。
/// Kotlin 调用：`DownloaderBridge.state(id: Long): String`
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_qomicex_launcher_downloader_DownloaderBridge_state(
    env: JNIEnv,
    _class: JClass,
    id: jlong,
) -> jstring {
    let body = panic::catch_unwind(AssertUnwindSafe(|| task_json(manager(), id as u64)));
    let body = body.unwrap_or_else(|_| r#"{"id":-1,"status":"failed","error":"state failed"}"#.to_string());
    env.new_string(&body)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// 任务列表。
/// Kotlin 调用：`DownloaderBridge.list(): String`
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_qomicex_launcher_downloader_DownloaderBridge_list(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let body = panic::catch_unwind(AssertUnwindSafe(|| {
        let rt = runtime();
        let ids: Vec<u64> = rt.block_on(manager().list()).into_iter().map(|(id, _)| id).collect();
        let arr: Vec<serde_json::Value> = ids
            .iter()
            .map(|id| {
                serde_json::from_str::<serde_json::Value>(&task_json(manager(), *id))
                    .unwrap_or(serde_json::Value::Null)
            })
            .collect();
        serde_json::Value::Array(arr).to_string()
    }));
    let body = body.unwrap_or_else(|_| "[]".to_string());
    env.new_string(&body)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

fn op_result(
    rt: &Runtime,
    fut: impl std::future::Future<Output = Result<(), crate::error::DownloadError>>,
) -> String {
    match rt.block_on(fut) {
        Ok(()) => r#"{"ok":true}"#.to_string(),
        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }).to_string(),
    }
}

macro_rules! define_op {
    ($name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "system" fn $name(env: JNIEnv, _class: JClass, id: jlong) -> jstring {
            let body = panic::catch_unwind(AssertUnwindSafe(|| {
                op_result(runtime(), manager().$method(id as u64))
            }));
            let body = body.unwrap_or_else(|_| r#"{"ok":false,"error":"jni panic"}"#.to_string());
            env.new_string(&body)
                .map(|s| s.into_raw())
                .unwrap_or(std::ptr::null_mut())
        }
    };
}

define_op!(Java_com_qomicex_launcher_downloader_DownloaderBridge_pause, pause);
define_op!(Java_com_qomicex_launcher_downloader_DownloaderBridge_resume, resume);
define_op!(Java_com_qomicex_launcher_downloader_DownloaderBridge_cancel, cancel);
define_op!(Java_com_qomicex_launcher_downloader_DownloaderBridge_retry, retry);
define_op!(Java_com_qomicex_launcher_downloader_DownloaderBridge_remove, remove);
