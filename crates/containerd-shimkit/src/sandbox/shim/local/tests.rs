use std::collections::HashMap;
use std::fs::{File, create_dir};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use containerd_shim::api::Status;
use containerd_shim::event::Event;
use protobuf::{MessageDyn, SpecialFields};
use serde_json as json;
use tempfile::tempdir;
use tokio::sync::RwLock;
use tokio::sync::mpsc::{UnboundedSender as Sender, unbounded_channel as channel};
use tokio_async_drop::tokio_async_drop;

use super::*;
use crate::sandbox::shim::events::EventSender;
use crate::sandbox::sync::WaitableCell;

struct ExecEntry {
    pid: Option<u32>,
    exit_code: WaitableCell<(u32, DateTime<Utc>)>,
}

/// In-memory instance stub. Handles both container and exec lifecycles.
pub struct InstanceStub {
    exit_code: WaitableCell<(u32, DateTime<Utc>)>,
    execs: Arc<RwLock<HashMap<String, ExecEntry>>>,
}

impl Instance for InstanceStub {
    async fn new(_id: String, _cfg: &InstanceConfig) -> Result<Self, Error> {
        Ok(Self {
            exit_code: WaitableCell::new(),
            execs: Arc::default(),
        })
    }
    async fn start(&self) -> Result<u32, Error> {
        Ok(std::process::id())
    }
    async fn kill(&self, _signal: u32) -> Result<(), Error> {
        let _ = self.exit_code.set((1, Utc::now()));
        Ok(())
    }
    async fn delete(&self) -> Result<(), Error> {
        Ok(())
    }
    async fn wait(&self) -> (u32, DateTime<Utc>) {
        *self.exit_code.wait().await
    }

    async fn register_exec(&self, exec_id: String, _cfg: ExecConfig) -> Result<(), Error> {
        self.execs.write().await.insert(
            exec_id,
            ExecEntry {
                pid: None,
                exit_code: WaitableCell::new(),
            },
        );
        Ok(())
    }
    async fn start_exec(&self, exec_id: &str) -> Result<u32, Error> {
        let mut execs = self.execs.write().await;
        let entry = execs
            .get_mut(exec_id)
            .ok_or_else(|| Error::NotFound(exec_id.to_string()))?;
        let pid = std::process::id();
        entry.pid = Some(pid);
        Ok(pid)
    }
    async fn kill_exec(&self, exec_id: &str, _signal: u32) -> Result<(), Error> {
        let exit_code = {
            let execs = self.execs.read().await;
            let entry = execs
                .get(exec_id)
                .ok_or_else(|| Error::NotFound(exec_id.to_string()))?;
            if entry.pid.is_none() {
                return Err(Error::FailedPrecondition("exec not started".to_string()));
            }
            entry.exit_code.clone()
        };
        let _ = exit_code.set((1, Utc::now()));
        Ok(())
    }
    async fn wait_exec(&self, exec_id: &str) -> Result<(u32, DateTime<Utc>), Error> {
        let exit_code = {
            let execs = self.execs.read().await;
            let entry = execs
                .get(exec_id)
                .ok_or_else(|| Error::NotFound(exec_id.to_string()))?;
            entry.exit_code.clone()
        };
        Ok(*exit_code.wait().await)
    }
    async fn delete_exec(&self, exec_id: &str) -> Result<(), Error> {
        self.execs.write().await.remove(exec_id);
        Ok(())
    }
    async fn exec_pid(&self, exec_id: &str) -> Result<Option<u32>, Error> {
        let execs = self.execs.read().await;
        let entry = execs
            .get(exec_id)
            .ok_or_else(|| Error::NotFound(exec_id.to_string()))?;
        Ok(entry.pid)
    }
}

// ─────────────────────────────────────────────────────────────────────────────

struct LocalWithDestructor<T: Instance + Send + Sync, E: EventSender> {
    local: Arc<Local<T, E>>,
}

impl<T: Instance + Send + Sync, E: EventSender> LocalWithDestructor<T, E> {
    fn new(local: Arc<Local<T, E>>) -> Self {
        Self { local }
    }
}

impl EventSender for Sender<(String, Box<dyn MessageDyn>)> {
    fn send(&self, event: impl Event) {
        let _ = self.send((event.topic(), Box::new(event)));
    }
}

impl<T: Instance + Send + Sync, E: EventSender> Drop for LocalWithDestructor<T, E> {
    fn drop(&mut self) {
        tokio_async_drop!({
            let instances = self.local.instances.write().await;
            for (_, instance) in instances.iter() {
                let _ = instance.kill(9).await;
                let _ = instance.delete().await;
            }
        })
    }
}

fn with_cri_sandbox(spec: Option<Spec>, id: String) -> Spec {
    let mut s = spec.unwrap_or_default();
    let mut annotations = HashMap::new();
    s.annotations().as_ref().map(|a| {
        a.iter().map(|(k, v)| {
            annotations.insert(k.to_string(), v.to_string());
        })
    });
    annotations.insert("io.kubernetes.cri.sandbox-id".to_string(), id);

    s.set_annotations(Some(annotations));
    s
}

fn create_bundle(dir: &std::path::Path, spec: Option<Spec>) -> Result<()> {
    create_dir(dir.join("rootfs"))?;

    let s = spec.unwrap_or_default();

    json::to_writer(File::create(dir.join("config.json"))?, &s)
        .context("could not write config.json")?;
    Ok(())
}

// Use a multi threaded runtime because LocalWithDestructor needs
// it to run its async drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_delete_after_create() -> anyhow::Result<()> {
    let dir = tempdir().unwrap();
    let id = "test-delete-after-create";
    create_bundle(dir.path(), None).unwrap();

    let (tx, _rx) = channel();
    let local = Arc::new(Local::<InstanceStub, _>::new(
        tx,
        WaitableCell::new(),
        "test_namespace",
        "/test/address",
    ));
    let mut _wrapped = LocalWithDestructor::new(local.clone());

    local
        .task_create(CreateTaskRequest {
            id: id.to_string(),
            bundle: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await?;

    local
        .task_delete(DeleteRequest {
            id: id.to_string(),
            ..Default::default()
        })
        .await?;

    Ok(())
}

// Use a multi threaded runtime because LocalWithDestructor needs
// it to run its async drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cri_task() -> Result<()> {
    // Currently the relationship between the "base" container and the "instances" are pretty weak.
    // When a cri sandbox is specified we just assume it's the sandbox container and treat it as such by not actually running the code (which is going to be wasm).
    let (etx, _erx) = channel();
    let exit_signal = WaitableCell::new();
    let local = Arc::new(Local::<InstanceStub, _>::new(
        etx,
        exit_signal,
        "test_namespace",
        "/test/address",
    ));

    let mut _wrapped = LocalWithDestructor::new(local.clone());

    let temp = tempdir().unwrap();
    let dir = temp.path();
    let sandbox_id = "test-cri-task".to_string();
    create_bundle(dir, Some(with_cri_sandbox(None, sandbox_id.clone())))?;

    local
        .task_create(CreateTaskRequest {
            id: "testbase".to_string(),
            bundle: dir.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await?;

    let state = local
        .task_state(StateRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::CREATED);

    // make sure that the instance exists
    let _i = local.get_instance("testbase").await?;

    local
        .task_start(StartRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await?;

    let state = local
        .task_state(StateRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::RUNNING);

    let ll = local.clone();
    let (base_tx, mut base_rx) = channel();
    tokio::spawn(async move {
        let resp = ll
            .task_wait(WaitRequest {
                id: "testbase".to_string(),
                ..Default::default()
            })
            .await;
        base_tx.send(resp).unwrap();
    });
    base_rx.try_recv().unwrap_err();

    let temp2 = tempdir().unwrap();
    let dir2 = temp2.path();
    create_bundle(dir2, Some(with_cri_sandbox(None, sandbox_id)))?;

    local
        .task_create(CreateTaskRequest {
            id: "testinstance".to_string(),
            bundle: dir2.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await?;

    let state = local
        .task_state(StateRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::CREATED);

    // make sure that the instance exists
    let _i = local.get_instance("testinstance").await?;

    local
        .task_start(StartRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await?;

    let state = local
        .task_state(StateRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::RUNNING);

    let stats = local
        .task_stats(StatsRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await?;
    assert!(stats.has_stats());

    let ll = local.clone();
    let (instance_tx, mut instance_rx) = channel();
    tokio::spawn(async move {
        let resp = ll
            .task_wait(WaitRequest {
                id: "testinstance".to_string(),
                ..Default::default()
            })
            .await;
        instance_tx.send(resp).unwrap();
    });
    instance_rx.try_recv().unwrap_err();

    local
        .task_kill(KillRequest {
            id: "testinstance".to_string(),
            signal: 9,
            ..Default::default()
        })
        .await?;

    instance_rx
        .recv()
        .with_timeout(Duration::from_secs(50))
        .await
        .flatten()
        .unwrap()?;

    let state = local
        .task_state(StateRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::STOPPED);
    local
        .task_delete(DeleteRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await?;

    match local
        .task_state(StateRequest {
            id: "testinstance".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err()
    {
        Error::NotFound(_) => {}
        e => return Err(e),
    }

    base_rx.try_recv().unwrap_err();
    let state = local
        .task_state(StateRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::RUNNING);

    local
        .task_kill(KillRequest {
            id: "testbase".to_string(),
            signal: 9,
            ..Default::default()
        })
        .await?;

    base_rx
        .recv()
        .with_timeout(Duration::from_secs(5))
        .await
        .flatten()
        .unwrap()?;
    let state = local
        .task_state(StateRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::STOPPED);

    local
        .task_delete(DeleteRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await?;
    match local
        .task_state(StateRequest {
            id: "testbase".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err()
    {
        Error::NotFound(_) => {}
        e => return Err(e),
    }

    Ok(())
}

// Use a multi threaded runtime because LocalWithDestructor needs
// it to run its async drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_task_lifecycle() -> Result<()> {
    let (etx, _erx) = channel(); // TODO: check events
    let exit_signal = WaitableCell::new();
    let local = Arc::new(Local::<InstanceStub, _>::new(
        etx,
        exit_signal,
        "test_namespace",
        "/test/address",
    ));

    let mut _wrapped = LocalWithDestructor::new(local.clone());

    let temp = tempdir().unwrap();
    let dir = temp.path();
    create_bundle(dir, None)?;

    match local
        .task_state(StateRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err()
    {
        Error::NotFound(_) => {}
        e => return Err(e),
    }

    local
        .task_create(CreateTaskRequest {
            id: "test".to_string(),
            bundle: dir.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await?;

    match local
        .task_create(CreateTaskRequest {
            id: "test".to_string(),
            bundle: dir.to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err()
    {
        Error::AlreadyExists(_) => {}
        e => return Err(e),
    }

    let state = local
        .task_state(StateRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await?;

    assert_eq!(state.status(), Status::CREATED);

    local
        .task_start(StartRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await?;

    let state = local
        .task_state(StateRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await?;

    assert_eq!(state.status(), Status::RUNNING);

    let (tx, mut rx) = channel();
    let ll = local.clone();
    tokio::spawn(async move {
        let resp = ll
            .task_wait(WaitRequest {
                id: "test".to_string(),
                ..Default::default()
            })
            .await;
        tx.send(resp).unwrap();
    });

    rx.try_recv().unwrap_err();

    let res = local
        .task_stats(StatsRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await?;
    assert!(res.has_stats());

    local
        .task_kill(KillRequest {
            id: "test".to_string(),
            signal: 9,
            ..Default::default()
        })
        .await?;

    rx.recv()
        .with_timeout(Duration::from_secs(5))
        .await
        .flatten()
        .unwrap()?;

    let state = local
        .task_state(StateRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await?;
    assert_eq!(state.status(), Status::STOPPED);

    local
        .task_delete(DeleteRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await?;

    match local
        .task_state(StateRequest {
            id: "test".to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err()
    {
        Error::NotFound(_) => {}
        e => return Err(e),
    }

    Ok(())
}

// ── Exec tests ───────────────────────────────────────────────────────────────

fn make_exec_local() -> (
    Arc<Local<InstanceStub, Sender<(String, Box<dyn MessageDyn>)>>>,
    LocalWithDestructor<InstanceStub, Sender<(String, Box<dyn MessageDyn>)>>,
) {
    let (tx, _rx) = channel();
    let local = Arc::new(Local::<InstanceStub, _>::new(
        tx,
        WaitableCell::new(),
        "test_ns",
        "/test/addr",
    ));
    let wrapped = LocalWithDestructor::new(local.clone());
    (local, wrapped)
}

/// Registers an exec sub-process and drives it through its full lifecycle:
/// CREATED -> RUNNING -> (kill) -> STOPPED -> delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_exec_lifecycle() -> anyhow::Result<()> {
    let (local, _guard) = make_exec_local();
    let dir = tempdir()?;
    create_bundle(dir.path(), None)?;

    local
        .task_create(CreateTaskRequest {
            id: "c1".into(),
            bundle: dir.path().to_str().unwrap().into(),
            ..Default::default()
        })
        .await?;
    local
        .task_start(StartRequest {
            id: "c1".into(),
            ..Default::default()
        })
        .await?;

    // register
    local
        .task_exec(ExecProcessRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .await?;

    // state before start -> CREATED
    let s = local
        .task_state(StateRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .await?;
    assert_eq!(s.status(), Status::CREATED, "expected CREATED before start");

    // start
    local
        .task_start(StartRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .await?;

    // state after start -> RUNNING
    let s = local
        .task_state(StateRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .await?;
    assert_eq!(s.status(), Status::RUNNING, "expected RUNNING after start");

    // kill
    local
        .task_kill(KillRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            signal: 9,
            ..Default::default()
        })
        .await?;

    // wait resolves
    let _ = local
        .task_wait(WaitRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .with_timeout(Duration::from_secs(5))
        .await
        .into_iter()
        .flatten();

    // state after exit -> STOPPED
    let s = local
        .task_state(StateRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .await?;
    assert_eq!(s.status(), Status::STOPPED, "expected STOPPED after exit");

    // delete succeeds
    local
        .task_delete(DeleteRequest {
            id: "c1".into(),
            exec_id: "e1".into(),
            ..Default::default()
        })
        .await?;

    Ok(())
}

/// Starting an exec that was never registered returns NotFound.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_exec_start_without_register_is_not_found() -> anyhow::Result<()> {
    let (local, _guard) = make_exec_local();
    let dir = tempdir()?;
    create_bundle(dir.path(), None)?;

    local
        .task_create(CreateTaskRequest {
            id: "c2".into(),
            bundle: dir.path().to_str().unwrap().into(),
            ..Default::default()
        })
        .await?;
    local
        .task_start(StartRequest {
            id: "c2".into(),
            ..Default::default()
        })
        .await?;

    let err = local
        .task_start(StartRequest {
            id: "c2".into(),
            exec_id: "ghost".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(
        matches!(err, Error::NotFound(_)),
        "expected NotFound, got {err:?}"
    );
    Ok(())
}

/// Deleting a still-running exec returns FailedPrecondition.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_exec_delete_running_is_failed_precondition() -> anyhow::Result<()> {
    let (local, _guard) = make_exec_local();
    let dir = tempdir()?;
    create_bundle(dir.path(), None)?;

    local
        .task_create(CreateTaskRequest {
            id: "c3".into(),
            bundle: dir.path().to_str().unwrap().into(),
            ..Default::default()
        })
        .await?;
    local
        .task_start(StartRequest {
            id: "c3".into(),
            ..Default::default()
        })
        .await?;

    local
        .task_exec(ExecProcessRequest {
            id: "c3".into(),
            exec_id: "e3".into(),
            ..Default::default()
        })
        .await?;
    local
        .task_start(StartRequest {
            id: "c3".into(),
            exec_id: "e3".into(),
            ..Default::default()
        })
        .await?;

    let err = local
        .task_delete(DeleteRequest {
            id: "c3".into(),
            exec_id: "e3".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(
        matches!(err, Error::FailedPrecondition(_)),
        "expected FailedPrecondition, got {err:?}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_default_runtime_options() -> Result<()> {
    let options: Option<&Any> = None;

    let config = Config::get_from_options(options).unwrap();

    assert_eq!(config.systemd_cgroup, false);

    Ok(())
}

#[test]
fn test_custom_runtime_options() -> Result<()> {
    let options = Options {
        type_url: "runtimeoptions.v1.Options".to_string(),
        config_path: "".to_string(),
        config_body: "SystemdCgroup = true\n".to_string(),
    };
    let req = CreateTaskRequest {
        options: Some(Any {
            type_url: options.type_url.clone(),
            value: options.encode_to_vec(),
            special_fields: SpecialFields::default(),
        })
        .into(),
        ..Default::default()
    };

    let config = Config::get_from_options(req.options.as_ref()).unwrap();

    assert_eq!(config.systemd_cgroup, true);

    Ok(())
}
