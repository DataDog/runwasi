use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use containerd_client::tonic::async_trait;
use containerd_shimkit::sandbox::sync::WaitableCell;
use containerd_shimkit::sandbox::{
    Error as SandboxError, ExecConfig, Instance as SandboxInstance, InstanceConfig,
};
use containerd_shimkit::set_logger_kv;
use libcontainer::container::builder::ContainerBuilder;
use libcontainer::syscall::syscall::SyscallType;
use nix::sys::wait::WaitStatus;
use oci_spec::runtime::Spec;
use tokio::sync::{OnceCell, RwLock};

use super::container::Container;
use super::exec::ExecState;
use crate::containerd;
use crate::sandbox::context::WasmLayer;
use crate::shim::{Compiler, Shim};
use crate::sys::container::executor::Executor;
use crate::sys::pid_fd::PidFd;

pub struct Instance<S: Shim> {
    pub(super) exit_code: WaitableCell<(u32, DateTime<Utc>)>,
    pub(super) container: Container,
    pub(super) id: String,
    pub(super) root_path: PathBuf,
    pub(super) is_wasm: bool,
    pub(super) exec_processes: RwLock<HashMap<String, ExecState>>,
    _phantom: PhantomData<S>,
}

#[async_trait]
trait OciClient {
    async fn load_modules<P: Compiler>(
        &self,
        id: &str,
        precompiler: Option<&P>,
    ) -> Result<Vec<WasmLayer>, SandboxError>;
}

struct EngineOciClient {
    client: containerd::Client,
    supported_layer_types: &'static [&'static str],
    name: &'static str,
}

#[async_trait]
impl OciClient for EngineOciClient {
    async fn load_modules<P: Compiler>(
        &self,
        id: &str,
        precompiler: Option<&P>,
    ) -> Result<Vec<WasmLayer>, SandboxError> {
        self.client
            .load_modules(id, self.name, self.supported_layer_types, precompiler)
            .await
    }
}

static OCI_CLIENT: OnceCell<Box<EngineOciClient>> = OnceCell::const_new();

impl<S: Shim> SandboxInstance for Instance<S> {
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "Info"))]
    async fn new(id: String, cfg: &InstanceConfig) -> Result<Self, SandboxError> {
        let oci_client = OCI_CLIENT
            .get_or_try_init(|| async {
                let client =
                    containerd::Client::connect(&cfg.containerd_address, &cfg.namespace).await?;
                let supported_layer_types = S::supported_layers_types();
                let name = S::name();
                Result::<_, SandboxError>::Ok(Box::new(EngineOciClient {
                    client,
                    supported_layer_types,
                    name,
                }))
            })
            .await?;

        let source_spec_path = cfg.bundle.join("config.json");
        let spec = Spec::load(&source_spec_path)?;
        let precompiler = S::compiler_with_spec(&spec).await;

        // check if container is OCI image with wasm layers and attempt to read the module
        let modules = oci_client
            .load_modules(&id, precompiler.as_ref())
            .await
            .unwrap_or_else(|e| {
                log::warn!("Error obtaining wasm layers for container {id}.  Will attempt to use files inside container image. Error: {e}");
                vec![]
            });

        let is_wasm = !modules.is_empty();

        let container = Container::build(
            |(id, cfg, modules)| {
                let source_spec_path = cfg.bundle.join("config.json");
                let spec = Spec::load(source_spec_path)?;
                let pod_id = pod_id(&spec);

                match pod_id {
                    Some(pod_id) => set_logger_kv([("instance", id.as_str()), ("pod", pod_id)]),
                    None => set_logger_kv([("instance", id.as_str())]),
                };

                let rootdir = cfg.determine_rootdir(S::name())?;

                let mut builder = ContainerBuilder::new(id, SyscallType::Linux)
                    .with_executor(Executor::<S>::new(modules))
                    .with_root_path(rootdir.clone())?;

                if let Ok(f) = cfg.open_stdin() {
                    builder = builder.with_stdin(f);
                }
                if let Ok(f) = cfg.open_stdout() {
                    builder = builder.with_stdout(f);
                }
                if let Ok(f) = cfg.open_stderr() {
                    builder = builder.with_stderr(f);
                }

                let container = builder
                    .as_init(&cfg.bundle)
                    .as_sibling(true)
                    .with_systemd(cfg.config.systemd_cgroup)
                    .build()?;

                Ok(container)
            },
            (id.clone(), cfg.clone(), modules),
        )?;

        Ok(Self {
            id,
            exit_code: WaitableCell::new(),
            container,
            root_path: cfg.determine_rootdir(S::name())?,
            is_wasm,
            exec_processes: RwLock::default(),
            _phantom: Default::default(),
        })
    }

    /// Start the instance
    /// The returned value should be a unique ID (such as a PID) for the instance.
    /// Nothing internally should be using this ID, but it is returned to containerd where a user may want to use it.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn start(&self) -> Result<u32, SandboxError> {
        log::info!("starting instance: {}", self.id);
        // make sure we have an exit code by the time we finish (even if there's a panic)
        let guard = self.exit_code.clone().set_guard_with(|| (137, Utc::now()));

        let pid = self.container.pid()?;

        // Use a pidfd FD so that we can wait for the process to exit asynchronously.
        // This should be created BEFORE calling container.start() to ensure we never
        // miss the SIGCHLD event.
        let pidfd = PidFd::new(pid)?;

        self.container.start()?;
        spawn_exit_waiter(pidfd, guard, self.exit_code.clone());
        Ok(pid as _)
    }

    /// Send a signal to the instance
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn kill(&self, signal: u32) -> Result<(), SandboxError> {
        log::info!("sending signal {signal} to instance: {}", self.id);
        self.container.kill(signal)?;
        Ok(())
    }

    /// Delete any reference to the instance
    /// This is called after the instance has exited.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn delete(&self) -> Result<(), SandboxError> {
        log::info!("deleting instance: {}", self.id);
        self.container.delete()?;
        Ok(())
    }

    /// Waits for the instance to finish and returns its exit code
    /// Returns None if the timeout is reached before the instance has finished.
    /// This is an async call.
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn wait(&self) -> (u32, DateTime<Utc>) {
        *self.exit_code.wait().await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn register_exec(&self, exec_id: String, cfg: ExecConfig) -> Result<(), SandboxError> {
        self.exec_register(exec_id, cfg).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn start_exec(&self, exec_id: &str) -> Result<u32, SandboxError> {
        self.exec_start(exec_id).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn kill_exec(&self, exec_id: &str, signal: u32) -> Result<(), SandboxError> {
        self.exec_kill(exec_id, signal).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn wait_exec(&self, exec_id: &str) -> Result<(u32, DateTime<Utc>), SandboxError> {
        self.exec_wait(exec_id).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn delete_exec(&self, exec_id: &str) -> Result<(), SandboxError> {
        self.exec_delete(exec_id).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Info"))]
    async fn exec_pid(&self, exec_id: &str) -> Result<Option<u32>, SandboxError> {
        self.exec_pid_get(exec_id).await
    }
}

fn pod_id(spec: &Spec) -> Option<&str> {
    spec.annotations()
        .as_ref()
        .and_then(|a| a.get("io.kubernetes.cri.sandbox-id"))
        .map(|s| s.as_str())
}

pub(super) fn spawn_exit_waiter<G: Send + 'static>(
    pidfd: PidFd,
    guard: G,
    exit_code: WaitableCell<(u32, DateTime<Utc>)>,
) {
    tokio::spawn(async move {
        let _guard = guard;
        let status = match pidfd.wait().await {
            Ok(WaitStatus::Exited(_, s)) => s,
            Ok(WaitStatus::Signaled(_, sig, _)) => 128 + sig as i32,
            Ok(res) => {
                log::error!("waitpid unexpected result: {res:?}");
                137
            }
            Err(e) => {
                log::error!("waitpid failed: {e}");
                137
            }
        } as u32;
        let _ = exit_code.set((status, Utc::now()));
    });
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use oci_spec::runtime::{ProcessBuilder, RootBuilder, SpecBuilder};

    use super::*;

    #[test]
    fn test_get_pod_id() -> Result<()> {
        use std::collections::HashMap;

        let mut annotations = HashMap::new();
        annotations.insert(
            "io.kubernetes.cri.sandbox-id".to_string(),
            "test-pod-id".to_string(),
        );

        let spec = SpecBuilder::default()
            .root(RootBuilder::default().path("rootfs").build()?)
            .process(ProcessBuilder::default().cwd("/").build()?)
            .annotations(annotations)
            .build()?;

        assert_eq!(pod_id(&spec), Some("test-pod-id"));

        Ok(())
    }

    #[test]
    fn test_get_pod_id_no_annotation() -> Result<()> {
        let spec = SpecBuilder::default()
            .root(RootBuilder::default().path("rootfs").build()?)
            .process(ProcessBuilder::default().cwd("/").build()?)
            .build()?;

        assert_eq!(pod_id(&spec), None);

        Ok(())
    }
}
