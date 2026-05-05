//! Exec sub-process support for native (youki) containers.
//!
//! The pattern of writing the OCI Process spec to a temp file mirrors go-runc:
//! <https://github.com/containerd/go-runc/blob/main/runc.go>
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::os::unix::io::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use containerd_shim::error::Error as ShimError;
use containerd_shimkit::sandbox::sync::WaitableCell;
use containerd_shimkit::sandbox::{Error as SandboxError, ExecConfig};
use futures::FutureExt as _;
use libcontainer::container::Container as YoukiContainer;
use libcontainer::container::builder::ContainerBuilder;
use libcontainer::syscall::syscall::SyscallType;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use super::container::Container;
use super::instance::{Instance, spawn_exit_waiter};
use crate::shim::Shim;
use crate::sys::pid_fd::PidFd;

pub(super) struct ExecState {
    pub(super) config: ExecConfig,
    pub(super) pid: Arc<OnceCell<u32>>,
    pub(super) exit_code: WaitableCell<(u32, DateTime<Utc>)>,
    pub(super) started: Arc<AtomicBool>,
}

/// Arguments for spawning a tenant exec process, serializable for IPC with the zygote.
#[derive(Serialize, Deserialize)]
pub struct ExecTenantArgs {
    pub container_id: String,
    pub exec_id: String,
    pub root_path: PathBuf,
    pub spec: Vec<u8>,
    pub stdin: Option<PathBuf>,
    pub stdout: Option<PathBuf>,
    pub stderr: Option<PathBuf>,
}

/// Accept only alphanumeric, `-`, and `_` — allowlist is safer than denylist for path components.
fn validate_id(id: &str) -> anyhow::Result<()> {
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        anyhow::bail!(
            "invalid id {:?}: must be non-empty and contain only [a-zA-Z0-9_-]",
            id
        );
    }
    Ok(())
}

impl Container {
    /// Spawn an exec sub-process inside the container via the zygote.
    pub fn spawn_exec_tenant(&self, args: ExecTenantArgs) -> anyhow::Result<i32> {
        self.run_impl(
            |_: &mut Option<YoukiContainer>, args: ExecTenantArgs| -> anyhow::Result<i32> {
                validate_id(&args.container_id)?;
                validate_id(&args.exec_id)?;

                // Mirrors runc pattern: write process spec to a temp file and pass
                // the path to youki via with_process().
                // See: https://github.com/containerd/go-runc/blob/main/runc.go
                let dir = std::env::var_os("XDG_RUNTIME_DIR")
                    .map(PathBuf::from)
                    .unwrap_or_else(std::env::temp_dir);
                let mut spec_file = tempfile::Builder::new()
                    .prefix("runwasi-exec-")
                    .suffix(".json")
                    .permissions(std::fs::Permissions::from_mode(0o600))
                    .tempfile_in(&dir)?;
                spec_file.write_all(&args.spec)?;
                spec_file.flush()?;
                let (_, spec_path) = spec_file.keep()?;

                let open_stdin = |p: &PathBuf| -> Option<OwnedFd> {
                    let _unblock = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(p)
                        .ok()?;
                    std::fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_CLOEXEC)
                        .open(p)
                        .ok()
                        .map(OwnedFd::from)
                };

                let open_output = |p: &PathBuf| -> Option<OwnedFd> {
                    std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .custom_flags(libc::O_CLOEXEC)
                        .open(p)
                        .ok()
                        .map(OwnedFd::from)
                };

                let mut builder = ContainerBuilder::new(args.container_id, SyscallType::Linux)
                    .with_root_path(&args.root_path)
                    .map_err(|e| anyhow!(e))?;
                if let Some(p) = args.stdin {
                    if let Some(f) = open_stdin(&p) {
                        builder = builder.with_stdin(f);
                    }
                }
                if let Some(p) = args.stdout {
                    if let Some(f) = open_output(&p) {
                        builder = builder.with_stdout(f);
                    }
                }
                if let Some(p) = args.stderr {
                    if let Some(f) = open_output(&p) {
                        builder = builder.with_stderr(f);
                    }
                }
                let pid = builder
                    .as_tenant()
                    .with_process(Some(&spec_path))
                    .as_sibling(true)
                    .build()
                    .map_err(|e| anyhow!(e))?;

                let _ = std::fs::remove_file(&spec_path);
                Ok(pid.as_raw())
            },
            args,
        )
    }
}

impl<S: Shim> Instance<S> {
    pub(super) async fn exec_register(
        &self,
        exec_id: String,
        cfg: ExecConfig,
    ) -> Result<(), SandboxError> {
        if self.is_wasm {
            return Err(SandboxError::Shim(ShimError::Unimplemented(
                "exec is not supported for WASM containers".to_string(),
            )));
        }
        let state = ExecState {
            config: cfg,
            pid: Arc::new(OnceCell::default()),
            exit_code: WaitableCell::new(),
            started: Arc::new(AtomicBool::new(false)),
        };
        self.exec_processes.write().await.insert(exec_id, state);
        Ok(())
    }

    pub(super) async fn exec_start(&self, exec_id: &str) -> Result<u32, SandboxError> {
        let (cfg, exit_code, pid_cell, started) = {
            let procs = self.exec_processes.read().await;
            let state = procs
                .get(exec_id)
                .ok_or_else(|| SandboxError::NotFound(exec_id.to_string()))?;
            (
                state.config.clone(),
                state.exit_code.clone(),
                state.pid.clone(),
                state.started.clone(),
            )
        };

        // Prevent concurrent or duplicate start of the same exec_id.
        if started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(SandboxError::AlreadyExists(exec_id.to_string()));
        }

        let args = ExecTenantArgs {
            container_id: self.id.clone(),
            exec_id: exec_id.to_string(),
            root_path: self.root_path.clone(),
            spec: cfg.spec,
            stdin: cfg.stdin,
            stdout: cfg.stdout,
            stderr: cfg.stderr,
        };

        let pid_raw = self
            .container
            .spawn_exec_tenant(args)
            .map_err(SandboxError::Any)?;
        let pid = pid_raw as u32;
        let _ = pid_cell.set(pid);

        let guard = exit_code.clone().set_guard_with(|| (137, Utc::now()));
        let pidfd = PidFd::new(pid_raw)?;
        spawn_exit_waiter(pidfd, guard, exit_code);

        Ok(pid)
    }

    pub(super) async fn exec_kill(&self, exec_id: &str, signal: u32) -> Result<(), SandboxError> {
        let (pid, exit_code) = {
            let procs = self.exec_processes.read().await;
            let state = procs
                .get(exec_id)
                .ok_or_else(|| SandboxError::NotFound(exec_id.to_string()))?;
            let pid =
                state.pid.get().copied().ok_or_else(|| {
                    SandboxError::FailedPrecondition("exec not started".to_string())
                })?;
            (pid, state.exit_code.clone())
        };
        // Refuse to signal a stale PID: if the process has already exited its
        // PID may have been recycled by the kernel.
        if exit_code.wait().now_or_never().is_some() {
            return Err(SandboxError::NotFound(format!(
                "exec {exec_id} has already exited"
            )));
        }
        let sig = Signal::try_from(signal as i32)
            .map_err(|e| SandboxError::InvalidArgument(format!("invalid signal: {e}")))?;
        signal::kill(Pid::from_raw(pid as i32), sig)?;
        Ok(())
    }

    pub(super) async fn exec_wait(
        &self,
        exec_id: &str,
    ) -> Result<(u32, DateTime<Utc>), SandboxError> {
        let exit_code = {
            let procs = self.exec_processes.read().await;
            let state = procs
                .get(exec_id)
                .ok_or_else(|| SandboxError::NotFound(exec_id.to_string()))?;
            state.exit_code.clone()
        };
        Ok(*exit_code.wait().await)
    }

    pub(super) async fn exec_delete(&self, exec_id: &str) -> Result<(), SandboxError> {
        self.exec_processes.write().await.remove(exec_id);
        Ok(())
    }

    pub(super) async fn exec_pid_get(&self, exec_id: &str) -> Result<Option<u32>, SandboxError> {
        let procs = self.exec_processes.read().await;
        let state = procs
            .get(exec_id)
            .ok_or_else(|| SandboxError::NotFound(exec_id.to_string()))?;
        Ok(state.pid.get().copied())
    }
}
