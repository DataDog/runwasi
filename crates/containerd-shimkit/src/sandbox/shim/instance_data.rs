use chrono::{DateTime, Utc};
use tokio::sync::{OnceCell, RwLock};

use crate::sandbox::shim::task_state::TaskState;
use crate::sandbox::{ExecConfig, Instance, InstanceConfig, Result};

pub(super) struct InstanceData<T: Instance> {
    pub instance: T,
    pub config: InstanceConfig,
    pid: OnceCell<u32>,
    state: RwLock<TaskState>,
}

impl<T: Instance> InstanceData<T> {
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "Debug"))]
    pub async fn new(
        id: impl AsRef<str> + std::fmt::Debug,
        config: InstanceConfig,
    ) -> Result<Self> {
        let id = id.as_ref().to_string();
        let instance = T::new(id, &config).await?;
        Ok(Self {
            instance,
            config,
            pid: OnceCell::default(),
            state: RwLock::new(TaskState::Created),
        })
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Debug"))]
    pub fn pid(&self) -> Option<u32> {
        self.pid.get().copied()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Debug"))]
    pub async fn start(&self) -> Result<u32> {
        let mut s = self.state.write().await;
        s.start()?;

        let res = self.instance.start().await;

        // These state transitions are always `Ok(())` because
        // we hold the lock since `s.start()`
        let _ = match res {
            Ok(pid) => {
                let _ = self.pid.set(pid);
                s.started()
            }
            Err(_) => s.stop(),
        };

        res
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Debug"))]
    pub async fn kill(&self, signal: u32) -> Result<()> {
        let mut s = self.state.write().await;
        s.kill()?;

        self.instance.kill(signal).await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Debug"))]
    pub async fn delete(&self) -> Result<()> {
        let mut s = self.state.write().await;
        s.delete()?;

        let res = self.instance.delete().await;

        if res.is_err() {
            // Always `Ok(())` because we hold the lock since `s.delete()`
            let _ = s.stop();
        }

        res
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self), level = "Debug"))]
    pub async fn wait(&self) -> (u32, DateTime<Utc>) {
        let res = self.instance.wait().await;
        let mut s = self.state.write().await;
        *s = TaskState::Exited;
        res
    }

    pub async fn register_exec(&self, exec_id: String, cfg: ExecConfig) -> Result<()> {
        self.instance.register_exec(exec_id, cfg).await
    }

    pub async fn start_exec(&self, exec_id: &str) -> Result<u32> {
        self.instance.start_exec(exec_id).await
    }

    pub async fn kill_exec(&self, exec_id: &str, signal: u32) -> Result<()> {
        self.instance.kill_exec(exec_id, signal).await
    }

    pub async fn wait_exec(&self, exec_id: &str) -> Result<(u32, DateTime<Utc>)> {
        self.instance.wait_exec(exec_id).await
    }

    pub async fn delete_exec(&self, exec_id: &str) -> Result<()> {
        self.instance.delete_exec(exec_id).await
    }

    pub async fn exec_pid(&self, exec_id: &str) -> Result<Option<u32>> {
        self.instance.exec_pid(exec_id).await
    }
}
