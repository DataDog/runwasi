use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use containerd_shim::error::Error as ShimError;
use containerd_shimkit::sandbox::{
    Error as SandboxError, ExecConfig, Instance as SandboxInstance, InstanceConfig,
};

use crate::shim::Shim;

pub struct Instance<S: Shim>(PhantomData<S>);

impl<S: Shim> SandboxInstance for Instance<S> {
    async fn new(_id: String, _cfg: &InstanceConfig) -> Result<Self, SandboxError> {
        todo!();
    }

    /// Start the instance
    /// The returned value should be a unique ID (such as a PID) for the instance.
    /// Nothing internally should be using this ID, but it is returned to containerd where a user may want to use it.
    async fn start(&self) -> Result<u32, SandboxError> {
        todo!();
    }

    /// Send a signal to the instance
    async fn kill(&self, _signal: u32) -> Result<(), SandboxError> {
        todo!();
    }

    /// Delete any reference to the instance
    /// This is called after the instance has exited.
    async fn delete(&self) -> Result<(), SandboxError> {
        todo!();
    }

    /// Waits for the instance to finish and returns its exit code
    /// Returns None if the timeout is reached before the instance has finished.
    /// This is an async call.
    async fn wait(&self) -> (u32, DateTime<Utc>) {
        todo!();
    }

    async fn register_exec(&self, _exec_id: String, _cfg: ExecConfig) -> Result<(), SandboxError> {
        todo!()
    }

    async fn start_exec(&self, _exec_id: &str) -> Result<u32, SandboxError> {
        todo!()
    }

    async fn kill_exec(&self, _exec_id: &str, _signal: u32) -> Result<(), SandboxError> {
        todo!()
    }

    async fn wait_exec(&self, _exec_id: &str) -> Result<(u32, DateTime<Utc>), SandboxError> {
        todo!()
    }

    async fn delete_exec(&self, _exec_id: &str) -> Result<(), SandboxError> {
        todo!()
    }

    async fn exec_pid(&self, _exec_id: &str) -> Result<Option<u32>, SandboxError> {
        todo!()
    }
}
