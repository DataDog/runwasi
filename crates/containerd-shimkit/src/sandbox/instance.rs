//! Abstractions for running/managing a wasm/wasi instance.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::error::Error;
use crate::sandbox::shim::Config;

/// Configuration for an exec sub-process inside a running container.
#[derive(Clone, Debug, Default)]
pub struct ExecConfig {
    pub stdin: Option<PathBuf>,
    pub stdout: Option<PathBuf>,
    pub stderr: Option<PathBuf>,
    /// OCI Process spec as raw JSON bytes.
    pub spec: Vec<u8>,
}

/// Generic options builder for creating a wasm instance.
/// This is passed to the `Instance::new` method.
#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct InstanceConfig {
    /// Optional stdin named pipe path.
    pub stdin: PathBuf,
    /// Optional stdout named pipe path.
    pub stdout: PathBuf,
    /// Optional stderr named pipe path.
    pub stderr: PathBuf,
    /// Path to the OCI bundle directory.
    pub bundle: PathBuf,
    /// Namespace for containerd
    pub namespace: String,
    /// GRPC address back to main containerd
    pub containerd_address: String,
    /// containerd runtime options config
    pub config: Config,
}

/// Represents a WASI module(s).
/// Instance is a trait that gets implemented by consumers of this library.
/// This trait requires that any type implementing it is `'static`, similar to `std::any::Any`.
/// This means that the type cannot contain a non-`'static` reference.
#[cfg_attr(not(doc), trait_variant::make(Send))]
pub trait Instance: 'static {
    /// Create a new instance
    async fn new(id: String, cfg: &InstanceConfig) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start the instance
    /// The returned value should be a unique ID (such as a PID) for the instance.
    /// Nothing internally should be using this ID, but it is returned to containerd where a user may want to use it.
    async fn start(&self) -> Result<u32, Error>;

    /// Send a signal to the instance
    async fn kill(&self, signal: u32) -> Result<(), Error>;

    /// Delete any reference to the instance
    /// This is called after the instance has exited.
    async fn delete(&self) -> Result<(), Error>;

    /// Waits for the instance to finish and returns its exit code
    /// This is an async call.
    async fn wait(&self) -> (u32, DateTime<Utc>);

    /// Register a sub-process inside the container.
    /// Called when the Exec RPC is received.
    async fn register_exec(&self, exec_id: String, cfg: ExecConfig) -> Result<(), Error>;

    /// Start a previously registered sub-process.  Returns the sub-process PID.
    async fn start_exec(&self, exec_id: &str) -> Result<u32, Error>;

    /// Send a signal to a running sub-process.
    async fn kill_exec(&self, exec_id: &str, signal: u32) -> Result<(), Error>;

    /// Wait for a sub-process to finish, returning its exit code and timestamp.
    async fn wait_exec(&self, exec_id: &str) -> Result<(u32, DateTime<Utc>), Error>;

    /// Delete a finished sub-process and clean up its resources.
    async fn delete_exec(&self, exec_id: &str) -> Result<(), Error>;

    /// Return the PID of a registered sub-process, or None if not yet started.
    async fn exec_pid(&self, exec_id: &str) -> Result<Option<u32>, Error>;
}
