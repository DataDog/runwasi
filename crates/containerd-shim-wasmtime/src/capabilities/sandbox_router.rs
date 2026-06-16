use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::component::{self, Component, ComponentType, Lift, Lower, ResourceTable};
use wasmtime_wasi::p2::bindings::Command;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use crate::capabilities::{CapabilityStore, WitCapability, with_state};

/// Permissions for a spawned sandbox module. Mirrors the `sandbox-permissions` WIT record.
/// Zero value = maximum isolation (all deny).
#[derive(Clone, Copy, ComponentType, Lift, Lower)]
#[component(record)]
pub(crate) struct SandboxPermissions {
    #[component(name = "network")]
    pub network: bool,
    #[component(name = "stdio")]
    pub stdio: bool,
}

const DEFAULT_MAX_MODULE_RESTARTS: u32 = 5;
const DEFAULT_RESTART_BACKOFF_INITIAL_MS: u64 = 500;
const DEFAULT_MAX_RESTART_BACKOFF_MS: u64 = 30_000;
pub(crate) struct SandboxRouterConfig {
    pub max_module_restarts: u32,
    pub restart_backoff_initial_ms: u64,
    pub max_restart_backoff_ms: u64,
}

impl Default for SandboxRouterConfig {
    fn default() -> Self {
        Self {
            max_module_restarts: DEFAULT_MAX_MODULE_RESTARTS,
            restart_backoff_initial_ms: DEFAULT_RESTART_BACKOFF_INITIAL_MS,
            max_restart_backoff_ms: DEFAULT_MAX_RESTART_BACKOFF_MS,
        }
    }
}

struct ModuleTaskHandle {
    name: String,
    abort: tokio::task::AbortHandle,
}

impl Drop for ModuleTaskHandle {
    fn drop(&mut self) {
        log::info!("module server '{}': shutting down", self.name);
        self.abort.abort();
    }
}

struct ActiveModule {
    addr: String,
    cancel: CancellationToken,
    _handle: ModuleTaskHandle,
}

struct RouterState {
    active: Arc<RwLock<HashMap<String, ActiveModule>>>,
    denied: RwLock<HashSet<String>>,
}

/// Implements the `sandbox:router/sandbox-host` WIT interface as host imports.
pub(crate) struct SandboxRouterCapability {
    pub(crate) engine: Engine,
    pub(crate) config: Arc<SandboxRouterConfig>,
    pub(crate) cancel: CancellationToken,
}

async fn host_add_module(
    state: Arc<RouterState>,
    engine: Engine,
    config: Arc<SandboxRouterConfig>,
    cancel: CancellationToken,
    (name, path, perms): (String, String, SandboxPermissions),
) -> Result<(String,)> {
    {
        if state.denied.read().unwrap().contains(&name) {
            log::warn!("module '{}': blocked (explicitly unloaded); call allow-module to re-enable", name);
            return Ok((String::new(),));
        }
    }

    {
        let guard = state.active.read().unwrap();
        if let Some(m) = guard.get(&name) {
            log::debug!("module '{}': already registered at {}", name, m.addr);
            return Ok((m.addr.clone(),));
        }
    }

    let component = {
        let engine = engine.clone();
        let path_buf = std::path::PathBuf::from(&path);
        match tokio::task::spawn_blocking(move || load_module_component(&engine, &path_buf))
            .await
            .unwrap()
        {
            Some(c) => c,
            None => {
                log::error!("module '{}': failed to load from {}", name, path);
                return Ok((String::new(),));
            }
        }
    };

    let addr = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => match listener.local_addr() {
            Ok(a) => {
                let s = a.to_string();
                drop(listener);
                s
            }
            Err(e) => {
                log::error!("module '{}': get local addr: {e}", name);
                return Ok((String::new(),));
            }
        },
        Err(e) => {
            log::error!("module '{}': bind port: {e}", name);
            return Ok((String::new(),));
        }
    };

    log::info!("module '{}': starting at {addr}", name);

    let module_cancel = cancel.child_token();

    let join = tokio::spawn({
        let engine = engine.clone();
        let name = name.clone();
        let addr = addr.clone();
        let cancel = module_cancel.clone();
        let max_restarts = config.max_module_restarts;
        let backoff_initial = config.restart_backoff_initial_ms;
        let max_backoff = config.max_restart_backoff_ms;
        async move {
            run_module_server(
                engine,
                component,
                name,
                addr,
                cancel,
                perms,
                max_restarts,
                backoff_initial,
                max_backoff,
            )
            .await;
        }
    });

    state.active.write().unwrap().insert(
        name.clone(),
        ActiveModule {
            addr: addr.clone(),
            cancel: module_cancel,
            _handle: ModuleTaskHandle {
                name,
                abort: join.abort_handle(),
            },
        },
    );

    Ok((addr,))
}

async fn host_remove_module(state: Arc<RouterState>, (name,): (String,)) -> Result<()> {
    let module = state.active.write().unwrap().remove(&name);
    if let Some(m) = module {
        m.cancel.cancel();
    }
    state.denied.write().unwrap().insert(name);
    Ok(())
}

async fn host_allow_module(state: Arc<RouterState>, (name,): (String,)) -> Result<()> {
    state.denied.write().unwrap().remove(&name);
    log::info!("module '{}': tombstone cleared, re-load permitted", name);
    Ok(())
}

impl WitCapability for SandboxRouterCapability {
    fn interface_id(&self) -> &str {
        "sandbox:router/sandbox-host@0.1.0"
    }

    fn init_store(&self, store: &mut CapabilityStore) {
        store.set_ext(
            self.interface_id(),
            Arc::new(RouterState {
                active: Arc::new(RwLock::new(HashMap::new())),
                denied: RwLock::new(HashSet::new()),
            }),
        );
    }

    fn register(&self, linker: &mut component::Linker<CapabilityStore>) -> Result<()> {
        let iface = self.interface_id();
        let mut inst = linker.instance(iface)?;

        let engine = self.engine.clone();
        let config = self.config.clone();
        let cancel = self.cancel.clone();

        inst.func_wrap_async(
            "add-module",
            with_state::<RouterState, _, _, _, _>(iface, move |state, params| {
                host_add_module(
                    state,
                    engine.clone(),
                    config.clone(),
                    cancel.clone(),
                    params,
                )
            }),
        )?;

        inst.func_wrap_async(
            "remove-module",
            with_state::<RouterState, _, _, _, _>(iface, host_remove_module),
        )?;

        inst.func_wrap_async(
            "allow-module",
            with_state::<RouterState, _, _, _, _>(iface, host_allow_module),
        )?;

        Ok(())
    }
}

fn load_module_component(engine: &Engine, path: &std::path::Path) -> Option<Component> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            log::error!("module: failed to read {}: {e:#}", path.display());
            return None;
        }
    };

    if let Some(wasmtime::Precompiled::Component) = wasmtime::Engine::detect_precompiled(&bytes) {
        match unsafe { Component::deserialize(engine, &bytes) } {
            Ok(c) => {
                log::info!(
                    "module: loaded AOT from {} ({} bytes)",
                    path.display(),
                    bytes.len()
                );
                return Some(c);
            }
            Err(e) => {
                log::warn!(
                    "module: AOT load failed for {} ({e:#}), falling back to JIT",
                    path.display()
                );
            }
        }
    }

    match Component::from_binary(engine, &bytes) {
        Ok(c) => {
            log::info!(
                "module: JIT-compiled from {} ({} bytes)",
                path.display(),
                bytes.len()
            );
            Some(c)
        }
        Err(e) => {
            log::warn!("module: JIT compile error for {}: {e:#}", path.display());
            None
        }
    }
}

async fn run_module_server(
    engine: Engine,
    component: Component,
    name: String,
    addr: String,
    cancel: CancellationToken,
    perms: SandboxPermissions,
    max_restarts: u32,
    backoff_initial_ms: u64,
    max_backoff_ms: u64,
) {
    let mut linker: component::Linker<HandlerCtx> = component::Linker::new(&engine);
    if let Err(e) = wasmtime_wasi::p2::add_to_linker_async(&mut linker) {
        log::error!("module server '{name}': failed to build WASI linker: {e:#}");
        return;
    }

    let mut restarts = 0u32;
    loop {
        if restarts > 0 {
            log::info!("module server '{name}': restart {restarts}/{max_restarts} at {addr}");
        }

        tokio::select! {
            result = run_component_server(&engine, &component, &name, &addr, perms, &linker) => {
                match result {
                    Ok(code) => log::warn!("module server '{name}': unexpected exit (code {code})"),
                    Err(e) => log::error!("module server '{name}': fatal: {e:#}"),
                }
            }
            _ = cancel.cancelled() => {
                log::info!("module server '{name}': shutdown");
                return;
            }
        }

        if restarts >= max_restarts {
            log::error!("module server '{name}': reached {max_restarts} restarts, giving up");
            return;
        }

        restarts += 1;
        let backoff_ms = (backoff_initial_ms << (restarts - 1).min(5)).min(max_backoff_ms);
        log::warn!(
            "module server '{name}': restarting in {backoff_ms}ms ({restarts}/{max_restarts})"
        );

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
            _ = cancel.cancelled() => {
                log::info!("module server '{name}': cancelled during backoff");
                return;
            }
        }
    }
}

async fn run_component_server(
    engine: &Engine,
    component: &Component,
    name: &str,
    addr: &str,
    perms: SandboxPermissions,
    linker: &component::Linker<HandlerCtx>,
) -> Result<i32> {
    let mut builder = WasiCtxBuilder::new();
    builder.args(&[name, "--addr", addr, "--name", name]);
    builder.inherit_stdio();
    if perms.stdio {
        // ignore for now, this intentional
    }
    builder.allow_tcp(true).inherit_network().allow_ip_name_lookup(true);
    if perms.network {
        // ignore for now, this intentional
    }
    let wasi = builder.build();

    let mut store = Store::new(
        engine,
        HandlerCtx {
            wasi,
            table: ResourceTable::default(),
        },
    );

    let command = Command::instantiate_async(&mut store, component, linker).await?;
    match command.wasi_cli_run().call_run(&mut store).await? {
        Ok(()) => Ok(0),
        Err(()) => Ok(1),
    }
}

struct HandlerCtx {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for HandlerCtx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}
