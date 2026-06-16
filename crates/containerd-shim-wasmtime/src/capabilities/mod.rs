use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use containerd_shim_wasm::sandbox::context::RuntimeContext;
use sandbox_router::{SandboxRouterCapability, SandboxRouterConfig};
use tokio_util::sync::CancellationToken;
use wasmtime::Engine;
use wasmtime::component::{self, Component, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpView};

pub(crate) mod sandbox_router;

pub(crate) struct CapabilityStore {
    pub wasi: WasiCtx,
    pub wasi_http: WasiHttpCtx,
    pub resource_table: ResourceTable,
    extensions: HashMap<String, Arc<dyn Any + Send + Sync>>,
}

impl CapabilityStore {
    pub fn new(wasi: WasiCtx) -> Self {
        Self {
            wasi,
            wasi_http: WasiHttpCtx::new(),
            resource_table: ResourceTable::default(),
            extensions: HashMap::new(),
        }
    }

    pub fn with_extensions(wasi: WasiCtx, extensions: HashMap<String, Arc<dyn Any + Send + Sync>>) -> Self {
        Self {
            wasi,
            wasi_http: WasiHttpCtx::new(),
            resource_table: ResourceTable::default(),
            extensions,
        }
    }

    pub fn into_extensions(self) -> HashMap<String, Arc<dyn Any + Send + Sync>> {
        self.extensions
    }

    pub fn set_ext<T: Any + Send + Sync>(&mut self, iface: &str, val: Arc<T>) {
        self.extensions.insert(iface.to_string(), val);
    }

    pub fn get_ext<T: Any + Send + Sync>(&self, iface: &str) -> Option<Arc<T>> {
        self.extensions.get(iface)?.clone().downcast::<T>().ok()
    }
}

impl WasiView for CapabilityStore {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.resource_table,
        }
    }
}

impl WasiHttpView for CapabilityStore {
    fn ctx(&mut self) -> &mut WasiHttpCtx {
        &mut self.wasi_http
    }

    fn table(&mut self) -> &mut ResourceTable {
        &mut self.resource_table
    }
}

/// Adapts a plain `async fn(Arc<S>, Params) -> Result<Return>` into a wasmtime host function.
pub(crate) fn with_state<S, Params, Return, F, Fut>(
    iface: &str,
    f: F,
) -> impl for<'a> Fn(
    wasmtime::StoreContextMut<'a, CapabilityStore>,
    Params,
) -> Box<dyn Future<Output = anyhow::Result<Return>> + Send + 'a>
+ Send
+ Sync
+ 'static
where
    S: Any + Send + Sync + 'static,
    Params: Send + 'static,
    Return: Send + 'static,
    F: Fn(Arc<S>, Params) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<Return>> + Send + 'static,
{
    let iface = iface.to_string();
    move |ctx, params| {
        let state = ctx.data().get_ext::<S>(&iface);
        let iface = iface.clone();
        let f = f.clone();
        Box::new(async move {
            let state = state.ok_or_else(|| {
                anyhow::anyhow!("capability '{iface}' not initialized in store")
            })?;
            f(state, params).await
        })
    }
}

/// A single WIT interface capability that can be registered into the linker
/// and initialized per-execution in the store.
pub(crate) trait WitCapability: Send + Sync {
    /// Full WIT interface id, e.g. "sandbox:router/sandbox-host@0.1.0"
    fn interface_id(&self) -> &str;

    /// Called once per engine to register host functions in the linker.
    fn register(&self, linker: &mut component::Linker<CapabilityStore>) -> Result<()>;

    /// Called once per execution to install initial state into the store.
    fn init_store(&self, store: &mut CapabilityStore);
}

pub(crate) struct CapabilityRegistry {
    caps: Vec<Arc<dyn WitCapability>>,
}

impl CapabilityRegistry {
    pub fn new(caps: Vec<Arc<dyn WitCapability>>) -> Self {
        Self { caps }
    }

    /// Build the registry from the runtime context.
    /// Reads `WASMTIME_CAPABILITIES` env var, comma-separated interface IDs
    pub fn from_ctx(
        ctx: &impl RuntimeContext,
        engine: &Engine,
        cancel: &CancellationToken,
    ) -> Self {
        let enabled: HashSet<String> = ctx
            .envs()
            .iter()
            .find(|e| e.starts_with("WASMTIME_CAPABILITIES="))
            .map(|e| {
                e["WASMTIME_CAPABILITIES=".len()..]
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();

        // All built-in capabilities, add new ones here, no other change needed.
        let all: Vec<Arc<dyn WitCapability>> = vec![Arc::new(SandboxRouterCapability {
            engine: engine.clone(),
            config: Arc::new(SandboxRouterConfig::default()),
            cancel: cancel.clone(),
        })];

        // TODO: switch to deny-by-default once all callers set WASMTIME_CAPABILITIES.
        // Currently allow-by-default: enable all capabilities unless WASMTIME_CAPABILITIES is set,
        // in which case restrict to the listed interfaces.
        let caps = if enabled.is_empty() {
            all
        } else {
            all.into_iter()
                .filter(|c| enabled.contains(c.interface_id()))
                .collect()
        };

        Self::new(caps)
    }

    /// Register all capabilities into the linker (call once per engine).
    pub fn register_linker(&self, linker: &mut component::Linker<CapabilityStore>) -> Result<()> {
        for cap in &self.caps {
            cap.register(linker)?;
        }
        Ok(())
    }

    /// Introspect component imports; init store state only for what the component needs.
    pub fn init_for_component(
        &self,
        component: &Component,
        engine: &Engine,
        store: &mut CapabilityStore,
    ) -> Result<()> {
        for (iface, _item) in component.component_type().imports(engine) {
            // WASI built-ins are handled by wasmtime_wasi::p2::add_to_linker_async
            if iface.starts_with("wasi:") {
                continue;
            }
            match self.caps.iter().find(|c| c.interface_id() == iface) {
                Some(cap) => cap.init_store(store),
                None => anyhow::bail!(
                    "component imports interface '{iface}' which is not registered or not enabled (set WASMTIME_CAPABILITIES)"
                ),
            }
        }
        Ok(())
    }
}
