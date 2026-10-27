//! The native realm a launch's members read their configuration from, and the
//! auth binding every member it constructs builds with.
//!
//! `runtime_options.native_realm = {realm_id, context_root, state_root,
//! user_config_root}` names a realm a native host keeps. The gateway reads
//! that realm's configuration with the calls a native host makes: Core's
//! [`meerkat_core::StorageLayout`] resolves the realm at the explicit state
//! root, the store's [`meerkat_store::FilesystemRealmConfigSource`] maps the
//! realm and its chain to their documents, Core's
//! [`meerkat_core::EffectiveConfigReader`] composes them, and the result takes
//! Core's environment overrides and validation. The composed configuration is
//! the members' whole configuration, in place of
//! `runtime_options.meerkat_config_path` and never beside it. Nothing is
//! copied or written, and no path is derived beyond what Core derives from
//! the four inputs.
//!
//! `runtime_options.member_auth_binding = {realm, binding, profile?,
//! origin?}` is Core's [`meerkat_core::AuthBindingRef`]; `origin`, when
//! present, must be `"configured"` ([`meerkat_core::BindingOrigin::Configured`]).
//! At init the gateway resolves it in the members' configuration with Core's
//! explicit resolution, refuses a definition profile whose model or provider
//! the binding does not serve, and refuses every path that would make
//! provider calls off the binding: an automatic spawn policy, a model
//! fallback target that names another binding or none, image generation and
//! the agent memory engines. It then installs [`MemberAuthBindingCustomizer`],
//! which forces the binding onto every member spawn Core asks the gateway's
//! spawn customizers about: fresh spawns, helpers, forks, respawns and
//! successors, and the rebuild of a member on its durable session, where an
//! explicit binding takes the place of the durable one. A spawn that names
//! another binding is refused, and an agent meets each refusal typed
//! ([`agent_tool_binding_refusal`]).
//!
//! Both are settled once, before any agent factory is built. A running
//! gateway never rebinds: a changed realm document or binding takes effect
//! in a new launch.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `runtime_options` key of the native realm.
const NATIVE_REALM_KEY: &str = "native_realm";
/// `runtime_options` key of the members' auth binding.
const MEMBER_AUTH_BINDING_KEY: &str = "member_auth_binding";
/// `runtime_options` key of the single host config file, which a native realm
/// replaces.
const MEERKAT_CONFIG_PATH_KEY: &str = "meerkat_config_path";
/// The one value `member_auth_binding.origin` may carry.
const CONFIGURED_ORIGIN: &str = "configured";
/// JSON-RPC invalid params.
const INVALID_PARAMS: i64 = -32602;
/// The kind an agent's mob tool answers when a spawn names another auth
/// binding than the launch's (a [`meerkat_core::ToolConsequenceDenial`]
/// code), and the prefix of the wiring error a host spawn meets.
pub(crate) const BINDING_CONFLICT_KIND: &str = "member_auth_binding_conflict";
/// The kind an agent's mob tool answers when a child mob it creates would
/// build members off the launch's auth binding.
pub(crate) const BINDING_UNCARRIED_KIND: &str = MemberRouteRefusalKind::Uncarried.as_str();

/// What a member route refusal is. Its [`Self::as_str`] is the refusal's
/// `error.data.kind` on `meta/init`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemberRouteRefusalKind {
    /// `native_realm` beside `meerkat_config_path`.
    ConfigConflict,
    /// `native_realm` without `member_auth_binding`.
    BindingRequired,
    /// `native_realm` is not a closed object of a realm id and three
    /// absolute roots.
    RealmInvalid,
    /// The state root holds no realm of that id.
    RealmUnknown,
    /// The realm's storage cannot be resolved at the state root.
    RealmUnreadable,
    /// The realm's documents cannot be read, parsed or composed, or the
    /// composed configuration does not validate.
    ConfigInvalid,
    /// `member_auth_binding` is not a closed object of valid ids.
    BindingInvalid,
    /// `member_auth_binding.origin` other than `"configured"`.
    OriginNotConfigured,
    /// The binding does not resolve in the members' configuration.
    BindingUnknown,
    /// A definition profile's model or provider is not one the binding
    /// serves.
    TargetMismatch,
    /// A path that would build a member without the binding: an automatic
    /// spawn policy, a model fallback target that names another binding or
    /// none, or an identity declaration Core materializes on its own.
    Uncarried,
}

impl MemberRouteRefusalKind {
    /// The refusal's `error.data.kind`.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigConflict => "native_realm_config_conflict",
            Self::BindingRequired => "member_auth_binding_required",
            Self::RealmInvalid => "native_realm_invalid",
            Self::RealmUnknown => "native_realm_unknown",
            Self::RealmUnreadable => "native_realm_unreadable",
            Self::ConfigInvalid => "native_realm_config_invalid",
            Self::BindingInvalid => "member_auth_binding_invalid",
            Self::OriginNotConfigured => "member_auth_binding_origin",
            Self::BindingUnknown => "member_auth_binding_unknown",
            Self::TargetMismatch => "member_auth_binding_mismatch",
            Self::Uncarried => "member_auth_binding_uncarried",
        }
    }

    /// The JSON-RPC code: the storage resolution code for a realm the state
    /// root does not resolve, invalid params for every other refusal (a
    /// realm document that does not read or validate included, as for the
    /// `meerkat_config_path` file).
    pub(crate) const fn code(self) -> i64 {
        match self {
            Self::RealmUnknown | Self::RealmUnreadable => crate::STORAGE_RESOLUTION_CODE,
            Self::ConfigConflict
            | Self::BindingRequired
            | Self::RealmInvalid
            | Self::ConfigInvalid
            | Self::BindingInvalid
            | Self::OriginNotConfigured
            | Self::BindingUnknown
            | Self::TargetMismatch
            | Self::Uncarried => INVALID_PARAMS,
        }
    }
}

/// A typed `meta/init` refusal of the members' native realm or auth binding:
/// its kind, the message, and `error.data` (`kind` plus the refusal's facts).
/// The gateway refuses init with it before any agent factory is built, and
/// never falls back to the default configuration or the default binding.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemberRouteRefusal {
    kind: MemberRouteRefusalKind,
    message: String,
    data: Value,
}

impl MemberRouteRefusal {
    fn new(kind: MemberRouteRefusalKind, message: String, facts: Map<String, Value>) -> Self {
        let mut data = facts;
        data.insert("kind".to_string(), Value::from(kind.as_str()));
        Self {
            kind,
            message,
            data: Value::Object(data),
        }
    }

    fn with_reason(kind: MemberRouteRefusalKind, message: String, reason: String) -> Self {
        let mut facts = Map::new();
        facts.insert("reason".to_string(), Value::from(reason));
        Self::new(kind, message, facts)
    }

    fn config_conflict() -> Self {
        Self::new(
            MemberRouteRefusalKind::ConfigConflict,
            format!(
                "runtime_options.{NATIVE_REALM_KEY} and runtime_options.{MEERKAT_CONFIG_PATH_KEY} \
                 are exclusive: the members' configuration is the native realm's or the named \
                 file's, never a merge of both. Remove one of them"
            ),
            Map::new(),
        )
    }

    fn binding_required() -> Self {
        Self::new(
            MemberRouteRefusalKind::BindingRequired,
            format!(
                "runtime_options.{NATIVE_REALM_KEY} needs runtime_options.\
                 {MEMBER_AUTH_BINDING_KEY}: a member's default binding selection starts at its \
                 own mob realm and never reaches the native realm's bindings, so name the \
                 binding the members build with"
            ),
            Map::new(),
        )
    }

    fn realm_invalid(reason: String) -> Self {
        Self::with_reason(
            MemberRouteRefusalKind::RealmInvalid,
            format!("runtime_options.{NATIVE_REALM_KEY} is invalid: {reason}"),
            reason,
        )
    }

    fn realm_unknown(realm_id: &str, state_root: &Path) -> Self {
        let mut facts = Map::new();
        facts.insert("realm_id".to_string(), Value::from(realm_id));
        facts.insert(
            "state_root".to_string(),
            Value::from(state_root.display().to_string()),
        );
        Self::new(
            MemberRouteRefusalKind::RealmUnknown,
            format!(
                "runtime_options.{NATIVE_REALM_KEY} names realm '{realm_id}', which does not \
                 exist under the state root {}",
                state_root.display()
            ),
            facts,
        )
    }

    fn realm_unreadable(realm_id: &str, reason: String) -> Self {
        let mut facts = Map::new();
        facts.insert("realm_id".to_string(), Value::from(realm_id));
        facts.insert("reason".to_string(), Value::from(reason.as_str()));
        Self::new(
            MemberRouteRefusalKind::RealmUnreadable,
            format!(
                "runtime_options.{NATIVE_REALM_KEY} realm '{realm_id}' cannot be resolved at its \
                 state root: {reason}"
            ),
            facts,
        )
    }

    fn config_invalid(realm_id: &str, reason: String) -> Self {
        let mut facts = Map::new();
        facts.insert("realm_id".to_string(), Value::from(realm_id));
        facts.insert("reason".to_string(), Value::from(reason.as_str()));
        Self::new(
            MemberRouteRefusalKind::ConfigInvalid,
            format!(
                "runtime_options.{NATIVE_REALM_KEY} realm '{realm_id}' has no usable \
                 configuration: {reason}"
            ),
            facts,
        )
    }

    fn binding_invalid(reason: String) -> Self {
        Self::with_reason(
            MemberRouteRefusalKind::BindingInvalid,
            format!("runtime_options.{MEMBER_AUTH_BINDING_KEY} is invalid: {reason}"),
            reason,
        )
    }

    fn origin_not_configured(origin: &str) -> Self {
        let mut facts = Map::new();
        facts.insert("origin".to_string(), Value::from(origin));
        Self::new(
            MemberRouteRefusalKind::OriginNotConfigured,
            format!(
                "runtime_options.{MEMBER_AUTH_BINDING_KEY}.origin is '{origin}', and only \
                 '{CONFIGURED_ORIGIN}' is accepted: the members build with a configured binding \
                 of their configuration"
            ),
            facts,
        )
    }

    fn binding_unknown(binding: &meerkat_core::AuthBindingRef, reason: String) -> Self {
        let mut facts = binding_facts(binding);
        facts.insert("reason".to_string(), Value::from(reason.as_str()));
        Self::new(
            MemberRouteRefusalKind::BindingUnknown,
            format!(
                "runtime_options.{MEMBER_AUTH_BINDING_KEY} realm '{}' binding '{}' does not \
                 resolve in the members' configuration: {reason}",
                binding.realm, binding.binding
            ),
            facts,
        )
    }

    fn target_mismatch(
        binding: &meerkat_core::AuthBindingRef,
        binding_provider: meerkat_core::Provider,
        profile: Option<(&str, &str)>,
        reason: String,
    ) -> Self {
        let mut facts = binding_facts(binding);
        facts.insert(
            "binding_provider".to_string(),
            Value::from(binding_provider.as_str()),
        );
        if let Some((name, model)) = profile {
            facts.insert("mob_profile".to_string(), Value::from(name));
            facts.insert("model".to_string(), Value::from(model));
        }
        facts.insert("reason".to_string(), Value::from(reason.as_str()));
        let subject = profile.map_or_else(String::new, |(name, model)| {
            format!(" for profile '{name}' (model '{model}')")
        });
        Self::new(
            MemberRouteRefusalKind::TargetMismatch,
            format!(
                "runtime_options.{MEMBER_AUTH_BINDING_KEY} realm '{}' binding '{}' (provider \
                 '{}') does not match{subject}: {reason}",
                binding.realm,
                binding.binding,
                binding_provider.as_str()
            ),
            facts,
        )
    }

    fn uncarried(reason: String) -> Self {
        Self::with_reason(
            MemberRouteRefusalKind::Uncarried,
            format!(
                "runtime_options.{MEMBER_AUTH_BINDING_KEY} cannot reach every member this \
                 launch builds: {reason}"
            ),
            reason,
        )
    }

    /// The refusal of a member declaration mutation
    /// (`mob/adopt_member_identity_declaration`,
    /// `mob/apply_member_tool_declaration`) on a launch with a member auth
    /// binding: Core materializes a declared identity from its sealed
    /// declaration without the spawn customizers, so the identity would
    /// build with the binding the declaration sealed, or with none, whatever
    /// binding the launch names.
    pub(crate) fn declaration_uncarried(method: &str) -> Self {
        Self::uncarried(format!(
            "{method} has Meerkat materialize the identity from its sealed declaration \
             without the gateway's spawn customizers, so it would not build with this \
             launch's binding; use a launch without runtime_options.member_auth_binding"
        ))
    }

    /// The refusal of agent memory engines (`agent_memory.distiller`,
    /// `agent_memory.steward`) on a launch with a member auth binding: they
    /// build their model clients from the default configuration with no
    /// binding, never from the members' configuration and binding.
    pub(crate) fn memory_engines_uncarried() -> Self {
        Self::uncarried(
            "agent_memory.distiller and agent_memory.steward build their model clients from \
             the default configuration with Meerkat's default binding selection, not from \
             the members' configuration and binding; disable them or the binding"
                .to_string(),
        )
    }

    pub(crate) fn code(&self) -> i64 {
        self.kind.code()
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// `error.data`: `kind` plus the refusal's facts.
    pub(crate) fn data(&self) -> &Value {
        &self.data
    }
}

impl std::fmt::Display for MemberRouteRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MemberRouteRefusal {}

/// The binding's own facts in a refusal's data: `realm`, `binding` and, when
/// the binding names one, its auth profile as `auth_profile`.
fn binding_facts(binding: &meerkat_core::AuthBindingRef) -> Map<String, Value> {
    let mut facts = Map::new();
    facts.insert("realm".to_string(), Value::from(binding.realm.as_str()));
    facts.insert("binding".to_string(), Value::from(binding.binding.as_str()));
    if let Some(profile) = binding.profile.as_ref() {
        facts.insert("auth_profile".to_string(), Value::from(profile.as_str()));
    }
    facts
}

/// A binding as a refusal message names it.
fn describe_binding(binding: &meerkat_core::AuthBindingRef) -> String {
    let profile = binding
        .profile
        .as_ref()
        .map_or_else(String::new, |profile| format!(" profile '{profile}'"));
    format!(
        "realm '{}' binding '{}'{profile}",
        binding.realm, binding.binding
    )
}

/// Why a spawn that names `named` may not build under `launch`: the launch
/// binding is forced, so any other binding (another realm, binding or auth
/// profile) is refused, and the identical one is accepted.
pub(crate) fn binding_conflict(
    named: &meerkat_core::AuthBindingRef,
    launch: &meerkat_core::AuthBindingRef,
) -> Option<String> {
    (named != launch).then(|| {
        format!(
            "{BINDING_CONFLICT_KIND}: the spawn names auth binding {}, and every member of \
             this launch builds with {} (runtime_options.member_auth_binding)",
            describe_binding(named),
            describe_binding(launch)
        )
    })
}

/// Why enabled image generation would build off the launch binding, if it is
/// enabled: Core builds an image generation executor for each image provider
/// other than the member's own by default selection in the member's realm,
/// never with the member's binding.
fn image_generation_escape(enabled: bool) -> Option<String> {
    enabled.then(|| {
        "tools.image_generation: Meerkat builds an image generation executor for each image \
         provider other than the member's own by default binding selection, never with the \
         launch binding; disable image generation"
            .to_string()
    })
}

/// Why a member profile would make provider calls off the launch binding,
/// if it would: an enabled fallback target off the binding, or enabled
/// image generation.
fn profile_escape(
    profile: &meerkat_mob::Profile,
    binding: &meerkat_core::AuthBindingRef,
) -> Option<String> {
    fallback_escape(profile.model_fallback.as_ref(), binding)
        .map(|reason| format!("model_fallback: {reason}"))
        .or_else(|| image_generation_escape(profile.tools.image_generation))
}

/// Why an enabled model fallback `table` would build a member off `binding`,
/// if it would: its first target that names another binding, or none, which
/// Core resolves by default selection and never to the launch binding. A
/// table that is absent or not enabled builds nothing.
fn fallback_escape(
    table: Option<&meerkat_core::config::ModelFallbackConfig>,
    binding: &meerkat_core::AuthBindingRef,
) -> Option<String> {
    let table = table.filter(|table| table.is_enabled())?;
    let target = table
        .chain
        .iter()
        .find(|target| target.auth_binding.as_ref() != Some(binding))?;
    Some(match target.auth_binding.as_ref() {
        None => format!(
            "fallback target '{}' names no auth_binding, so Meerkat would select a default \
             binding for it",
            target.model
        ),
        Some(other) => format!(
            "fallback target '{}' names {}, not {}",
            target.model,
            describe_binding(other),
            describe_binding(binding)
        ),
    })
}

/// `runtime_options.native_realm`: the native realm whose effective
/// configuration the launch's members build from. Every field is required,
/// and no other is accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeRealmSelection {
    /// The realm's id: its directory name under `state_root`, by Core's realm
    /// id rule.
    pub(crate) realm_id: String,
    /// The native host's context root: Core's invocation context, never
    /// walked for the realm.
    pub(crate) context_root: PathBuf,
    /// The realms root the realm lives under, used as given.
    pub(crate) state_root: PathBuf,
    /// The home-like root whose `.rkat/config.toml` is the `global` realm's
    /// document.
    pub(crate) user_config_root: PathBuf,
}

impl NativeRealmSelection {
    /// Parse the option's value, refused as `native_realm_invalid`.
    pub(crate) fn parse(value: &Value) -> Result<Self, MemberRouteRefusal> {
        let selection: Self = serde_json::from_value(value.clone()).map_err(|error| {
            MemberRouteRefusal::realm_invalid(format!(
                "it must be an object {{realm_id, context_root, state_root, user_config_root}}: \
                 {error}"
            ))
        })?;
        meerkat_core::runtime_bootstrap::validate_explicit_realm_id(&selection.realm_id)
            .map_err(|error| MemberRouteRefusal::realm_invalid(format!("realm_id: {error}")))?;
        for (field, root) in [
            ("context_root", &selection.context_root),
            ("state_root", &selection.state_root),
            ("user_config_root", &selection.user_config_root),
        ] {
            if !root.is_absolute() {
                return Err(MemberRouteRefusal::realm_invalid(format!(
                    "{field} must be an absolute path, got '{}'",
                    root.display()
                )));
            }
        }
        Ok(selection)
    }
}

/// The wire form of `runtime_options.member_auth_binding`: Core's
/// `AuthBindingRef` as a closed object. The ids are Core's typed atoms, so
/// serde applies Core's slug rule to each.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberAuthBindingWire {
    realm: meerkat_core::RealmId,
    binding: meerkat_core::BindingId,
    #[serde(default)]
    profile: Option<meerkat_core::ProfileId>,
    #[serde(default)]
    origin: Option<String>,
}

/// Parse `runtime_options.member_auth_binding` into Core's typed binding.
/// `origin` may be absent or `"configured"`, which the binding carries as
/// [`meerkat_core::BindingOrigin::Configured`]; any other value is refused,
/// never dropped and never read as another origin.
pub(crate) fn parse_member_auth_binding(
    value: &Value,
) -> Result<meerkat_core::AuthBindingRef, MemberRouteRefusal> {
    let wire: MemberAuthBindingWire = serde_json::from_value(value.clone()).map_err(|error| {
        MemberRouteRefusal::binding_invalid(format!(
            "it must be an object {{realm, binding, profile?, origin?}}: {error}"
        ))
    })?;
    if let Some(origin) = wire.origin.as_deref()
        && origin != CONFIGURED_ORIGIN
    {
        return Err(MemberRouteRefusal::origin_not_configured(origin));
    }
    Ok(meerkat_core::AuthBindingRef {
        realm: wire.realm,
        binding: wire.binding,
        profile: wire.profile,
        origin: meerkat_core::BindingOrigin::Configured,
    })
}

/// A launch's member route: the native realm its members read their
/// configuration from and the binding they build with, as `meta/init`
/// declared them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MemberRoute {
    pub(crate) native_realm: Option<NativeRealmSelection>,
    pub(crate) member_auth_binding: Option<meerkat_core::AuthBindingRef>,
}

impl MemberRoute {
    /// Parse both options from the init params. A `runtime_options` that is
    /// not an object is left to the general option parse, which refuses it.
    pub(crate) fn from_init_params(params: &Value) -> Result<Self, MemberRouteRefusal> {
        match params.get("runtime_options").and_then(Value::as_object) {
            Some(options) => Self::from_runtime_options(options),
            None => Ok(Self::default()),
        }
    }

    /// Parse both options and check them against each other and against
    /// `meerkat_config_path`.
    pub(crate) fn from_runtime_options(
        options: &Map<String, Value>,
    ) -> Result<Self, MemberRouteRefusal> {
        if options.contains_key(NATIVE_REALM_KEY) && options.contains_key(MEERKAT_CONFIG_PATH_KEY) {
            return Err(MemberRouteRefusal::config_conflict());
        }
        let native_realm = options
            .get(NATIVE_REALM_KEY)
            .map(NativeRealmSelection::parse)
            .transpose()?;
        let member_auth_binding = options
            .get(MEMBER_AUTH_BINDING_KEY)
            .map(parse_member_auth_binding)
            .transpose()?;
        if native_realm.is_some() && member_auth_binding.is_none() {
            return Err(MemberRouteRefusal::binding_required());
        }
        Ok(Self {
            native_realm,
            member_auth_binding,
        })
    }
}

/// Read the native realm's effective configuration with the calls a native
/// host makes, read only: Core's storage layout resolves the realm at the
/// explicit state root (which wins over every probe, so `context_root`
/// selects no document), Core's own realm probe checks that the realm exists
/// there (the reserved `global` realm keeps no manifest: its document is the
/// user document), the store's filesystem source maps the realm and its
/// chain to their documents, Core's effective reader composes them, and the
/// result takes Core's environment overrides and `Config::validate`, as a
/// native host's does. A realm the state root cannot resolve is a storage
/// refusal; documents that do not read, compose or validate are refused as
/// invalid params, as a `meerkat_config_path` file is.
pub(crate) async fn read_native_realm_config(
    selection: &NativeRealmSelection,
) -> Result<meerkat_core::Config, MemberRouteRefusal> {
    let unreadable =
        |reason: String| MemberRouteRefusal::realm_unreadable(&selection.realm_id, reason);
    let invalid = |reason: String| MemberRouteRefusal::config_invalid(&selection.realm_id, reason);
    let storage = meerkat_core::StorageLayout::resolve(
        meerkat_core::StorageLayoutInputs {
            invocation_context: selection.context_root.clone(),
            explicit_state_root: Some(selection.state_root.clone()),
            user_config_root: Some(selection.user_config_root.clone()),
            default_root: Some(meerkat_core::RealmRootDefault::UserGlobal),
            probe_local_candidate: true,
        },
        &meerkat_core::RealmConfig {
            selection: meerkat_core::RealmSelection::Explicit {
                realm_id: selection.realm_id.clone(),
            },
            ..meerkat_core::RealmConfig::default()
        },
    )
    .map_err(|error| unreadable(error.to_string()))?;
    let realm = &storage.locator.realm;
    let state_root = &storage.locator.state_root;
    if !realm.is_global() {
        match meerkat_core::realm_exists_under(state_root, realm) {
            Ok(true) => {}
            Ok(false) => {
                return Err(MemberRouteRefusal::realm_unknown(
                    &selection.realm_id,
                    state_root,
                ));
            }
            Err(error) => return Err(unreadable(error.to_string())),
        }
    }
    let user_document = storage.layout.global_config_path().ok_or_else(|| {
        unreadable("the storage layout resolved no user configuration root".to_string())
    })?;
    let source = Arc::new(meerkat_store::FilesystemRealmConfigSource::new(
        state_root,
        user_document,
        meerkat_models::canonical(),
    ));
    let mut config = meerkat_core::EffectiveConfigReader::new(source)
        .effective_config(realm)
        .await
        .map_err(|error| invalid(error.to_string()))?;
    config
        .apply_env_overrides()
        .map_err(|error| invalid(error.to_string()))?;
    config
        .validate(meerkat_models::canonical())
        .map_err(|error| invalid(error.to_string()))?;
    Ok(config)
}

/// Check the members' binding at init, against the members' configuration
/// and the mob definition, so a refusal answers before any factory is built:
///
/// - the binding resolves in `config` by Core's explicit resolution: its
///   realm is in the configuration and the binding is defined on that
///   realm's chain;
/// - every inline profile of `definition` resolves to the provider the
///   binding's backend serves (the profile's own provider, else the one the
///   members' model registry, with the definition's `[models.<id>]`, owns
///   the model under) and the model is not registered under another
///   provider; under a self-hosted binding the model is registered with a
///   server, as Core's build requires, and that server is the one the
///   binding's backend declares. A realm-referenced profile is resolved when
///   its member is built, where Core refuses a binding that does not serve
///   it;
/// - no path builds a member off the binding: the definition declares no
///   automatic spawn policy, whose members Core builds without the gateway's
///   spawn customizers, and no enabled model fallback table (the members'
///   configuration, the definition's `[runtime.model_fallback]`, an inline
///   profile's `model_fallback`) has a target that names another binding or
///   none, which Core would resolve by default selection;
/// - no inline profile enables image generation, whose executors for the
///   image providers other than the member's own Core builds by default
///   selection.
pub(crate) fn validate_member_auth_binding(
    config: &meerkat_core::Config,
    binding: &meerkat_core::AuthBindingRef,
    definition: &meerkat_mob::MobDefinition,
) -> Result<(), MemberRouteRefusal> {
    let target = meerkat_core::resolve_explicit_auth_binding_target(config, binding)
        .map_err(|error| MemberRouteRefusal::binding_unknown(binding, error.to_string()))?;
    let binding_provider = target.backend.provider;
    let registry = meerkat_core::ModelRegistry::from_config_with_models(
        config,
        &definition.models,
        meerkat_models::canonical(),
    )
    .map_err(|error| {
        MemberRouteRefusal::target_mismatch(
            binding,
            binding_provider,
            None,
            format!("the members' model registry cannot be built: {error}"),
        )
    })?;
    for (name, profile) in &definition.profiles {
        let Some(profile) = profile.as_inline() else {
            continue;
        };
        let model = profile.model.as_str();
        let subject = Some((name.as_str(), model));
        let mismatch = |reason: String| {
            MemberRouteRefusal::target_mismatch(binding, binding_provider, subject, reason)
        };
        let profile_provider = profile
            .provider
            .or_else(|| registry.entry(model).map(|entry| entry.provider));
        match profile_provider {
            Some(provider) if provider == binding_provider => {}
            Some(provider) => {
                return Err(mismatch(format!(
                    "the profile's provider is '{}'",
                    provider.as_str()
                )));
            }
            None => {
                return Err(mismatch(
                    "the profile names no provider and the members' model registry does not \
                     know the model"
                        .to_string(),
                ));
            }
        }
        if let Some(reason) = registry.provider_override_mismatch_reason(binding_provider, model) {
            return Err(mismatch(reason));
        }
        if binding_provider == meerkat_core::Provider::SelfHosted {
            let Some(registered_server) = registry
                .entry_for_provider(meerkat_core::Provider::SelfHosted, model)
                .and_then(|entry| entry.self_hosted.as_ref())
                .map(|server| server.server_id.clone())
            else {
                return Err(mismatch(format!(
                    "self-hosted model '{model}' is not registered with a server in the \
                     members' configuration"
                )));
            };
            let server = profile
                .self_hosted_server_id
                .clone()
                .unwrap_or(registered_server);
            meerkat_core::validate_explicit_self_hosted_target(config, &target, &server, model)
                .map_err(|error| mismatch(error.to_string()))?;
        }
        if let Some(reason) = image_generation_escape(profile.tools.image_generation) {
            return Err(MemberRouteRefusal::uncarried(format!(
                "profiles.{name}.{reason}"
            )));
        }
    }
    if matches!(
        definition.spawn_policy,
        Some(meerkat_mob::definition::SpawnPolicyConfig::Auto { .. })
    ) {
        return Err(MemberRouteRefusal::uncarried(
            "the definition's automatic spawn policy creates members that Core builds without \
             the gateway's spawn customizers, so they would not carry the binding; remove \
             [spawn_policy] or the binding"
                .to_string(),
        ));
    }
    let fallback_tables = [
        (
            "the members' configuration [model_fallback]".to_string(),
            Some(&config.model_fallback),
        ),
        (
            "[runtime.model_fallback]".to_string(),
            definition.runtime.model_fallback.as_ref(),
        ),
    ]
    .into_iter()
    .chain(definition.profiles.iter().filter_map(|(name, profile)| {
        profile.as_inline().map(|profile| {
            (
                format!("profiles.{name}.model_fallback"),
                profile.model_fallback.as_ref(),
            )
        })
    }));
    for (place, table) in fallback_tables {
        if let Some(reason) = fallback_escape(table, binding) {
            return Err(MemberRouteRefusal::uncarried(format!(
                "{place}: {reason}; name {} on every fallback target, or disable the fallback",
                describe_binding(binding)
            )));
        }
    }
    Ok(())
}

/// Why a public profile input (an inline profile in an agent's `mob_create`
/// definition or `tooling` override) would make provider calls off the
/// launch binding, if it would: an enabled fallback target off the binding,
/// or enabled image generation.
fn profile_input_escape(
    profile: &meerkat_contracts::wire::MobProfileInput,
    binding: &meerkat_core::AuthBindingRef,
) -> Option<String> {
    fallback_escape(profile.model_fallback.as_ref(), binding)
        .map(|reason| format!("model_fallback: {reason}"))
        .or_else(|| image_generation_escape(profile.tools.image_generation))
}

/// The typed refusal an agent's mob tool answers under a member auth
/// binding, before the tool runs: `(kind, message)` for a
/// [`meerkat_core::ToolConsequenceDenial`]. `mob_spawn_member` naming
/// another `auth_binding` is [`BINDING_CONFLICT_KIND`]; an inline `tooling`
/// profile (`mob_spawn_member`, `delegate`) or a `mob_create` child
/// definition that would make provider calls off the binding is
/// [`BINDING_UNCARRIED_KIND`]. Arguments that do not decode are left to the
/// tool, which refuses them the same way.
pub(crate) fn agent_tool_binding_refusal(
    tool: &str,
    args: &Value,
    binding: &meerkat_core::AuthBindingRef,
) -> Option<(&'static str, String)> {
    match tool {
        "mob_create" => args
            .get("definition")
            .and_then(|definition| child_mob_definition_escape(definition, binding))
            .map(|reason| {
                (
                    BINDING_UNCARRIED_KIND,
                    format!(
                        "this launch builds every member with runtime_options.\
                         member_auth_binding, and this child mob would not: {reason}"
                    ),
                )
            }),
        "mob_spawn_member" | "delegate" => {
            let named = args
                .get("auth_binding")
                .filter(|value| !value.is_null())
                .and_then(|value| {
                    serde_json::from_value::<meerkat_contracts::wire::WireAuthBindingRef>(
                        value.clone(),
                    )
                    .ok()
                })
                .map(meerkat_core::AuthBindingRef::from);
            if let Some(conflict) = named
                .as_ref()
                .and_then(|named| binding_conflict(named, binding))
            {
                return Some((BINDING_CONFLICT_KIND, conflict));
            }
            tooling_profile_input(args)
                .and_then(|profile| profile_input_escape(&profile, binding))
                .map(|reason| {
                    (
                        BINDING_UNCARRIED_KIND,
                        format!(
                            "this launch builds every member with runtime_options.\
                             member_auth_binding, and this tooling profile would not: {reason}"
                        ),
                    )
                })
        }
        _ => None,
    }
}

/// The inline profile of an agent tool's `tooling` override
/// (`{"mode": "profile", "source": {"type": "inline", ...}}`), as Core's
/// public profile input, when there is one and it decodes.
fn tooling_profile_input(args: &Value) -> Option<meerkat_contracts::wire::MobProfileInput> {
    let tooling = args.get("tooling")?;
    if tooling.get("mode").and_then(Value::as_str) != Some("profile") {
        return None;
    }
    let mut source = tooling.get("source")?.as_object()?.clone();
    if source.remove("type").as_ref().and_then(Value::as_str) != Some("inline") {
        return None;
    }
    serde_json::from_value(Value::Object(source)).ok()
}

/// Why a child mob an agent creates (`mob_create`, whose `definition` is
/// Core's `MobDefinitionInput`) would build members off the launch's member
/// auth binding, if it would: an automatic spawn policy, whose members Core
/// builds without the spawn customizers, an enabled model fallback table
/// (`runtime.model_fallback`, an inline profile's `model_fallback`) with a
/// target that names another binding or none, or an inline profile that
/// enables image generation. A definition that does not decode is left to
/// Core, which refuses it the same way.
pub(crate) fn child_mob_definition_escape(
    definition: &Value,
    binding: &meerkat_core::AuthBindingRef,
) -> Option<String> {
    let input: meerkat_contracts::wire::MobDefinitionInput =
        serde_json::from_value(definition.clone()).ok()?;
    if matches!(
        input.spawn_policy,
        Some(meerkat_contracts::wire::MobSpawnPolicyInput::Auto { .. })
    ) {
        return Some(
            "its automatic spawn_policy creates members Meerkat builds without the spawn \
             customizers, so they would not build with the launch's member auth binding"
                .to_string(),
        );
    }
    if let Some(reason) = fallback_escape(input.runtime.model_fallback.as_ref(), binding) {
        return Some(format!("runtime.model_fallback: {reason}"));
    }
    input
        .profiles
        .iter()
        .find_map(|(name, profile)| match profile {
            meerkat_contracts::wire::MobProfileBindingInput::Inline(profile) => {
                profile_input_escape(profile, binding)
                    .map(|reason| format!("profiles.{name}.{reason}"))
            }
            meerkat_contracts::wire::MobProfileBindingInput::RealmRef { .. } => None,
        })
}

/// Forces the launch's member auth binding onto every member spawn Core asks
/// the gateway's spawn customizers about: fresh spawns, helpers, batch items,
/// flow provisioning, forks, respawns and successors
/// ([`meerkat_mob::SpawnMemberCustomizer::customize_spawn`]), and the rebuild
/// of a member on its durable session, process-restart restore included
/// (`customize_resume`, whose default asks `customize_spawn`), where Core
/// takes an explicit binding in place of the session's durable one.
///
/// A spawn that names the same binding is accepted. One that names another
/// binding is refused with [`meerkat_mob::MobError::WiringError`] (as Core's
/// own child tool policy refuses), whoever builds the member, so no caller
/// (an agent's `mob_spawn_member` argument, a helper option) moves a member
/// onto another account; an agent meets it typed first
/// ([`agent_tool_binding_refusal`]). A member this gateway builds is also
/// refused when its profile override enables image generation or carries a
/// model fallback off the binding, or its spawn explicitly enables image
/// generation or web search: Core builds those executors by default
/// selection. A member the spawn places on another member host, or binds to
/// an external process, is built elsewhere, so nothing is filled in; a
/// profile- or definition-level external backend still receives the binding,
/// which nothing builds locally.
///
/// Core asks no customizer for a durable fork's seating (a `fork_off` child,
/// a live delegation fork, a council participant) or its rebuilds: those
/// repeat the member's first build from its own durable records, which name
/// the binding its source was built with. Nor does it ask one for an identity
/// it materializes from a sealed declaration, so the gateway refuses
/// `mob/adopt_member_identity_declaration` and
/// `mob/apply_member_tool_declaration` under a member auth binding
/// ([`MemberRouteRefusal::declaration_uncarried`]), or for a policy
/// auto-spawn, which init and the child-mob guard
/// ([`child_mob_definition_escape`]) refuse.
pub(crate) struct MemberAuthBindingCustomizer {
    binding: meerkat_core::AuthBindingRef,
}

impl MemberAuthBindingCustomizer {
    pub(crate) fn new(binding: meerkat_core::AuthBindingRef) -> Self {
        Self { binding }
    }

    /// The customizer body, factored off the trait so tests can drive it:
    /// `SpawnCustomizationContext` is `#[non_exhaustive]` and only
    /// meerkat-mob constructs it. The binding does not depend on the spawn
    /// source.
    fn apply(&self, spec: &mut meerkat_mob::SpawnMemberSpec) -> Result<(), meerkat_mob::MobError> {
        // A named binding is compared on every spawn, a placed or external
        // member's included, so no caller chooses another account; only a
        // member this gateway builds gets the launch binding filled in.
        if let Some(conflict) = spec
            .auth_binding
            .as_ref()
            .and_then(|named| binding_conflict(named, &self.binding))
        {
            return Err(meerkat_mob::MobError::WiringError(format!(
                "member '{}': {conflict}",
                spec.identity
            )));
        }
        if !built_by_this_gateway(spec) {
            return Ok(());
        }
        if spec.auth_binding.is_none() {
            spec.auth_binding = Some(self.binding.clone());
        }
        let enabled = meerkat_core::ToolCategoryOverride::Enable;
        let escape = spec
            .override_profile
            .as_ref()
            .and_then(|profile| profile_escape(profile, &self.binding))
            .map(|reason| format!("profile override {reason}"))
            .or_else(|| {
                (spec.tool_category_overrides.image_generation == enabled).then(|| {
                    "tool_category_overrides.image_generation: Meerkat builds image \
                     generation executors for the other image providers by default binding \
                     selection"
                        .to_string()
                })
            })
            .or_else(|| {
                (spec.tool_category_overrides.web_search == enabled).then(|| {
                    "tool_category_overrides.web_search: an explicit web search enable has \
                     Meerkat build its fallback web search executor by default binding \
                     selection when native search is not active"
                        .to_string()
                })
            });
        if let Some(reason) = escape {
            return Err(meerkat_mob::MobError::WiringError(format!(
                "{BINDING_UNCARRIED_KIND}: member '{}' {reason}; every member of this launch \
                 builds with {} (runtime_options.member_auth_binding)",
                spec.identity,
                describe_binding(&self.binding)
            )));
        }
        Ok(())
    }
}

/// Whether this gateway's member factory builds the member `spec` spawns, as
/// the spawn request says: a local session member, not one it places on
/// another member host and not one it binds to an external process.
fn built_by_this_gateway(spec: &meerkat_mob::SpawnMemberSpec) -> bool {
    spec.placement.is_none()
        && !matches!(spec.backend, Some(meerkat_mob::MobBackendKind::External))
        && matches!(
            spec.binding,
            None | Some(meerkat_mob::RuntimeBinding::Session)
        )
}

impl meerkat_mob::SpawnMemberCustomizer for MemberAuthBindingCustomizer {
    fn customize_spawn(
        &self,
        _ctx: &meerkat_mob::SpawnCustomizationContext,
        spec: &mut meerkat_mob::SpawnMemberSpec,
    ) -> Result<(), meerkat_mob::MobError> {
        self.apply(spec)
    }
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic,
    clippy::needless_raw_string_hashes
)]
mod tests {
    use super::*;
    use serde_json::json;

    const REALM: &str = "native-team";

    /// The JSON a member auth binding serializes to: Core's own
    /// serialization of the typed binding, which omits the configured
    /// origin.
    fn member_auth_binding_json(binding: &meerkat_core::AuthBindingRef) -> Value {
        serde_json::to_value(binding).expect("Core serializes an auth binding")
    }

    fn binding(name: &str) -> meerkat_core::AuthBindingRef {
        meerkat_core::AuthBindingRef {
            realm: meerkat_core::RealmId::parse(REALM).unwrap(),
            binding: meerkat_core::BindingId::parse(name).unwrap(),
            profile: None,
            origin: meerkat_core::BindingOrigin::Configured,
        }
    }

    fn selection_json(root: &Path) -> Value {
        json!({
            "realm_id": REALM,
            "context_root": root.join("context"),
            "state_root": root.join("realms"),
            "user_config_root": root.join("home"),
        })
    }

    fn route(options: &Value) -> Result<MemberRoute, MemberRouteRefusal> {
        MemberRoute::from_runtime_options(options.as_object().expect("an options object"))
    }

    fn refusal_kind(result: Result<impl std::fmt::Debug, MemberRouteRefusal>) -> String {
        match result {
            Ok(value) => panic!("expected a refusal, got {value:?}"),
            Err(refusal) => {
                assert_eq!(
                    refusal.data()["kind"],
                    json!(refusal.kind.as_str()),
                    "data.kind is the refusal's kind"
                );
                refusal.kind.as_str().to_string()
            }
        }
    }

    /// The realm's own document: two OpenAI API key bindings, `a` the realm
    /// default and `b` the one the members are bound to, each reading its key
    /// from its own environment variable, and a self-hosted server with one
    /// alias behind a third binding.
    const REALM_DOCUMENT: &str = r#"
[self_hosted.servers.local]
transport = "openai_compatible"
base_url = "http://127.0.0.1:9"
api_style = "chat_completions"

[self_hosted.models.realm-alias]
server = "local"
remote_model = "remote-model"

[realm.native-team]
default_binding = "a"

[realm.native-team.backend.openai_main]
provider = "openai"
backend_kind = "openai_api"

[realm.native-team.auth.key_a]
provider = "openai"
auth_method = "api_key"
source = { kind = "env", env = "NATIVE_REALM_UNIT_KEY_A" }

[realm.native-team.auth.key_b]
provider = "openai"
auth_method = "api_key"
source = { kind = "env", env = "NATIVE_REALM_UNIT_KEY_B" }

[realm.native-team.binding.a]
backend_profile = "openai_main"
auth_profile = "key_a"

[realm.native-team.binding.b]
backend_profile = "openai_main"
auth_profile = "key_b"

[realm.native-team.backend.local]
provider = "self_hosted"
backend_kind = "self_hosted"
server = "local"

[realm.native-team.auth.local_none]
provider = "self_hosted"
auth_method = "none"
source = { kind = "platform_default" }

[realm.native-team.binding.local]
backend_profile = "local"
auth_profile = "local_none"
"#;

    /// A native realm on disk: its manifest written by the store's own realm
    /// API, its document beside it, and an empty user configuration root.
    async fn native_realm_on_disk(root: &Path) -> NativeRealmSelection {
        let realms = root.join("realms");
        meerkat_store::ensure_realm_manifest_in(&realms, REALM, None, None)
            .await
            .expect("the realm manifest");
        std::fs::write(
            meerkat_store::realm_paths_in(&realms, REALM).config_path,
            REALM_DOCUMENT,
        )
        .expect("the realm document");
        std::fs::create_dir_all(root.join("home")).expect("the user configuration root");
        std::fs::create_dir_all(root.join("context")).expect("the context root");
        NativeRealmSelection::parse(&selection_json(root)).expect("a valid selection")
    }

    fn definition(profiles: &str) -> meerkat_mob::MobDefinition {
        meerkat_mob::MobDefinition::from_toml(&format!(
            "[mob]\nid = \"native-realm-unit\"\n\n{profiles}"
        ))
        .expect("the test definition")
    }

    const OPENAI_PROFILE: &str = "[profiles.worker]\nmodel = \"gpt-5.5\"\nprovider = \"openai\"\n";

    // -- parse and serde round trip ----------------------------------------

    #[test]
    fn native_realm_parses_and_round_trips_through_serde() {
        let root = Path::new("/srv/native");
        let value = selection_json(root);
        let parsed = NativeRealmSelection::parse(&value).expect("a valid selection");
        assert_eq!(parsed.realm_id, REALM);
        assert_eq!(parsed.state_root, root.join("realms"));
        let serialized = serde_json::to_value(&parsed).expect("serialize");
        assert_eq!(serialized, value, "the wire form is the option as sent");
        assert_eq!(
            NativeRealmSelection::parse(&serialized).expect("reparse"),
            parsed,
            "serialize then parse is the identity"
        );
    }

    #[test]
    fn member_auth_binding_parses_into_cores_typed_binding_and_round_trips() {
        let parsed = parse_member_auth_binding(&json!({
            "realm": REALM,
            "binding": "b",
            "profile": "work",
            "origin": "configured",
        }))
        .expect("a configured binding");
        assert_eq!(
            parsed,
            meerkat_core::AuthBindingRef {
                realm: meerkat_core::RealmId::parse(REALM).unwrap(),
                binding: meerkat_core::BindingId::parse("b").unwrap(),
                profile: Some(meerkat_core::ProfileId::parse("work").unwrap()),
                origin: meerkat_core::BindingOrigin::Configured,
            }
        );
        // Core serializes the configured origin by omitting it; the option
        // reads that form back to the same binding.
        let serialized = member_auth_binding_json(&parsed);
        assert_eq!(
            serialized,
            json!({ "realm": REALM, "binding": "b", "profile": "work" })
        );
        assert_eq!(
            parse_member_auth_binding(&serialized).expect("reparse"),
            parsed
        );
        // And Core's own deserialization of the option's form agrees.
        let core: meerkat_core::AuthBindingRef =
            serde_json::from_value(serialized).expect("Core reads the option's form");
        assert_eq!(core, parsed);
        // Without `origin` and `profile` the binding is configured and has
        // no profile.
        let bare = parse_member_auth_binding(&json!({ "realm": REALM, "binding": "b" }))
            .expect("a bare binding");
        assert_eq!(bare, binding("b"));
    }

    #[test]
    fn absent_options_route_nothing() {
        assert_eq!(route(&json!({})).unwrap(), MemberRoute::default());
        assert_eq!(
            MemberRoute::from_init_params(&json!({})).unwrap(),
            MemberRoute::default()
        );
        // A binding alone routes the members of a host config file or the
        // default configuration.
        let alone = route(&json!({
            "member_auth_binding": { "realm": REALM, "binding": "b" }
        }))
        .expect("a binding alone");
        assert_eq!(alone.member_auth_binding, Some(binding("b")));
        assert_eq!(alone.native_realm, None);
    }

    // -- refusals -----------------------------------------------------------

    #[test]
    fn a_native_realm_beside_the_host_config_file_is_refused() {
        let options = json!({
            "native_realm": selection_json(Path::new("/srv/native")),
            "member_auth_binding": { "realm": REALM, "binding": "b" },
            "meerkat_config_path": "/srv/native/config.toml",
        });
        assert_eq!(
            refusal_kind(route(&options)),
            "native_realm_config_conflict"
        );
        let refusal = route(&options).unwrap_err();
        assert_eq!(refusal.code(), -32602);
        assert!(
            refusal.message().contains("meerkat_config_path"),
            "{refusal}"
        );
    }

    #[test]
    fn a_native_realm_without_a_binding_is_refused() {
        let options = json!({ "native_realm": selection_json(Path::new("/srv/native")) });
        assert_eq!(
            refusal_kind(route(&options)),
            "member_auth_binding_required"
        );
    }

    #[test]
    fn malformed_native_realms_are_refused() {
        let root = Path::new("/srv/native");
        let mut unknown_field = selection_json(root);
        unknown_field["extra"] = json!(true);
        let mut missing = selection_json(root);
        missing.as_object_mut().unwrap().remove("state_root");
        let mut relative = selection_json(root);
        relative["state_root"] = json!("realms");
        let mut bad_id = selection_json(root);
        bad_id["realm_id"] = json!("not a realm");
        for value in [
            json!("native-team"),
            unknown_field,
            missing,
            relative,
            bad_id,
        ] {
            let options = json!({
                "native_realm": value,
                "member_auth_binding": { "realm": REALM, "binding": "b" },
            });
            assert_eq!(
                refusal_kind(route(&options)),
                "native_realm_invalid",
                "{options}"
            );
        }
    }

    #[test]
    fn malformed_bindings_are_refused() {
        for value in [
            json!("native-team:b"),
            json!({ "realm": REALM }),
            json!({ "realm": REALM, "binding": "" }),
            json!({ "realm": REALM, "binding": "has space" }),
            json!({ "realm": REALM, "binding": "b", "token": "x" }),
            json!({ "realm": REALM, "binding": "b", "origin": 1 }),
        ] {
            assert_eq!(
                refusal_kind(parse_member_auth_binding(&value)),
                "member_auth_binding_invalid",
                "{value}"
            );
        }
    }

    #[test]
    fn an_origin_other_than_configured_is_refused() {
        for origin in ["synthetic_env_default", "Configured", "env"] {
            let refusal = parse_member_auth_binding(&json!({
                "realm": REALM,
                "binding": "b",
                "origin": origin,
            }))
            .unwrap_err();
            assert_eq!(refusal.kind, MemberRouteRefusalKind::OriginNotConfigured);
            assert_eq!(refusal.data()["origin"], json!(origin));
            assert_eq!(refusal.code(), -32602);
        }
    }

    #[tokio::test]
    async fn an_unknown_realm_is_refused_with_the_storage_resolution_code() {
        let root = tempfile::tempdir().unwrap();
        let realms = root.path().join("realms");
        std::fs::create_dir_all(&realms).unwrap();
        let selection = NativeRealmSelection::parse(&selection_json(root.path())).unwrap();
        let refusal = read_native_realm_config(&selection).await.unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::RealmUnknown);
        assert_eq!(refusal.code(), crate::STORAGE_RESOLUTION_CODE);
        assert_eq!(refusal.data()["realm_id"], json!(REALM));
        assert!(
            !realms.join(REALM).exists(),
            "reading a native realm creates nothing in it"
        );
    }

    /// A realm document that does not parse is refused as invalid params,
    /// as a `meerkat_config_path` file that does not parse is.
    #[tokio::test]
    async fn an_unreadable_realm_document_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let selection = native_realm_on_disk(root.path()).await;
        std::fs::write(
            meerkat_store::realm_paths_in(&selection.state_root, REALM).config_path,
            "[realm.native-team\nnot toml",
        )
        .unwrap();
        let refusal = read_native_realm_config(&selection).await.unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::ConfigInvalid);
        assert_eq!(refusal.code(), -32602);
        assert_eq!(refusal.data()["kind"], json!("native_realm_config_invalid"));
        assert_eq!(refusal.data()["realm_id"], json!(REALM));
        assert!(refusal.data()["reason"].is_string(), "{:?}", refusal.data());
    }

    #[tokio::test]
    async fn the_native_realm_reads_its_own_document_through_cores_reader() {
        let root = tempfile::tempdir().unwrap();
        let selection = native_realm_on_disk(root.path()).await;
        let config = read_native_realm_config(&selection)
            .await
            .expect("the realm");
        let section = config
            .realm
            .get(REALM)
            .expect("the composed configuration keeps the realm's section");
        assert!(section.binding.contains_key("b"), "{section:?}");
        assert!(
            config.self_hosted.models.contains_key("realm-alias"),
            "the realm document's tables reach the members' configuration"
        );
    }

    #[tokio::test]
    async fn a_missing_binding_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let config = read_native_realm_config(&native_realm_on_disk(root.path()).await)
            .await
            .expect("the realm");
        let refusal =
            validate_member_auth_binding(&config, &binding("absent"), &definition(OPENAI_PROFILE))
                .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::BindingUnknown);
        assert_eq!(refusal.data()["binding"], json!("absent"));
        // A binding's auth profile is named `auth_profile`.
        let mut with_profile = binding("absent");
        with_profile.profile = Some(meerkat_core::ProfileId::parse("work").unwrap());
        let refusal =
            validate_member_auth_binding(&config, &with_profile, &definition(OPENAI_PROFILE))
                .unwrap_err();
        assert_eq!(refusal.data()["auth_profile"], json!("work"));
        // A realm the configuration does not hold is refused the same way.
        let mut elsewhere = binding("b");
        elsewhere.realm = meerkat_core::RealmId::parse("another-team").unwrap();
        let refusal =
            validate_member_auth_binding(&config, &elsewhere, &definition(OPENAI_PROFILE))
                .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::BindingUnknown);
        // And the default configuration holds no realm binding at all.
        let refusal = validate_member_auth_binding(
            &meerkat_core::Config::default(),
            &binding("b"),
            &definition(OPENAI_PROFILE),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::BindingUnknown);
    }

    #[tokio::test]
    async fn a_profile_the_binding_does_not_serve_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let config = read_native_realm_config(&native_realm_on_disk(root.path()).await)
            .await
            .expect("the realm");
        validate_member_auth_binding(&config, &binding("b"), &definition(OPENAI_PROFILE))
            .expect("an OpenAI profile matches the OpenAI binding");

        // Another provider, named by the profile.
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(
                "[profiles.lead]\nmodel = \"claude-sonnet-4-5\"\nprovider = \"anthropic\"\n",
            ),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::TargetMismatch);
        assert_eq!(refusal.data()["mob_profile"], json!("lead"));
        assert_eq!(refusal.data()["binding_provider"], json!("openai"));
        assert!(
            refusal.data().get("profile").is_none() && refusal.data().get("profile_id").is_none(),
            "the definition profile is `mob_profile`, the binding's auth profile `auth_profile`: \
             {:?}",
            refusal.data()
        );

        // Another provider, owned by the catalog.
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition("[profiles.lead]\nmodel = \"claude-sonnet-4-5\"\n"),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::TargetMismatch);

        // The binding's provider named over a model another provider owns.
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition("[profiles.lead]\nmodel = \"claude-sonnet-4-5\"\nprovider = \"openai\"\n"),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::TargetMismatch);
        assert_eq!(refusal.data()["model"], json!("claude-sonnet-4-5"));

        // A self-hosted binding serves its own server's aliases only.
        validate_member_auth_binding(
            &config,
            &binding("local"),
            &definition(
                "[profiles.local]\nmodel = \"realm-alias\"\nprovider = \"self_hosted\"\n\
                 self_hosted_server_id = \"local\"\n",
            ),
        )
        .expect("the realm's alias on the binding's server");
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(
                "[profiles.local]\nmodel = \"realm-alias\"\nprovider = \"self_hosted\"\n\
                 self_hosted_server_id = \"local\"\n",
            ),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::TargetMismatch);
        // A self-hosted model the members' configuration does not register
        // fails at the build, as Core's factory requires it registered with
        // a server: init refuses it first.
        let refusal = validate_member_auth_binding(
            &config,
            &binding("local"),
            &definition(
                "[profiles.local]\nmodel = \"unregistered-model\"\nprovider = \"self_hosted\"\n",
            ),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::TargetMismatch);
        assert_eq!(refusal.data()["model"], json!("unregistered-model"));
    }

    #[tokio::test]
    async fn an_automatic_spawn_policy_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let config = read_native_realm_config(&native_realm_on_disk(root.path()).await)
            .await
            .expect("the realm");
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(&format!(
                "{OPENAI_PROFILE}\n[spawn_policy]\nmode = \"auto\"\n\n\
                 [spawn_policy.profile_map]\nanyone = \"worker\"\n"
            )),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::Uncarried);
    }

    // -- the customizer -----------------------------------------------------

    #[test]
    fn every_locally_built_spawn_carries_the_binding() {
        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let mut fresh = meerkat_mob::SpawnMemberSpec::new("worker", "keeper");
        customizer.apply(&mut fresh).expect("a fresh spawn");
        assert_eq!(fresh.auth_binding, Some(binding("b")));

        // The rebuild of a member on its durable session.
        let mut resumed = meerkat_mob::SpawnMemberSpec::new("worker", "keeper").with_launch_mode(
            meerkat_mob::launch::MemberLaunchMode::Resume {
                bridge_session_id: meerkat_core::types::SessionId::new(),
                resume_from_role: None,
            },
        );
        customizer.apply(&mut resumed).expect("a rebuild");
        assert_eq!(resumed.auth_binding, Some(binding("b")));

        // A member bound to a local session explicitly.
        let mut session_bound = meerkat_mob::SpawnMemberSpec::new("worker", "spawned");
        session_bound.binding = Some(meerkat_mob::RuntimeBinding::Session);
        customizer
            .apply(&mut session_bound)
            .expect("a session member");
        assert_eq!(session_bound.auth_binding, Some(binding("b")));

        // A spawn that names the launch binding itself is accepted as is.
        let mut same =
            meerkat_mob::SpawnMemberSpec::new("worker", "helper").with_auth_binding(binding("b"));
        customizer.apply(&mut same).expect("the launch binding");
        assert_eq!(same.auth_binding, Some(binding("b")));
    }

    /// The launch binding is forced: a spawn that names another binding,
    /// another realm's or the same binding with another auth profile, is
    /// refused, never built on another account.
    #[test]
    fn a_spawn_that_names_another_binding_is_refused() {
        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let mut other_profile = binding("b");
        other_profile.profile = Some(meerkat_core::ProfileId::parse("work").unwrap());
        let mut other_realm = binding("b");
        other_realm.realm = meerkat_core::RealmId::parse("global").unwrap();
        for named in [binding("a"), other_profile, other_realm] {
            let mut spec = meerkat_mob::SpawnMemberSpec::new("worker", "helper")
                .with_auth_binding(named.clone());
            let error = customizer.apply(&mut spec).unwrap_err();
            assert!(
                matches!(&error, meerkat_mob::MobError::WiringError(message)
                    if message.contains("member_auth_binding")),
                "{error:?}"
            );
            assert_eq!(spec.auth_binding, Some(named), "nothing is rewritten");
        }
    }

    /// A profile override whose model fallback would build off the binding
    /// is refused at the spawn, as an inline profile's is at init.
    #[test]
    fn a_profile_override_whose_fallback_leaves_the_binding_is_refused() {
        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let override_with = |fallback: &str| {
            let definition = definition(&format!(
                "{OPENAI_PROFILE}\n[profiles.worker.model_fallback]\nenabled = true\n{fallback}"
            ));
            definition
                .profiles
                .values()
                .find_map(|profile| profile.as_inline().cloned())
                .expect("an inline profile")
        };
        let mut unbound = meerkat_mob::SpawnMemberSpec::new("worker", "helper");
        unbound.override_profile = Some(override_with(
            "[[profiles.worker.model_fallback.chain]]\nmodel = \"gpt-5.4\"\n",
        ));
        assert!(customizer.apply(&mut unbound).is_err());

        let mut bound = meerkat_mob::SpawnMemberSpec::new("worker", "helper");
        bound.override_profile = Some(override_with(
            "[[profiles.worker.model_fallback.chain]]\nmodel = \"gpt-5.4\"\n\
             auth_binding = { realm = \"native-team\", binding = \"b\" }\n",
        ));
        customizer
            .apply(&mut bound)
            .expect("a fallback on the launch binding");
    }

    #[test]
    fn members_built_elsewhere_are_left_alone() {
        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let mut placed = meerkat_mob::SpawnMemberSpec::new("worker", "remote").with_placement(
            meerkat_mob::machines::mob_machine::HostId::from("member-host".to_string()),
        );
        customizer.apply(&mut placed).expect("a placed member");
        assert_eq!(placed.auth_binding, None);

        let mut external = meerkat_mob::SpawnMemberSpec::new("worker", "peer")
            .with_backend(meerkat_mob::MobBackendKind::External);
        customizer.apply(&mut external).expect("an external member");
        assert_eq!(external.auth_binding, None);
    }

    // -- model fallback, child mobs, adoption -------------------------------

    /// An enabled fallback chain builds a member with each target's own
    /// binding, and a target that names none by default selection: init
    /// refuses either unless the target names the launch binding, wherever
    /// the table is (the members' configuration, the definition runtime, an
    /// inline profile). A disabled table builds nothing and passes.
    #[tokio::test]
    async fn a_fallback_target_off_the_binding_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let config = read_native_realm_config(&native_realm_on_disk(root.path()).await)
            .await
            .expect("the realm");
        let unbound = "[[profiles.worker.model_fallback.chain]]\nmodel = \"gpt-5.4\"\n";
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(&format!(
                "{OPENAI_PROFILE}\n[profiles.worker.model_fallback]\nenabled = true\n{unbound}"
            )),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::Uncarried);
        assert!(
            refusal.data()["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("profiles.worker.model_fallback")),
            "{:?}",
            refusal.data()
        );

        let other = "[[runtime.model_fallback.chain]]\nmodel = \"gpt-5.4\"\n\
                     auth_binding = { realm = \"native-team\", binding = \"a\" }\n";
        let refusal = validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(&format!(
                "[runtime.model_fallback]\nenabled = true\n{other}\n{OPENAI_PROFILE}"
            )),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::Uncarried);

        let mut fallback_config = config.clone();
        fallback_config.model_fallback.enabled = Some(true);
        fallback_config
            .model_fallback
            .chain
            .push(meerkat_core::config::ModelFallbackTarget {
                model: "gpt-5.4".to_string(),
                provider: None,
                auth_binding: None,
            });
        let refusal = validate_member_auth_binding(
            &fallback_config,
            &binding("b"),
            &definition(OPENAI_PROFILE),
        )
        .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::Uncarried);

        // On the launch binding, or disabled, the chain passes.
        let bound = "[[profiles.worker.model_fallback.chain]]\nmodel = \"gpt-5.4\"\n\
                     auth_binding = { realm = \"native-team\", binding = \"b\" }\n";
        validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(&format!(
                "{OPENAI_PROFILE}\n[profiles.worker.model_fallback]\nenabled = true\n{bound}"
            )),
        )
        .expect("a fallback on the launch binding");
        validate_member_auth_binding(
            &config,
            &binding("b"),
            &definition(&format!(
                "{OPENAI_PROFILE}\n[profiles.worker.model_fallback]\nenabled = false\n{unbound}"
            )),
        )
        .expect("a disabled fallback");
    }

    /// A child mob an agent creates may not build members off the binding:
    /// an automatic spawn policy or a fallback target off the binding is
    /// named, and a plain child definition (or one Core will refuse to
    /// decode anyway) passes.
    #[test]
    fn a_child_mob_that_would_leave_the_binding_is_named() {
        let launch = binding("b");
        let plain = json!({
            "id": "child",
            "profiles": { "worker": { "model": "gpt-5.5" } },
        });
        assert_eq!(child_mob_definition_escape(&plain, &launch), None);
        assert_eq!(
            child_mob_definition_escape(&json!("not a definition"), &launch),
            None
        );

        let mut auto = plain.clone();
        auto["spawn_policy"] = json!({ "mode": "auto", "profile_map": { "anyone": "worker" } });
        assert!(
            child_mob_definition_escape(&auto, &launch)
                .is_some_and(|reason| reason.contains("spawn_policy")),
            "an automatic spawn policy is named"
        );
        let mut none_policy = plain.clone();
        none_policy["spawn_policy"] = json!({ "mode": "none" });
        assert_eq!(child_mob_definition_escape(&none_policy, &launch), None);

        let mut runtime_fallback = plain.clone();
        runtime_fallback["runtime"] = json!({ "model_fallback": {
            "enabled": true,
            "chain": [{ "model": "gpt-5.4" }],
        } });
        assert!(
            child_mob_definition_escape(&runtime_fallback, &launch)
                .is_some_and(|reason| reason.contains("runtime.model_fallback"))
        );
        let mut profile_fallback = plain.clone();
        profile_fallback["profiles"]["worker"]["model_fallback"] = json!({
            "enabled": true,
            "chain": [{ "model": "gpt-5.4", "auth_binding": { "realm": REALM, "binding": "a" } }],
        });
        assert!(
            child_mob_definition_escape(&profile_fallback, &launch)
                .is_some_and(|reason| reason.contains("profiles.worker.model_fallback"))
        );
        let mut bound_fallback = plain;
        bound_fallback["profiles"]["worker"]["model_fallback"] = json!({
            "enabled": true,
            "chain": [{ "model": "gpt-5.4", "auth_binding": member_auth_binding_json(&launch) }],
        });
        assert_eq!(child_mob_definition_escape(&bound_fallback, &launch), None);
    }

    #[test]
    fn adoption_under_a_member_auth_binding_is_refused_as_uncarried() {
        let refusal =
            MemberRouteRefusal::declaration_uncarried("mob/adopt_member_identity_declaration");
        assert_eq!(refusal.code(), -32602);
        assert_eq!(
            refusal.data()["kind"],
            json!("member_auth_binding_uncarried")
        );
        assert!(
            refusal.data()["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("mob/adopt_member_identity_declaration")),
            "{:?}",
            refusal.data()
        );
    }

    /// Agent memory engines build their clients off the binding, so a launch
    /// with one refuses them, typed.
    #[test]
    fn agent_memory_engines_under_a_member_auth_binding_are_refused_as_uncarried() {
        let refusal = MemberRouteRefusal::memory_engines_uncarried();
        assert_eq!(refusal.code(), -32602);
        assert_eq!(
            refusal.data()["kind"],
            json!("member_auth_binding_uncarried")
        );
        assert!(
            refusal.data()["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("agent_memory.distiller")),
            "{:?}",
            refusal.data()
        );
    }

    /// Image generation builds executors for the other image providers by
    /// default selection: an inline profile that enables it is refused at
    /// init, in a profile override at the spawn, and in a child mob's or a
    /// tooling profile's inline profile at the agent's tool.
    #[tokio::test]
    async fn image_generation_off_the_binding_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let config = read_native_realm_config(&native_realm_on_disk(root.path()).await)
            .await
            .expect("the realm");
        let with_images =
            format!("{OPENAI_PROFILE}\n[profiles.worker.tools]\nimage_generation = true\n");
        let refusal =
            validate_member_auth_binding(&config, &binding("b"), &definition(&with_images))
                .unwrap_err();
        assert_eq!(refusal.kind, MemberRouteRefusalKind::Uncarried);
        assert!(
            refusal.data()["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("profiles.worker.tools.image_generation")),
            "{:?}",
            refusal.data()
        );

        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let mut overridden = meerkat_mob::SpawnMemberSpec::new("worker", "helper");
        overridden.override_profile = definition(&with_images)
            .profiles
            .values()
            .find_map(|profile| profile.as_inline().cloned());
        assert!(customizer.apply(&mut overridden).is_err());

        let mut child = json!({
            "id": "child",
            "profiles": { "worker": { "model": "gpt-5.5", "tools": { "image_generation": true } } },
        });
        assert!(
            child_mob_definition_escape(&child, &binding("b"))
                .is_some_and(|reason| reason.contains("image_generation"))
        );
        child["profiles"]["worker"]["tools"]["image_generation"] = json!(false);
        assert_eq!(child_mob_definition_escape(&child, &binding("b")), None);
    }

    /// A spawn that explicitly enables image generation or web search is
    /// refused: Core builds those executors by default selection.
    #[test]
    fn a_spawn_enabling_executors_off_the_binding_is_refused() {
        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let mut web = meerkat_mob::SpawnMemberSpec::new("worker", "helper");
        web.tool_category_overrides.web_search = meerkat_core::ToolCategoryOverride::Enable;
        assert!(customizer.apply(&mut web).is_err());
        let mut images = meerkat_mob::SpawnMemberSpec::new("worker", "helper");
        images.tool_category_overrides.image_generation =
            meerkat_core::ToolCategoryOverride::Enable;
        assert!(customizer.apply(&mut images).is_err());
        let mut inherit = meerkat_mob::SpawnMemberSpec::new("worker", "helper");
        customizer
            .apply(&mut inherit)
            .expect("inherited categories pass");
    }

    /// A placed or external member is built elsewhere and gets nothing filled
    /// in, but one that names another binding is still refused: no caller
    /// chooses another account.
    #[test]
    fn a_member_built_elsewhere_naming_another_binding_is_refused() {
        let customizer = MemberAuthBindingCustomizer::new(binding("b"));
        let mut placed = meerkat_mob::SpawnMemberSpec::new("worker", "remote")
            .with_placement(meerkat_mob::machines::mob_machine::HostId::from(
                "member-host".to_string(),
            ))
            .with_auth_binding(binding("a"));
        assert!(customizer.apply(&mut placed).is_err());
        let mut placed_same = meerkat_mob::SpawnMemberSpec::new("worker", "remote")
            .with_placement(meerkat_mob::machines::mob_machine::HostId::from(
                "member-host".to_string(),
            ))
            .with_auth_binding(binding("b"));
        customizer
            .apply(&mut placed_same)
            .expect("the launch binding itself");
    }

    /// The typed refusals an agent's mob tool answers: another binding is
    /// `member_auth_binding_conflict`; an inline tooling profile or a child
    /// definition off the binding is `member_auth_binding_uncarried`; the
    /// launch binding and other tools pass.
    #[test]
    fn agent_tools_refuse_bindings_and_profiles_off_the_launch_binding_typed() {
        let launch = binding("b");
        let spawn = |extra: Value| {
            let mut args = json!({ "mob_id": "child", "profile": "worker", "member_id": "w1" });
            if let (Some(args), Some(extra)) = (args.as_object_mut(), extra.as_object()) {
                args.extend(extra.clone());
            }
            args
        };
        let conflict = agent_tool_binding_refusal(
            "mob_spawn_member",
            &spawn(json!({ "auth_binding": { "realm": REALM, "binding": "a" } })),
            &launch,
        );
        assert_eq!(conflict.map(|(kind, _)| kind), Some(BINDING_CONFLICT_KIND));
        assert_eq!(
            agent_tool_binding_refusal(
                "mob_spawn_member",
                &spawn(json!({ "auth_binding": { "realm": REALM, "binding": "b" } })),
                &launch,
            ),
            None
        );
        let tooling = json!({ "tooling": {
            "mode": "profile",
            "source": { "type": "inline", "model": "gpt-5.5", "tools": { "image_generation": true } },
        } });
        assert_eq!(
            agent_tool_binding_refusal("delegate", &tooling, &launch).map(|(kind, _)| kind),
            Some(BINDING_UNCARRIED_KIND)
        );
        assert_eq!(
            agent_tool_binding_refusal("mob_spawn_member", &spawn(tooling.clone()), &launch)
                .map(|(kind, _)| kind),
            Some(BINDING_UNCARRIED_KIND)
        );
        let inherit = json!({ "tooling": { "mode": "inherit_parent" } });
        assert_eq!(
            agent_tool_binding_refusal("delegate", &inherit, &launch),
            None
        );
        let child = json!({ "definition": {
            "id": "child",
            "profiles": { "worker": { "model": "gpt-5.5" } },
            "spawn_policy": { "mode": "auto", "profile_map": { "anyone": "worker" } },
        } });
        assert_eq!(
            agent_tool_binding_refusal("mob_create", &child, &launch).map(|(kind, _)| kind),
            Some(BINDING_UNCARRIED_KIND)
        );
        assert_eq!(
            agent_tool_binding_refusal("fork_off", &child, &launch),
            None
        );
    }
}
