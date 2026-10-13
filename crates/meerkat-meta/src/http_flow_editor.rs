use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, Uri, header},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::Value;

use crate::access::{ACTION_MOBPACK_AUTHOR, ACTION_MOBPACK_DEPLOY, AccessController, AccessView};
use crate::http_console::ACCESS_DENIED_RPC_CODE;
use crate::http_sse::sse_access_context;
use crate::mobpack::MobpackRuntimeCatalogState;
use crate::rpc::{JSONRPC_VERSION, JsonRpcError, JsonRpcRequest, JsonRpcResponse};
use crate::runtime::RuntimeDecisionState;

const FLOW_EDITOR_FRONTEND_INDEX_HTML: &str = include_str!("../flow-editor-dist/index.html");
const FLOW_EDITOR_FRONTEND_VENDOR_JS: &str = include_str!("../flow-editor-dist/react-globals.js");
const FLOW_EDITOR_FRONTEND_APP_JS: &str = include_str!("../flow-editor-dist/flow-editor.js");
const FLOW_EDITOR_FRONTEND_APP_CSS: &str = include_str!("../flow-editor-dist/flow-editor.css");

/// Standalone HTTP surface for the Meta Flow Editor.
///
/// This route plane deliberately serves only the editor shell and the
/// mobpack-authoring JSON-RPC methods. Console runtime methods remain on the
/// console RPC plane.
pub fn flow_editor_router() -> Router {
    standalone_favicon_router::<()>()
        .merge(flow_editor_frontend_router::<()>())
        .merge(flow_editor_rpc_router::<()>())
}

pub fn flow_editor_router_with_host_deploy() -> Router {
    standalone_favicon_router::<()>()
        .merge(flow_editor_frontend_router::<()>())
        .merge(flow_editor_rpc_router_allowing_host_deploy::<()>())
}

/// Quiet-favicon route for the standalone Flow Editor server only. It must
/// not be part of `flow_editor_frontend_router`: hosts that merge the Flow
/// Editor with the console (for example `build_reference_app_router`) already
/// register `/favicon.ico`, and axum panics on duplicate routes.
fn standalone_favicon_router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/favicon.ico", get(|| async { StatusCode::NO_CONTENT }))
}

pub fn protected_flow_editor_router(decisions: RuntimeDecisionState) -> Router {
    flow_editor_frontend_router::<()>().merge(flow_editor_rpc_router_with_decisions(decisions))
}

pub(crate) fn protected_flow_editor_router_with_runtime_catalog(
    decisions: RuntimeDecisionState,
    runtime_catalog: MobpackRuntimeCatalogState,
    access: Option<AccessController>,
) -> Router {
    flow_editor_frontend_router::<()>().merge(flow_editor_rpc_router_with_runtime_catalog(
        decisions,
        Some(runtime_catalog),
        access,
    ))
}

pub fn flow_editor_frontend_router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/flow-editor", get(flow_editor_frontend_index_handler))
        .route("/flow-editor/", get(flow_editor_frontend_index_handler))
        .route(
            "/flow-editor/assets/react-globals.js",
            get(flow_editor_frontend_vendor_js_handler),
        )
        .route(
            "/flow-editor/assets/flow-editor.js",
            get(flow_editor_frontend_app_js_handler),
        )
        .route(
            "/flow-editor/assets/flow-editor.css",
            get(flow_editor_frontend_app_css_handler),
        )
}

pub fn flow_editor_rpc_router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/flow-editor/rpc", post(flow_editor_rpc_handler))
}

pub fn flow_editor_rpc_router_allowing_host_deploy<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route(
        "/flow-editor/rpc",
        post(flow_editor_rpc_handler_allowing_host_deploy),
    )
}

pub fn flow_editor_rpc_router_with_decisions(decisions: RuntimeDecisionState) -> Router {
    flow_editor_rpc_router_with_runtime_catalog(decisions, None, None)
}

fn flow_editor_rpc_router_with_runtime_catalog(
    decisions: RuntimeDecisionState,
    runtime_catalog: Option<MobpackRuntimeCatalogState>,
    access: Option<AccessController>,
) -> Router {
    Router::new()
        .route("/flow-editor/rpc", post(protected_flow_editor_rpc_handler))
        .with_state(FlowEditorRpcState {
            decisions,
            runtime_catalog,
            access,
        })
}

pub async fn flow_editor_frontend_index_handler() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            // Same policy as the console document: only its own origin
            // (for example a console operator panel) may frame it.
            (header::CONTENT_SECURITY_POLICY, "frame-ancestors 'self'"),
        ],
        FLOW_EDITOR_FRONTEND_INDEX_HTML,
    )
}

pub async fn flow_editor_frontend_vendor_js_handler() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        FLOW_EDITOR_FRONTEND_VENDOR_JS,
    )
}

pub async fn flow_editor_frontend_app_js_handler() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "no-store"),
        ],
        FLOW_EDITOR_FRONTEND_APP_JS,
    )
}

pub async fn flow_editor_frontend_app_css_handler() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        FLOW_EDITOR_FRONTEND_APP_CSS,
    )
}

pub async fn flow_editor_rpc_handler(Json(request): Json<Value>) -> impl IntoResponse {
    flow_editor_rpc_handler_with_policy(request, false).await
}

pub async fn flow_editor_rpc_handler_allowing_host_deploy(
    Json(request): Json<Value>,
) -> impl IntoResponse {
    flow_editor_rpc_handler_with_policy(request, true).await
}

async fn flow_editor_rpc_handler_with_policy(
    request: Value,
    allow_host_deploy: bool,
) -> impl IntoResponse {
    let parsed_request = match serde_json::from_value::<JsonRpcRequest>(request) {
        Ok(req) => req,
        Err(_) => {
            return (
                StatusCode::OK,
                Json::<Value>(serde_json::json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": Value::Null,
                    "error": { "code": -32600, "message": "Invalid Request" }
                })),
            );
        }
    };
    let response_id = parsed_request.id.clone().unwrap_or(Value::Null);
    let response = dispatch_flow_editor_rpc_blocking(
        parsed_request,
        FlowEditorAuthReport {
            authenticated: false,
            mode: if allow_host_deploy {
                "standalone_host_deploy"
            } else {
                "none"
            },
            reason: if allow_host_deploy {
                "standalone Flow Editor authoring server with explicit host deploy opt-in"
            } else {
                "standalone Flow Editor authoring server"
            },
            host_mutation_allowed: allow_host_deploy,
            deploy_execute_allowed: allow_host_deploy,
        },
        None,
        response_id,
    )
    .await;
    (StatusCode::OK, Json::<Value>(response))
}

/// Run the synchronous flow-editor RPC dispatcher on the blocking pool.
///
/// `meta/mobpacks/deploy` with `execute: true` (and `validate` via
/// `rkat mob validate`) blocks on a child process for up to the deploy
/// execution timeout; parking that wait on a tokio worker thread would
/// starve the async runtime, so every HTTP entry point routes through
/// `spawn_blocking` here.
async fn dispatch_flow_editor_rpc_blocking(
    request: JsonRpcRequest,
    auth: FlowEditorAuthReport,
    runtime_catalog: Option<MobpackRuntimeCatalogState>,
    response_id: Value,
) -> Value {
    match tokio::task::spawn_blocking(move || {
        handle_flow_editor_rpc_with_auth(request, auth, runtime_catalog.as_ref())
    })
    .await
    {
        Ok(response) => response,
        Err(err) => serde_json::json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": response_id,
            "error": {
                "code": -32603,
                "message": format!("flow editor rpc task failed: {err}"),
            }
        }),
    }
}

#[derive(Clone)]
struct FlowEditorRpcState {
    decisions: RuntimeDecisionState,
    runtime_catalog: Option<MobpackRuntimeCatalogState>,
    access: Option<AccessController>,
}

#[derive(Clone, Copy)]
struct FlowEditorAuthReport {
    authenticated: bool,
    mode: &'static str,
    reason: &'static str,
    host_mutation_allowed: bool,
    deploy_execute_allowed: bool,
}

async fn protected_flow_editor_rpc_handler(
    State(state): State<FlowEditorRpcState>,
    headers: HeaderMap,
    uri: Uri,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let parsed_request = match serde_json::from_value::<JsonRpcRequest>(request) {
        Ok(req) => req,
        Err(_) => {
            return (
                StatusCode::OK,
                Json::<Value>(serde_json::json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": Value::Null,
                    "error": { "code": -32600, "message": "Invalid Request" }
                })),
            );
        }
    };
    let access_view = match sse_access_context(
        Some(&state.decisions),
        state.access.as_ref(),
        &headers,
        &uri,
    ) {
        Ok(view) => view,
        Err(()) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json::<Value>(serde_json::json!({
                    "jsonrpc": JSONRPC_VERSION,
                    "id": parsed_request.id.unwrap_or(Value::Null),
                    "error": {
                        "code": -32600,
                        "message": "unauthorized: flow editor rpc requires a valid auth token",
                    }
                })),
            );
        }
    };
    if let Some(error) = flow_editor_rpc_access_violation(access_view.as_ref(), &parsed_request) {
        return (
            StatusCode::OK,
            Json::<Value>(serde_json::json!({
                "jsonrpc": JSONRPC_VERSION,
                "id": parsed_request.id.unwrap_or(Value::Null),
                "error": error,
            })),
        );
    }
    // Intersect capability advertisements with the caller's ABAC grants so
    // the editor does not surface deploy affordances the caller can never
    // use; per-call enforcement above remains authoritative.
    let deploy_grant = access_view
        .as_ref()
        .filter(|view| view.enforced())
        .is_none_or(|view| view.may_perform_anywhere(ACTION_MOBPACK_DEPLOY));
    let response_id = parsed_request.id.clone().unwrap_or(Value::Null);
    let response = dispatch_flow_editor_rpc_blocking(
        parsed_request,
        FlowEditorAuthReport {
            authenticated: true,
            mode: "reference_app",
            reason: "reference app Flow Editor authoring server",
            host_mutation_allowed: deploy_grant,
            deploy_execute_allowed: deploy_grant,
        },
        state.runtime_catalog.clone(),
        response_id,
    )
    .await;
    (StatusCode::OK, Json::<Value>(response))
}

/// Map a flow-editor RPC method to the ABAC action it requires. Mirrors the
/// console's `console_rpc_access_requirement`: `meta/capabilities` stays
/// open (it only describes the method surface), every mobpack-authoring
/// method requires `mobpack.author`, and a deploy with `execute: true`
/// requires `mobpack.deploy` instead.
fn flow_editor_rpc_access_requirement(method: &str, params: &Value) -> Option<&'static str> {
    if method == "meta/capabilities" {
        return None;
    }
    if !crate::rpc::MOBPACK_AUTHORING_METHODS.contains(&method) {
        // Unknown methods fall through to the dispatcher's method-not-found.
        return None;
    }
    if method == "meta/mobpacks/deploy"
        && params
            .get("execute")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        return Some(ACTION_MOBPACK_DEPLOY);
    }
    // `validate` with `rkat_validate: true` writes a rendered mobpack archive
    // to a caller-controlled path and spawns `rkat mob validate` — a
    // host-mutating side effect, not a pure authoring read. Gate it behind
    // the same deploy grant as host-executing deploys.
    if method == "meta/mobpacks/validate" && validate_rkat_execution_requested(params) {
        return Some(ACTION_MOBPACK_DEPLOY);
    }
    Some(ACTION_MOBPACK_AUTHOR)
}

/// True when a `meta/mobpacks/validate` request asks to render the pack and
/// run `rkat mob validate` on the host (mirrors
/// `mobpack::validate_with_rkat_requested`). This is the host-mutating arm:
/// it writes a mobpack archive and spawns a child process.
fn validate_rkat_execution_requested(params: &Value) -> bool {
    params
        .get("rkat_validate")
        .or_else(|| params.get("rkatValidate"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn flow_editor_rpc_access_violation(
    view: Option<&AccessView>,
    request: &JsonRpcRequest,
) -> Option<Value> {
    let view = view.filter(|view| view.enforced())?;
    let action = flow_editor_rpc_access_requirement(request.method.as_str(), &request.params)?;
    if view.allows(action) {
        return None;
    }
    Some(serde_json::json!({
        "code": ACCESS_DENIED_RPC_CODE,
        "message": format!("access denied: {action}"),
        "data": { "kind": "access_denied", "action": action },
    }))
}

pub fn handle_flow_editor_rpc(request: JsonRpcRequest) -> Value {
    handle_flow_editor_rpc_with_auth(
        request,
        FlowEditorAuthReport {
            authenticated: false,
            mode: "none",
            reason: "standalone Flow Editor authoring server",
            host_mutation_allowed: false,
            deploy_execute_allowed: false,
        },
        None,
    )
}

fn handle_flow_editor_rpc_with_auth(
    request: JsonRpcRequest,
    auth: FlowEditorAuthReport,
    runtime_catalog: Option<&MobpackRuntimeCatalogState>,
) -> Value {
    let response_id = request.id.clone().unwrap_or(Value::Null);
    match request.method.as_str() {
        "meta/capabilities" => {
            let mut methods = vec!["meta/capabilities"];
            methods.extend_from_slice(crate::rpc::MOBPACK_AUTHORING_METHODS);
            let mut authoring_capabilities = crate::rpc::mobpack_authoring_capabilities();
            authoring_capabilities["host_mutation_allowed"] =
                serde_json::json!(auth.host_mutation_allowed);
            authoring_capabilities["deploy_execute_allowed"] =
                serde_json::json!(auth.deploy_execute_allowed);
            authoring_capabilities["runtime_backed_catalogs"] =
                serde_json::json!(runtime_catalog.is_some());
            response_value(
                response_id,
                Some(serde_json::json!({
                    "methods": methods,
                    "authenticated": auth.authenticated,
                    "auth": {
                        "mode": auth.mode,
                        "reason": auth.reason
                    },
                    "features": {
                        "flow_editor": true,
                        "mobpack_authoring": true,
                    },
                    "authoring_capabilities": authoring_capabilities,
                })),
                None,
            )
        }
        "meta/mobpacks/catalogs" => response_value(
            response_id,
            Some(crate::mobpack::mobpack_catalogs_response_with_runtime(
                runtime_catalog,
            )),
            None,
        ),
        "meta/tools/catalog" => response_value(
            response_id,
            Some(crate::mobpack::mobpack_tools_catalog_response_with_runtime(
                runtime_catalog,
            )),
            None,
        ),
        "meta/skills/catalog" => response_value(
            response_id,
            Some(crate::mobpack::mobpack_skills_catalog_response_with_runtime(
                runtime_catalog,
            )),
            None,
        ),
        "meta/agent_definitions/list" => response_value(
            response_id,
            Some(
                crate::mobpack::mobpack_agent_definitions_response_with_runtime(runtime_catalog),
            ),
            None,
        ),
        "meta/mobpacks/templates" => response_value(
            response_id,
            Some(crate::mobpack::mobpack_templates_response_with_runtime(
                runtime_catalog,
            )),
            None,
        ),
        "meta/mobpacks/deploy"
            if !auth.deploy_execute_allowed && deploy_execute_requested(&request.params) =>
        {
            response_value(
                response_id,
                None,
                Some(JsonRpcError {
                    code: -32602,
                    message: "standalone Flow Editor RPC cannot execute host deploys; use deploy planning or run rkat mob run manually".to_string(),
                    data: Some(serde_json::json!({
                        "method": "meta/mobpacks/deploy",
                        "execute": true,
                        "deploy_command": "rkat mob run"
                    })),
                }),
            )
        }
        // `validate` with `rkat_validate: true` is host-mutating (it writes a
        // rendered mobpack archive to a caller-controlled path and spawns
        // `rkat mob validate`). Without the host grant, strip the flag so the
        // request degrades to pure structural validation instead of touching
        // the host filesystem or spawning a process. Fail-closed: the default
        // standalone surface (no `--allow-host-deploy`) and any caller lacking
        // `mobpack.deploy` can never reach the file-write/exec sink.
        "meta/mobpacks/validate"
            if !auth.host_mutation_allowed
                && validate_rkat_execution_requested(&request.params) =>
        {
            let mut params = request.params.clone();
            if let Some(object) = params.as_object_mut() {
                object.remove("rkat_validate");
                object.remove("rkatValidate");
            }
            match crate::rpc::handle_mobpack_authoring_rpc_with_runtime(
                "meta/mobpacks/validate",
                &params,
                response_id.clone(),
                runtime_catalog,
            ) {
                Some(response) => serde_json::to_value(response).unwrap_or_else(|_| {
                    serde_json::json!({
                        "jsonrpc": JSONRPC_VERSION,
                        "id": Value::Null,
                        "error": { "code": -32603, "message": "serialization failed" }
                    })
                }),
                None => response_value(
                    response_id,
                    None,
                    Some(JsonRpcError {
                        code: -32601,
                        message: "method not found on flow editor rpc: meta/mobpacks/validate"
                            .to_string(),
                        data: None,
                    }),
                ),
            }
        }
        method if crate::rpc::MOBPACK_AUTHORING_METHODS.contains(&method) => {
            match crate::rpc::handle_mobpack_authoring_rpc_with_runtime(
                method,
                &request.params,
                response_id.clone(),
                runtime_catalog,
            ) {
                Some(response) => serde_json::to_value(response).unwrap_or_else(|_| {
                    serde_json::json!({
                        "jsonrpc": JSONRPC_VERSION,
                        "id": Value::Null,
                        "error": {
                            "code": -32603,
                            "message": "serialization failed",
                        }
                    })
                }),
                None => response_value(
                    response_id,
                    None,
                    Some(JsonRpcError {
                        code: -32601,
                        message: format!("method not found on flow editor rpc: {method}"),
                        data: None,
                    }),
                ),
            }
        }
        other => response_value(
            response_id,
            None,
            Some(JsonRpcError {
                code: -32601,
                message: format!("method not found on flow editor rpc: {other}"),
                data: None,
            }),
        ),
    }
}

fn deploy_execute_requested(params: &Value) -> bool {
    params
        .get("execute")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn response_value(id: Value, result: Option<Value>, error: Option<JsonRpcError>) -> Value {
    serde_json::to_value(JsonRpcResponse {
        jsonrpc: JSONRPC_VERSION.to_string(),
        id,
        result,
        error,
    })
    .unwrap_or_else(|_| {
        serde_json::json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": Value::Null,
            "error": {
                "code": -32603,
                "message": "serialization failed",
            }
        })
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]
mod tests {
    use axum::{body::Body, http::Request};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::rpc::JsonRpcRequest;

    #[tokio::test]
    async fn flow_editor_document_may_only_be_framed_by_its_own_origin() {
        use tower::ServiceExt;
        for path in ["/flow-editor", "/flow-editor/"] {
            let response = super::flow_editor_frontend_router::<()>()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .body(axum::body::Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), axum::http::StatusCode::OK);
            assert_eq!(
                response
                    .headers()
                    .get(axum::http::header::CONTENT_SECURITY_POLICY)
                    .and_then(|value| value.to_str().ok()),
                Some("frame-ancestors 'self'"),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn standalone_flow_editor_serves_empty_favicon_response() {
        let response = super::flow_editor_router()
            .oneshot(
                Request::builder()
                    .uri("/favicon.ico")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("favicon response");

        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
    }

    fn authoring_access_controller(rules: Vec<crate::access::AccessRule>) -> AccessController {
        AccessController::new(crate::access::AccessControlConfig {
            enabled: true,
            admins: vec!["root@example.test".to_string()],
            groups: std::collections::BTreeMap::new(),
            rules,
            approvers: std::collections::BTreeMap::new(),
        })
        .expect("valid access config")
    }

    fn allow_everyone(id: &str, actions: &[&str]) -> crate::access::AccessRule {
        crate::access::AccessRule {
            id: id.to_string(),
            actions: actions.iter().map(ToString::to_string).collect(),
            ..crate::access::AccessRule::default()
        }
    }

    fn open_decision_state() -> crate::runtime::RuntimeDecisionState {
        crate::runtime::RuntimeDecisionState {
            bigquery: crate::decisions::BigQueryNaming {
                dataset: "flow_editor_dataset".to_string(),
                table: "flow_editor_table".to_string(),
            },
            modules: vec![],
            auth: crate::decisions::AuthPolicy::default(),
            trusted_oidc: crate::runtime::TrustedOidcRuntimeConfig {
                discovery_json: r#"{"issuer":"https://noop.example.com"}"#.to_string(),
                jwks_json: r#"{"keys":[]}"#.to_string(),
                audience: "flow-editor-tests".to_string(),
                require_verified_email: false,
            },
            console: crate::decisions::ConsolePolicy {
                require_app_auth: false,
                ..crate::decisions::ConsolePolicy::default()
            },
            ops: crate::decisions::RuntimeOpsPolicy::default(),
            release_metadata: crate::decisions::ReleaseMetadata {
                targets: vec!["crates.io".to_string()],
                support_matrix: "lts".to_string(),
            },
        }
    }

    async fn protected_rpc_response(access: Option<AccessController>, body: Value) -> Value {
        let router =
            super::flow_editor_rpc_router_with_runtime_catalog(open_decision_state(), None, access);
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/flow-editor/rpc")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("rpc response");
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json body")
    }

    fn rpc_body(method: &str, params: Value) -> Value {
        json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params })
    }

    #[tokio::test]
    async fn access_control_denies_authoring_without_a_mobpack_author_grant() {
        let access = authoring_access_controller(Vec::new());
        let response =
            protected_rpc_response(Some(access), rpc_body("meta/mobpacks/schema", json!({}))).await;
        assert_eq!(response["error"]["code"], json!(ACCESS_DENIED_RPC_CODE));
        assert_eq!(response["error"]["data"]["action"], json!("mobpack.author"));
    }

    #[tokio::test]
    async fn access_control_allows_authoring_with_a_mobpack_author_grant() {
        let access =
            authoring_access_controller(vec![allow_everyone("authors", &["mobpack.author"])]);
        let response =
            protected_rpc_response(Some(access), rpc_body("meta/mobpacks/schema", json!({}))).await;
        assert!(response["error"].is_null(), "{response:#?}");
        assert!(response["result"].is_object(), "{response:#?}");
    }

    #[tokio::test]
    async fn access_control_requires_mobpack_deploy_for_deploy_execute() {
        let access =
            authoring_access_controller(vec![allow_everyone("authors", &["mobpack.author"])]);
        let response = protected_rpc_response(
            Some(access),
            rpc_body("meta/mobpacks/deploy", json!({ "execute": true })),
        )
        .await;
        assert_eq!(response["error"]["code"], json!(ACCESS_DENIED_RPC_CODE));
        assert_eq!(response["error"]["data"]["action"], json!("mobpack.deploy"));
    }

    #[tokio::test]
    async fn access_control_capabilities_intersect_deploy_grants() {
        let access =
            authoring_access_controller(vec![allow_everyone("authors", &["mobpack.author"])]);
        let response =
            protected_rpc_response(Some(access), rpc_body("meta/capabilities", Value::Null)).await;
        let capabilities = &response["result"]["authoring_capabilities"];
        assert_eq!(capabilities["deploy_execute_allowed"], json!(false));
        assert_eq!(capabilities["host_mutation_allowed"], json!(false));

        let access = authoring_access_controller(vec![allow_everyone(
            "operators",
            &["mobpack.author", "mobpack.deploy"],
        )]);
        let response =
            protected_rpc_response(Some(access), rpc_body("meta/capabilities", Value::Null)).await;
        let capabilities = &response["result"]["authoring_capabilities"];
        assert_eq!(capabilities["deploy_execute_allowed"], json!(true));
        assert_eq!(capabilities["host_mutation_allowed"], json!(true));
    }

    #[tokio::test]
    async fn access_control_disabled_leaves_authoring_open() {
        let response =
            protected_rpc_response(None, rpc_body("meta/mobpacks/schema", json!({}))).await;
        assert!(response["error"].is_null(), "{response:#?}");
    }

    use super::AccessController;
    use crate::http_console::ACCESS_DENIED_RPC_CODE;

    #[test]
    fn flow_editor_frontend_router_merges_with_console_frontend_router() {
        // `build_reference_app_router` merges both frontends into one app
        // router; duplicate paths (such as /favicon.ico) make axum panic at
        // startup, so the shared frontend router must stay collision-free.
        let _ = crate::http_console::console_frontend_router()
            .merge(super::flow_editor_frontend_router::<()>());
    }

    #[test]
    fn flow_editor_rpc_exposes_only_mobpack_authoring_methods() {
        let response = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/capabilities".to_string(),
            params: Value::Null,
        });
        let methods = response["result"]["methods"]
            .as_array()
            .expect("methods array")
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        let mut expected_methods = vec!["meta/capabilities"];
        expected_methods.extend_from_slice(crate::rpc::MOBPACK_AUTHORING_METHODS);
        assert_eq!(methods, expected_methods);
        assert!(!methods.contains(&"meta/console/send"));
        assert_eq!(response["result"]["authenticated"], json!(false));
        assert_eq!(response["result"]["auth"]["mode"], json!("none"));
        assert_eq!(
            response["result"]["authoring_capabilities"]["domain"],
            json!("mobpack_authoring")
        );
        assert_eq!(
            response["result"]["authoring_capabilities"]["runtime_mutation"],
            json!(false)
        );
        assert_eq!(
            response["result"]["authoring_capabilities"]["host_mutation_allowed"],
            json!(false)
        );
        assert_eq!(
            response["result"]["authoring_capabilities"]["deploy_execute_allowed"],
            json!(false)
        );
        assert_eq!(
            response["result"]["authoring_capabilities"]["deploy_command"],
            json!("rkat mob run")
        );
        assert_eq!(
            response["result"]["authoring_capabilities"]["methods"]
                .as_array()
                .expect("authoring methods")
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>(),
            crate::rpc::MOBPACK_AUTHORING_METHODS
        );
    }

    #[test]
    fn standalone_flow_editor_rpc_rejects_host_deploy_execution() {
        let catalogs = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/mobpacks/catalogs".to_string(),
            params: Value::Null,
        });
        assert_eq!(catalogs["result"]["runtime_backed"], json!(false));
        assert_eq!(
            catalogs["result"]["authoring_provider"]["runtime_binding"],
            json!("unbound")
        );
        let sample = catalogs["result"]["sample_mobpacks"]
            .as_array()
            .expect("sample mobpacks")
            .iter()
            .find(|sample| sample["id"] == "sample_docs_only")
            .expect("docs sample");

        let response = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(2)),
            method: "meta/mobpacks/deploy".to_string(),
            params: json!({
                "document": sample["document"].clone(),
                "prompt": "Reply with exactly OK.",
                "execute": true
            }),
        });

        assert_eq!(response["error"]["code"], json!(-32602));
        assert!(
            response["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("cannot execute host deploys")),
            "{response:#?}"
        );
        assert_eq!(
            response["error"]["data"]["deploy_command"],
            json!("rkat mob run")
        );
    }

    #[test]
    fn protected_flow_editor_rpc_returns_runtime_bound_catalogs_when_state_is_available() {
        let response = super::handle_flow_editor_rpc_with_auth(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                id: Some(json!(1)),
                method: "meta/mobpacks/catalogs".to_string(),
                params: Value::Null,
            },
            super::FlowEditorAuthReport {
                authenticated: true,
                mode: "reference_app",
                reason: "reference app Flow Editor authoring server",
                host_mutation_allowed: true,
                deploy_execute_allowed: true,
            },
            Some(&crate::mobpack::MobpackRuntimeCatalogState {
                loaded_modules: vec!["worker".to_string()],
                runtime_methods: vec!["meta/mobpacks/deploy".to_string()],
                has_contact_directory: true,
                has_peer_mob_handles: false,
                has_inproc_contacts: false,
                runtime_flow_rows: Vec::new(),
                runtime_agent_definition_sources: Vec::new(),
                runtime_skill_realms: Vec::new(),
            }),
        );

        assert!(response["error"].is_null(), "{response:#?}");
        assert_eq!(response["result"]["runtime_backed"], json!(true));
        assert_eq!(
            response["result"]["authoring_provider"]["id"],
            json!("unified_runtime")
        );
        assert_eq!(
            response["result"]["authoring_provider"]["runtime_binding"],
            json!("bound")
        );
        assert_eq!(
            response["result"]["authoring_provider"]["loaded_modules"],
            json!(["worker"])
        );
        assert_eq!(
            response["result"]["authoring_provider"]["cross_mob"]["contact_directory"],
            json!(true)
        );
        assert_eq!(
            response["result"]["catalog_snapshot"]["runtime_backed"],
            json!(true)
        );
    }

    #[cfg(unix)]
    #[test]
    fn standalone_flow_editor_rpc_executes_host_deploy_when_explicitly_enabled() {
        let catalogs = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/mobpacks/catalogs".to_string(),
            params: Value::Null,
        });
        let sample = catalogs["result"]["sample_mobpacks"]
            .as_array()
            .expect("sample mobpacks")
            .iter()
            .find(|sample| sample["id"] == "sample_docs_only")
            .expect("docs sample");
        let dir = tempfile::tempdir().expect("tempdir");
        let fake_rkat = dir.path().join("rkat");
        let args_file = dir.path().join("rkat.args");
        std::fs::write(
            &fake_rkat,
            format!(
                "#!/bin/sh\n\
                 printf '%s\\n' \"$@\" > {}\n\
                 echo flow-editor-rkat-ok\n\
                 printf 'run\\tmob=docs\\tflow=main\\trun_id=run-1\\tstatus=completed\\n'\n\
                 printf 'result\\t{{\"reply\":\"OK\"}}\\n'\n",
                args_file.to_string_lossy()
            ),
        )
        .expect("write fake rkat");
        let mut permissions = std::fs::metadata(&fake_rkat)
            .expect("fake rkat metadata")
            .permissions();
        use std::os::unix::fs::PermissionsExt;
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_rkat, permissions).expect("chmod fake rkat");

        let capabilities = super::handle_flow_editor_rpc_with_auth(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                id: Some(json!(2)),
                method: "meta/capabilities".to_string(),
                params: Value::Null,
            },
            super::FlowEditorAuthReport {
                authenticated: false,
                mode: "standalone_host_deploy",
                reason: "standalone Flow Editor authoring server with explicit host deploy opt-in",
                host_mutation_allowed: true,
                deploy_execute_allowed: true,
            },
            None,
        );
        assert_eq!(
            capabilities["result"]["authoring_capabilities"]["host_mutation_allowed"],
            json!(true)
        );
        assert_eq!(
            capabilities["result"]["authoring_capabilities"]["deploy_execute_allowed"],
            json!(true)
        );

        let response = super::handle_flow_editor_rpc_with_auth(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                id: Some(json!(3)),
                method: "meta/mobpacks/deploy".to_string(),
                params: json!({
                    "document": sample["document"].clone(),
                    "output_dir": dir.path(),
                    "prompt": "Reply with exactly OK.",
                    "rkat_bin": fake_rkat,
                    "execute": true
                }),
            },
            super::FlowEditorAuthReport {
                authenticated: false,
                mode: "standalone_host_deploy",
                reason: "standalone Flow Editor authoring server with explicit host deploy opt-in",
                host_mutation_allowed: true,
                deploy_execute_allowed: true,
            },
            None,
        );

        assert!(response["error"].is_null(), "{response:#?}");
        assert_eq!(response["result"]["executed"], json!(true));
        assert_eq!(response["result"]["success"], json!(true));
        assert_eq!(response["result"]["status_code"], json!(0));
        assert!(
            response["result"]["stdout"]
                .as_str()
                .is_some_and(|stdout| stdout.contains("flow-editor-rkat-ok")),
            "{response:#?}"
        );
        let argv = std::fs::read_to_string(args_file).expect("recorded fake rkat args");
        assert!(argv.lines().any(|line| line == "mob"));
        assert!(argv.lines().any(|line| line == "run"));
        // Single-token hyphen-safe `--prompt=<text>` form.
        assert!(
            argv.lines()
                .any(|line| line == "--prompt=Reply with exactly OK.")
        );
    }

    /// Security regression: `meta/mobpacks/validate` with
    /// `rkat_validate: true` writes a rendered mobpack archive to a
    /// caller-controlled path and spawns `rkat mob validate`. On the default
    /// standalone surface (no `--allow-host-deploy`, `host_mutation_allowed`
    /// false) an unauthenticated client must NOT reach that file-write/exec
    /// sink: the request degrades to structural-only validation, leaving no
    /// archive on disk and spawning no process.
    #[cfg(unix)]
    #[test]
    fn standalone_flow_editor_validate_cannot_write_files_or_spawn_rkat_without_host_grant() {
        let catalogs = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/mobpacks/catalogs".to_string(),
            params: Value::Null,
        });
        let sample = catalogs["result"]["sample_mobpacks"]
            .as_array()
            .expect("sample mobpacks")
            .iter()
            .find(|sample| sample["id"] == "sample_docs_only")
            .expect("docs sample");

        let dir = tempfile::tempdir().expect("tempdir");
        // A `rkat` that records the fact it ran — it must never be invoked.
        let fake_rkat = dir.path().join("rkat");
        let spawn_marker = dir.path().join("rkat.ran");
        std::fs::write(
            &fake_rkat,
            format!(
                "#!/bin/sh\ntouch {}\necho should-not-run\n",
                spawn_marker.to_string_lossy()
            ),
        )
        .expect("write fake rkat");
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&fake_rkat)
            .expect("fake rkat metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_rkat, permissions).expect("chmod fake rkat");

        // Attacker-controlled absolute pack path: it must never be written.
        let attacker_pack_path = dir.path().join("attacker-controlled.mobpack");

        let response = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(2)),
            method: "meta/mobpacks/validate".to_string(),
            params: json!({
                "document": sample["document"].clone(),
                "rkat_validate": true,
                "rkat_bin": fake_rkat,
                "validation_pack_path": attacker_pack_path,
            }),
        });

        // Structural validation still runs and succeeds …
        assert!(response["error"].is_null(), "{response:#?}");
        assert_eq!(response["result"]["ok"], json!(true), "{response:#?}");
        // … but it did NOT escalate to the host-executing rkat path.
        assert_ne!(
            response["result"]["validation_source"],
            json!("rkat mob validate"),
            "rkat validate must not run without the host grant: {response:#?}"
        );
        assert!(
            !attacker_pack_path.exists(),
            "validate must not write a caller-controlled archive without the host grant"
        );
        assert!(
            !spawn_marker.exists(),
            "validate must not spawn rkat without the host grant"
        );
    }

    /// The ABAC access requirement classifies validate-with-rkat as a
    /// host-mutating operation: it requires `mobpack.deploy`, not just
    /// `mobpack.author`. A pure structural validate stays an authoring read.
    #[tokio::test]
    async fn access_control_requires_mobpack_deploy_for_validate_with_rkat() {
        let access =
            authoring_access_controller(vec![allow_everyone("authors", &["mobpack.author"])]);
        let response = protected_rpc_response(
            Some(access),
            rpc_body(
                "meta/mobpacks/validate",
                json!({ "document": json!({}), "rkat_validate": true }),
            ),
        )
        .await;
        assert_eq!(response["error"]["code"], json!(ACCESS_DENIED_RPC_CODE));
        assert_eq!(response["error"]["data"]["action"], json!("mobpack.deploy"));

        // A structural-only validate is authoring, not deploy: the author
        // grant is sufficient (no access-denied).
        let access =
            authoring_access_controller(vec![allow_everyone("authors", &["mobpack.author"])]);
        let response = protected_rpc_response(
            Some(access),
            rpc_body("meta/mobpacks/validate", json!({ "document": json!({}) })),
        )
        .await;
        assert_ne!(
            response["error"]["code"],
            json!(ACCESS_DENIED_RPC_CODE),
            "structural validate must remain an authoring read: {response:#?}"
        );
    }

    #[test]
    fn flow_editor_rpc_plans_real_sample_mobpack_deploy() {
        let catalogs = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/mobpacks/catalogs".to_string(),
            params: Value::Null,
        });
        let sample = catalogs["result"]["sample_mobpacks"]
            .as_array()
            .expect("sample mobpacks")
            .iter()
            .find(|sample| sample["id"] == "sample_docs_only")
            .expect("docs sample");

        let response = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(2)),
            method: "meta/mobpacks/deploy".to_string(),
            params: json!({
                "document": sample["document"].clone(),
                "prompt": "Reply with exactly OK."
            }),
        });

        assert!(response["error"].is_null(), "{response:#?}");
        assert_eq!(
            &response["result"]["argv"].as_array().expect("argv")[0..3],
            [json!("rkat"), json!("mob"), json!("run")]
        );
        assert!(
            response["result"]["plan_trace"]
                .as_array()
                .expect("plan trace")
                .iter()
                .any(|row| row["head"]
                    .as_str()
                    .is_some_and(|head| head.starts_with("PROFILE ·"))),
            "{response:#?}"
        );
    }

    #[test]
    fn flow_editor_rpc_previews_document_backed_deploy_command() {
        let catalogs = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/mobpacks/catalogs".to_string(),
            params: Value::Null,
        });
        let sample = catalogs["result"]["sample_mobpacks"]
            .as_array()
            .expect("sample mobpacks")
            .iter()
            .find(|sample| sample["id"] == "sample_docs_only")
            .expect("docs sample");
        let response = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(2)),
            method: "meta/mobpacks/deploy_command".to_string(),
            params: json!({
                "document": sample["document"].clone(),
                "prompt": "Preview prompt."
            }),
        });

        assert!(response["error"].is_null(), "{response:#?}");
        assert_eq!(
            &response["result"]["argv"].as_array().expect("argv")[0..3],
            [json!("rkat"), json!("mob"), json!("run")]
        );
        assert_eq!(
            response["result"]["source"],
            json!("meerkat_meta::mobpack::deploy_argv")
        );
        assert_eq!(response["result"]["filename"], json!("docs-only.mobpack"));
        assert_eq!(response["result"]["validation"]["ok"], json!(true));
        assert!(
            response["result"]["command"]
                .as_str()
                .is_some_and(|command| command.contains("docs-only.mobpack")
                    && command.contains("Preview prompt.")),
            "{response:#?}"
        );

        let rejected = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(3)),
            method: "meta/mobpacks/deploy_command".to_string(),
            params: json!({
                "deploy": { "command": "rkat mob run" },
                "pack_path": "<pack.mobpack>"
            }),
        });
        assert!(
            rejected["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("requires document")),
            "{rejected:#?}"
        );
    }

    #[test]
    fn flow_editor_rpc_previews_source_without_exporting_archive_payload() {
        let catalogs = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(1)),
            method: "meta/mobpacks/catalogs".to_string(),
            params: Value::Null,
        });
        let sample = catalogs["result"]["sample_mobpacks"]
            .as_array()
            .expect("sample mobpacks")
            .iter()
            .find(|sample| sample["id"] == "sample_docs_only")
            .expect("docs sample");

        let response = super::handle_flow_editor_rpc(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(json!(2)),
            method: "meta/mobpacks/source".to_string(),
            params: json!({ "document": sample["document"].clone() }),
        });

        assert!(response["error"].is_null(), "{response:#?}");
        assert_eq!(response["result"]["source"], json!("meta/mobpacks/source"));
        assert!(
            response["result"].get("content_base64").is_none(),
            "{response:#?}"
        );
        let source_files = response["result"]["source_files"]
            .as_array()
            .expect("source files");
        assert!(
            source_files
                .iter()
                .any(|file| file["path"] == "meta/mob.toml"
                    && file["text"]
                        .as_str()
                        .is_some_and(|text| text.contains("[mob]"))),
            "{response:#?}"
        );
    }
}
