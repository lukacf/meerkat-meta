export const CONSOLE_CONTRACT_VERSION = "0.5.0";

export const CONSOLE_REST_PATHS = {
  experience: "/console/experience",
  modules: "/console/modules",
  identities: "/console/identities",
  timeline: "/console/timeline",
  timelineStream: "/console/timeline/stream",
  identityTimelineStreamTemplate: "/console/identity/{identity}/stream",
  legacySend: "/console/send",
  mcpAppsResolve: "/console/mcp-apps/resolve",
  mcpAppsReadResource: "/console/mcp-apps/read-resource",
  mcpAppsCallTool: "/console/mcp-apps/call-tool",
} as const;

export const CONSOLE_RPC_PATHS = {
  jsonRpc: "/console/rpc",
  multipartJsonRpc: "/console/rpc/multipart",
} as const;

export const CONSOLE_RPC_METHODS = {
  capabilities: "mobkit/capabilities",
  send: "mobkit/console/send",
  listIdentities: "mobkit/console/list_identities",
  inspectIdentity: "mobkit/console/inspect_identity",
  queryTimeline: "mobkit/console/query_timeline",
  blobUpload: "mobkit/blob/upload",
  retireIdentity: "mobkit/retire",
  respawnIdentity: "mobkit/respawn",
  resetIdentity: "mobkit/reset",
  stopMemberRun: "mobkit/stop_member_run",
  routingRoutesList: "mobkit/routing/routes/list",
  deliveryHistory: "mobkit/delivery/history",
  gatingPending: "mobkit/gating/pending",
  gatingAudit: "mobkit/gating/audit",
  gatingDecide: "mobkit/gating/decide",
  accessStatus: "mobkit/access/status",
  accessGet: "mobkit/access/get",
  accessSet: "mobkit/access/set",
  accessEnable: "mobkit/access/enable",
  accessRuleUpsert: "mobkit/access/rules/upsert",
  accessRuleDelete: "mobkit/access/rules/delete",
  accessGroupSet: "mobkit/access/groups/set",
  accessGroupDelete: "mobkit/access/groups/delete",
  accessPreview: "mobkit/access/preview",
  memoryPanelRecords: "mobkit/memory/panel/records",
  memoryPanelRecord: "mobkit/memory/panel/record",
  memoryPanelQuarantine: "mobkit/memory/panel/quarantine",
  memoryPanelDreams: "mobkit/memory/panel/dreams",
  memoryPanelOverview: "mobkit/memory/panel/overview",
  memoryPanelProposals: "mobkit/memory/panel/proposals",
  memoryPanelInjections: "mobkit/memory/panel/injections",
  memoryPanelHarvests: "mobkit/memory/panel/harvests",
  memoryPanelDreamRuns: "mobkit/memory/panel/dream_runs",
  memoryPanelAuditVerdicts: "mobkit/memory/panel/audit_verdicts",
  memoryQuarantineDecide: "mobkit/memory/quarantine/decide",
  workgraphSnapshot: "mobkit/workgraph/snapshot",
  workgraphEvents: "mobkit/workgraph/events",
  workgraphGet: "mobkit/workgraph/get",
  workgraphGoalStatus: "mobkit/workgraph/goal/status",
  workgraphClaim: "mobkit/workgraph/claim",
  workgraphRelease: "mobkit/workgraph/release",
  workgraphClose: "mobkit/workgraph/close",
  workgraphGoalConfirm: "mobkit/workgraph/goal/confirm",
  workgraphGoalRequestClose: "mobkit/workgraph/goal/request_close",
  workgraphAttentionPause: "mobkit/workgraph/attention/pause",
  workgraphAttentionResume: "mobkit/workgraph/attention/resume",
  workgraphAttentionReassign: "mobkit/workgraph/attention/reassign",
  topologyQuery: "mobkit/topology/query",
  topologyPlan: "mobkit/topology/plan",
  topologyApply: "mobkit/topology/apply",
  topologyOperationGet: "mobkit/topology/operation/get",
  topologyAuditQuery: "mobkit/topology/audit/query",
} as const;

export const CONSOLE_BLOB_PATH_PREFIX = "/blobs/";

export const CONSOLE_TIMELINE_QUERY_MODES = ["since", "recent"] as const;

export const CONSOLE_TIMELINE_REPLAY_UNAVAILABLE_CODE = -32013;
