//! Console-facing contract types for the planned identity-native operator workbench.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const IDENTITY_STREAM_NAME: &str = "identity";
pub const ALL_EVENTS_STREAM_NAME: &str = "all_events";
pub const SYSTEM_EVENT_IDENTITY: &str = "_system";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleIdentityEventEnvelope {
    pub event_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_id: Option<String>,
    pub identity: String,
    pub event_type: String,
    pub timestamp_ms: u64,
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayUnavailableError {
    pub error: String,
    pub stream: String,
    pub requested_last_event_id: String,
    pub latest_event_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsoleInteractionRejectedError {
    pub code: i64,
    pub message: String,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn console_identity_event_envelope_roundtrips_with_interaction_id() {
        let original = ConsoleIdentityEventEnvelope {
            event_id: "evt-1".to_string(),
            interaction_id: Some("turn-123".to_string()),
            identity: "identity:luka".to_string(),
            event_type: "interaction_complete".to_string(),
            timestamp_ms: 1_717_171_717,
            data: serde_json::json!({
                "status": "ok",
                "tool_call_id": "tool-1"
            }),
        };

        let encoded = serde_json::to_value(&original).expect("envelope should serialize");
        let decoded: ConsoleIdentityEventEnvelope =
            serde_json::from_value(encoded).expect("envelope should deserialize");

        assert_eq!(decoded, original);
    }

    #[test]
    fn console_identity_event_envelope_roundtrips_without_interaction_id() {
        let original = ConsoleIdentityEventEnvelope {
            event_id: "evt-2".to_string(),
            interaction_id: None,
            identity: "identity:luka".to_string(),
            event_type: "lease_updated".to_string(),
            timestamp_ms: 1_717_171_718,
            data: serde_json::json!({
                "state": "healthy"
            }),
        };

        let encoded = serde_json::to_value(&original).expect("envelope should serialize");
        let decoded: ConsoleIdentityEventEnvelope =
            serde_json::from_value(encoded).expect("envelope should deserialize");

        assert_eq!(decoded, original);
    }

    #[test]
    fn replay_unavailable_error_roundtrips_through_json() {
        let original = ReplayUnavailableError {
            error: "replay_unavailable".to_string(),
            stream: "identity".to_string(),
            requested_last_event_id: "evt-1".to_string(),
            latest_event_id: "evt-9".to_string(),
        };

        let encoded = serde_json::to_value(&original).expect("replay error should serialize");
        let decoded: ReplayUnavailableError =
            serde_json::from_value(encoded).expect("replay error should deserialize");

        assert_eq!(decoded, original);
    }

    #[test]
    fn console_interaction_rejected_error_roundtrips_through_json() {
        let original = ConsoleInteractionRejectedError {
            code: -32001,
            message: "unknown identity".to_string(),
        };

        let encoded = serde_json::to_value(&original).expect("rejection error should serialize");
        let decoded: ConsoleInteractionRejectedError =
            serde_json::from_value(encoded).expect("rejection error should deserialize");

        assert_eq!(decoded, original);
    }
}
