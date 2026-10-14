#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendState {
    Requested,
    AcceptedPersisted,
    Dispatching,
    Delivered,
    DeliveryFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendTransition {
    PersistAccepted,
    StartDispatch,
    MarkDelivered,
    MarkDeliveryFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendEffect {
    AppendFrame,
    UpdateFrameStatus,
    EmitFrame,
    DispatchSend,
}

impl SendState {
    pub fn apply(
        self,
        transition: SendTransition,
    ) -> Result<(Self, Vec<SendEffect>), &'static str> {
        match (self, transition) {
            (Self::Requested, SendTransition::PersistAccepted) => Ok((
                Self::AcceptedPersisted,
                vec![SendEffect::AppendFrame, SendEffect::EmitFrame],
            )),
            (Self::AcceptedPersisted, SendTransition::StartDispatch) => Ok((
                Self::Dispatching,
                vec![SendEffect::UpdateFrameStatus, SendEffect::DispatchSend],
            )),
            (Self::Dispatching, SendTransition::MarkDelivered) => Ok((
                Self::Delivered,
                vec![SendEffect::UpdateFrameStatus, SendEffect::EmitFrame],
            )),
            (Self::Dispatching, SendTransition::MarkDeliveryFailed) => Ok((
                Self::DeliveryFailed,
                vec![SendEffect::UpdateFrameStatus, SendEffect::EmitFrame],
            )),
            _ => Err("illegal send state transition"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceIngestionState {
    Registered,
    Backfilling,
    CaughtUp,
    Live,
    GapDetected,
    ReplayUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceIngestionTransition {
    StartBackfill,
    BackfillComplete,
    StartLive,
    DetectGap,
    MarkReplayUnavailable,
    Recover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceIngestionEffect {
    QuerySourceHistory,
    AppendFrame,
    SubscribeLive,
    MarkReplayUnavailable,
}

impl SourceIngestionState {
    pub fn apply(
        self,
        transition: SourceIngestionTransition,
    ) -> Result<(Self, Vec<SourceIngestionEffect>), &'static str> {
        match (self, transition) {
            (Self::Registered, SourceIngestionTransition::StartBackfill) => Ok((
                Self::Backfilling,
                vec![SourceIngestionEffect::QuerySourceHistory],
            )),
            (Self::Backfilling, SourceIngestionTransition::BackfillComplete) => {
                Ok((Self::CaughtUp, vec![SourceIngestionEffect::AppendFrame]))
            }
            (Self::CaughtUp, SourceIngestionTransition::StartLive) => {
                Ok((Self::Live, vec![SourceIngestionEffect::SubscribeLive]))
            }
            (Self::Live, SourceIngestionTransition::DetectGap) => {
                Ok((Self::GapDetected, Vec::new()))
            }
            (Self::GapDetected, SourceIngestionTransition::Recover) => Ok((
                Self::Backfilling,
                vec![SourceIngestionEffect::QuerySourceHistory],
            )),
            (Self::GapDetected, SourceIngestionTransition::MarkReplayUnavailable) => Ok((
                Self::ReplayUnavailable,
                vec![SourceIngestionEffect::MarkReplayUnavailable],
            )),
            (Self::ReplayUnavailable, SourceIngestionTransition::Recover) => Ok((
                Self::Backfilling,
                vec![SourceIngestionEffect::QuerySourceHistory],
            )),
            _ => Err("illegal source ingestion state transition"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySubscriptionState {
    Snapshotting,
    Live,
    Lagged,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySubscriptionTransition {
    SnapshotComplete,
    MarkLagged,
    RecoverLive,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaySubscriptionEffect {
    EmitSnapshotStarted,
    EmitReplayFrames,
    EmitSnapshotComplete,
    EmitLagged,
    CloseStream,
}

impl ReplaySubscriptionState {
    pub fn apply(
        self,
        transition: ReplaySubscriptionTransition,
    ) -> Result<(Self, Vec<ReplaySubscriptionEffect>), &'static str> {
        match (self, transition) {
            (Self::Snapshotting, ReplaySubscriptionTransition::SnapshotComplete) => Ok((
                Self::Live,
                vec![
                    ReplaySubscriptionEffect::EmitSnapshotStarted,
                    ReplaySubscriptionEffect::EmitReplayFrames,
                    ReplaySubscriptionEffect::EmitSnapshotComplete,
                ],
            )),
            (Self::Live, ReplaySubscriptionTransition::MarkLagged) => {
                Ok((Self::Lagged, vec![ReplaySubscriptionEffect::EmitLagged]))
            }
            (Self::Lagged, ReplaySubscriptionTransition::RecoverLive) => {
                Ok((Self::Live, Vec::new()))
            }
            (_, ReplaySubscriptionTransition::Close) if self != Self::Closed => {
                Ok((Self::Closed, vec![ReplaySubscriptionEffect::CloseStream]))
            }
            _ => Err("illegal replay subscription state transition"),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn send_reducer_allows_happy_path() {
        let (state, effects) = SendState::Requested
            .apply(SendTransition::PersistAccepted)
            .expect("requested -> accepted should be legal");
        assert_eq!(state, SendState::AcceptedPersisted);
        assert!(effects.contains(&SendEffect::AppendFrame));

        let (state, effects) = state
            .apply(SendTransition::StartDispatch)
            .expect("accepted -> dispatching should be legal");
        assert_eq!(state, SendState::Dispatching);
        assert!(effects.contains(&SendEffect::DispatchSend));

        let (state, effects) = state
            .apply(SendTransition::MarkDelivered)
            .expect("dispatching -> delivered should be legal");
        assert_eq!(state, SendState::Delivered);
        assert!(effects.contains(&SendEffect::UpdateFrameStatus));
    }

    #[test]
    fn send_reducer_rejects_terminal_to_dispatch() {
        assert_eq!(
            SendState::Delivered.apply(SendTransition::StartDispatch),
            Err("illegal send state transition")
        );
    }

    #[test]
    fn source_ingestion_reducer_models_backfill_to_live() {
        let (state, _) = SourceIngestionState::Registered
            .apply(SourceIngestionTransition::StartBackfill)
            .expect("registered -> backfilling");
        let (state, _) = state
            .apply(SourceIngestionTransition::BackfillComplete)
            .expect("backfilling -> caught up");
        let (state, effects) = state
            .apply(SourceIngestionTransition::StartLive)
            .expect("caught up -> live");
        assert_eq!(state, SourceIngestionState::Live);
        assert!(effects.contains(&SourceIngestionEffect::SubscribeLive));
    }

    #[test]
    fn replay_subscription_reducer_emits_snapshot_boundary() {
        let (state, effects) = ReplaySubscriptionState::Snapshotting
            .apply(ReplaySubscriptionTransition::SnapshotComplete)
            .expect("snapshotting -> live");
        assert_eq!(state, ReplaySubscriptionState::Live);
        assert_eq!(
            effects,
            vec![
                ReplaySubscriptionEffect::EmitSnapshotStarted,
                ReplaySubscriptionEffect::EmitReplayFrames,
                ReplaySubscriptionEffect::EmitSnapshotComplete,
            ]
        );
    }
}
