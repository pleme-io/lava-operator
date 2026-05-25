//! Bridge between the controller's typed surfaces and lava-anomaly's
//! routing primitives. Translates DriftScanOutcome / synth errors /
//! apply errors into typed [`LavaAnomaly`]s, then routes them through
//! the configured [`AnomalyRouter`].
//!
//! Solid abstraction: the controller never builds anomalies by hand;
//! it calls one of the typed `emit_*` functions and gets back a
//! typed [`RoutingDecision`] + a payload-ready chain entry.

use indexmap::IndexMap;
use lava_anomaly::{
    AnomalyKind, AnomalyPayload, AnomalyRouter, LavaAnomaly, RemediationPolicy, RoutingDecision,
    RouterError,
};
use lava_drift::Severity;
use lava_outcome_chain::ResourceAddress;

use crate::drift::DriftScanOutcome;

/// Build + route an anomaly from a drift-scan outcome. Returns the
/// typed decision + the chain payload the caller persists.
///
/// # Errors
/// Surfaces [`RouterError`] from the router, or `serde_json::Error`
/// if the anomaly's content hash fails to encode.
pub fn from_drift<R: AnomalyRouter>(
    router: &R,
    policy: &RemediationPolicy,
    source: ResourceAddress,
    scan: &DriftScanOutcome,
) -> Result<(RoutingDecision, AnomalyPayload), AnomalyBridgeError> {
    let severity = scan.report.max_severity.unwrap_or(Severity::Cosmetic);
    let mut metadata: IndexMap<String, String> = IndexMap::new();
    metadata.insert("drifted_fields".into(), scan.report.count().to_string());
    metadata.insert("spec_hash".into(), scan.report.spec_hash.hex());
    let anomaly = LavaAnomaly::new(
        AnomalyKind::DriftDetected,
        severity,
        source,
        format!("drift detected: {} field(s)", scan.report.count()),
    );
    let anomaly = metadata
        .into_iter()
        .fold(anomaly, |a, (k, v)| a.with_metadata(k, v));
    let decision = router.route(&anomaly, policy)?;
    let payload = AnomalyPayload::from(anomaly, decision.clone())
        .map_err(|e| AnomalyBridgeError::Encode(e.to_string()))?;
    Ok((decision, payload))
}

/// Build + route an anomaly from a synthesize failure.
///
/// # Errors
/// See [`from_drift`].
pub fn from_synth_error<R: AnomalyRouter>(
    router: &R,
    policy: &RemediationPolicy,
    source: ResourceAddress,
    message: impl Into<String>,
) -> Result<(RoutingDecision, AnomalyPayload), AnomalyBridgeError> {
    let anomaly = LavaAnomaly::new(
        AnomalyKind::SynthesisFailure,
        Severity::Functional,
        source,
        message,
    );
    let decision = router.route(&anomaly, policy)?;
    let payload = AnomalyPayload::from(anomaly, decision.clone())
        .map_err(|e| AnomalyBridgeError::Encode(e.to_string()))?;
    Ok((decision, payload))
}

/// Build + route an anomaly from an apply failure.
///
/// # Errors
/// See [`from_drift`].
pub fn from_apply_error<R: AnomalyRouter>(
    router: &R,
    policy: &RemediationPolicy,
    source: ResourceAddress,
    message: impl Into<String>,
) -> Result<(RoutingDecision, AnomalyPayload), AnomalyBridgeError> {
    let anomaly = LavaAnomaly::new(
        AnomalyKind::ApplyFailure,
        Severity::Critical,
        source,
        message,
    );
    let decision = router.route(&anomaly, policy)?;
    let payload = AnomalyPayload::from(anomaly, decision.clone())
        .map_err(|e| AnomalyBridgeError::Encode(e.to_string()))?;
    Ok((decision, payload))
}

#[derive(Debug, thiserror::Error)]
pub enum AnomalyBridgeError {
    #[error("router: {0}")]
    Router(#[from] RouterError),
    #[error("encode: {0}")]
    Encode(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use lava_anomaly::{PolicyRouter, RemediationAction};
    use lava_drift::{ChangeKind, DriftDetector, DriftFinding, MockPlanner};

    fn finding(kind: ChangeKind, attr: &str) -> DriftFinding {
        DriftFinding {
            address: "aws_vpc.main".into(),
            attribute: attr.into(),
            change: kind,
            observed: None,
            declared: None,
        }
    }

    #[test]
    fn from_drift_routes_functional_to_autocorrect_by_default() {
        let detector = DriftDetector::new(MockPlanner::new(vec![finding(
            ChangeKind::Update,
            "cidr_block",
        )]));
        let scan = crate::drift::scan_for_drift(
            &detector,
            &crate::Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        let (decision, payload) = from_drift(
            &PolicyRouter,
            &RemediationPolicy::default(),
            ResourceAddress::new("rio", "infra", "x"),
            &scan,
        )
        .unwrap();
        assert_eq!(decision.action, RemediationAction::AutoCorrect);
        assert_eq!(payload.anomaly.kind, AnomalyKind::DriftDetected);
        assert_eq!(payload.anomaly.severity, Severity::Functional);
        assert!(payload.anomaly.metadata.contains_key("drifted_fields"));
    }

    #[test]
    fn from_drift_routes_critical_to_require_approval_by_default() {
        let detector = DriftDetector::new(MockPlanner::new(vec![finding(
            ChangeKind::Delete,
            "*",
        )]));
        let scan = crate::drift::scan_for_drift(
            &detector,
            &crate::Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        let (decision, _) = from_drift(
            &PolicyRouter,
            &RemediationPolicy::default(),
            ResourceAddress::new("rio", "infra", "x"),
            &scan,
        )
        .unwrap();
        assert_eq!(decision.action, RemediationAction::RequireApproval);
    }

    #[test]
    fn from_synth_error_routes_functional_to_autocorrect() {
        let (decision, payload) = from_synth_error(
            &PolicyRouter,
            &RemediationPolicy::default(),
            ResourceAddress::new("c", "n", "x"),
            "boom",
        )
        .unwrap();
        assert_eq!(decision.action, RemediationAction::AutoCorrect);
        assert_eq!(payload.anomaly.kind, AnomalyKind::SynthesisFailure);
    }

    #[test]
    fn from_apply_error_routes_critical_to_require_approval() {
        let (decision, payload) = from_apply_error(
            &PolicyRouter,
            &RemediationPolicy::default(),
            ResourceAddress::new("c", "n", "x"),
            "provider denied",
        )
        .unwrap();
        assert_eq!(decision.action, RemediationAction::RequireApproval);
        assert_eq!(payload.anomaly.kind, AnomalyKind::ApplyFailure);
    }
}
