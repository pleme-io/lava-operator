//! Drift integration — wires `lava_drift::DriftDetector` into the
//! controller's reconcile path.
//!
//! Solid abstraction: the controller calls [`scan_for_drift`] with
//! the typed Source + bindings + a `PlannerBackend` impl; this
//! module decides whether the result moves the phase to `Drifted`,
//! `Reconverging`, or stays `Applied`.

use indexmap::IndexMap;
use lava_drift::{DriftDetector, DriftReport, PlannerBackend, PlannerError, Severity};

use crate::{Condition, Phase, ReconcileOutcome, Source};

/// Run the drift detector + map the resulting [`DriftReport`] to a
/// typed reconcile outcome. Used by the periodic-poll path inside
/// the controller.
///
/// # Errors
/// Bubbles up [`PlannerError`].
pub fn scan_for_drift<B: PlannerBackend>(
    detector: &DriftDetector<B>,
    source: &Source,
    bindings: &IndexMap<String, String>,
) -> Result<DriftScanOutcome, PlannerError> {
    let source_text = source_text(source);
    let report = detector.scan(&source_text, bindings)?;
    Ok(DriftScanOutcome::from_report(report))
}

/// Render the typed [`Source`] into the string the planner consumes.
/// For `Source::Inline` the string IS the source; for `Source::Name`
/// the controller should resolve via lava-architectures first; for
/// `Source::Git` the bridge fetches before this fn is called.
fn source_text(source: &Source) -> String {
    match source {
        Source::Inline { inline } => inline.clone(),
        Source::Name { name } => name.clone(),
        Source::Git { path, .. } => path.clone(),
    }
}

/// Typed outcome of one drift scan. The controller maps this to the
/// next [`Phase`] + emits an OutcomeChain receipt with the matching
/// diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftScanOutcome {
    pub report: DriftReport,
    pub recommended_phase: Phase,
    pub remediation_hint: RemediationHint,
}

/// Recommendation surface — L4's AnomalyController owns the typed
/// policy; this hint lets L3 do the right thing in the absence of a
/// policy CR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemediationHint {
    /// Drift-free; stay Applied.
    None,
    /// Cosmetic drift; record it but don't reconverge automatically.
    Record,
    /// Functional drift; reconverge.
    AutoCorrect,
    /// Critical drift; requeue + emit anomaly + WAIT for policy.
    HoldForPolicy,
}

impl DriftScanOutcome {
    fn from_report(report: DriftReport) -> Self {
        let (recommended_phase, remediation_hint) = match report.max_severity {
            None => (Phase::Applied, RemediationHint::None),
            Some(Severity::Cosmetic) => (Phase::Drifted, RemediationHint::Record),
            Some(Severity::Functional) => (Phase::Reconverging, RemediationHint::AutoCorrect),
            Some(Severity::Critical) => (Phase::Drifted, RemediationHint::HoldForPolicy),
        };
        Self {
            report,
            recommended_phase,
            remediation_hint,
        }
    }

    /// Project the scan outcome into a [`ReconcileOutcome`] the
    /// existing controller machinery already understands.
    #[must_use]
    pub fn to_reconcile_outcome(&self) -> ReconcileOutcome {
        let conditions = match self.remediation_hint {
            RemediationHint::None => vec![Condition::ok("Drift", "Clean")],
            RemediationHint::Record => vec![Condition::ok(
                "Drift",
                format!("Cosmetic ({} fields)", self.report.count()),
            )],
            RemediationHint::AutoCorrect => vec![Condition::ok(
                "Drift",
                format!("Functional ({} fields) — reconverging", self.report.count()),
            )],
            RemediationHint::HoldForPolicy => vec![Condition::fail(
                "Drift",
                format!(
                    "Critical ({} fields) — awaiting RemediationPolicy",
                    self.report.count()
                ),
            )],
        };
        ReconcileOutcome {
            phase: self.recommended_phase,
            conditions,
            terraform_json: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lava_drift::{ChangeKind, DriftFinding, MockPlanner};

    fn finding(kind: ChangeKind, attr: &str) -> DriftFinding {
        DriftFinding {
            address: "aws_vpc.main".into(),
            attribute: attr.into(),
            change: kind,
            observed: Some("a".into()),
            declared: Some("b".into()),
        }
    }

    #[test]
    fn empty_report_recommends_applied_and_none_hint() {
        let detector = DriftDetector::new(MockPlanner::empty());
        let outcome = scan_for_drift(
            &detector,
            &Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        assert_eq!(outcome.recommended_phase, Phase::Applied);
        assert_eq!(outcome.remediation_hint, RemediationHint::None);
    }

    #[test]
    fn cosmetic_drift_recommends_drifted_and_record() {
        let detector = DriftDetector::new(MockPlanner::new(vec![finding(
            ChangeKind::Update,
            "tags.Owner",
        )]));
        let outcome = scan_for_drift(
            &detector,
            &Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        assert_eq!(outcome.recommended_phase, Phase::Drifted);
        assert_eq!(outcome.remediation_hint, RemediationHint::Record);
    }

    #[test]
    fn functional_drift_recommends_reconverging_and_autocorrect() {
        let detector = DriftDetector::new(MockPlanner::new(vec![finding(
            ChangeKind::Update,
            "cidr_block",
        )]));
        let outcome = scan_for_drift(
            &detector,
            &Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        assert_eq!(outcome.recommended_phase, Phase::Reconverging);
        assert_eq!(outcome.remediation_hint, RemediationHint::AutoCorrect);
    }

    #[test]
    fn critical_drift_recommends_drifted_and_hold_for_policy() {
        let detector = DriftDetector::new(MockPlanner::new(vec![finding(
            ChangeKind::Delete,
            "*",
        )]));
        let outcome = scan_for_drift(
            &detector,
            &Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        assert_eq!(outcome.recommended_phase, Phase::Drifted);
        assert_eq!(outcome.remediation_hint, RemediationHint::HoldForPolicy);
    }

    #[test]
    fn outcome_projects_to_reconcile_outcome() {
        let detector = DriftDetector::new(MockPlanner::new(vec![finding(
            ChangeKind::Delete,
            "*",
        )]));
        let outcome = scan_for_drift(
            &detector,
            &Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        )
        .unwrap();
        let recon = outcome.to_reconcile_outcome();
        assert_eq!(recon.phase, Phase::Drifted);
        assert_eq!(recon.conditions.len(), 1);
        assert_eq!(recon.conditions[0].status, "False"); // critical → fail-condition
    }
}
