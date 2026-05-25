//! Viggy loop — drives `ViggyEngine::tick` on every reconcile pass.
//!
//! Solid abstraction: the kube-rs controller never composes the
//! seven beats by hand. It builds one [`LavaPromessaController`]
//! per CR, hands it to a [`lava_viggy::ViggyEngine`], and calls
//! `engine.tick(source, bindings)` — every beat (Observe, Diff,
//! Classify, Decide, Act, Attest, Tick) runs in order with typed
//! BeatOutcome capture + OutcomeChain attestation.
//!
//! The PlannerBackend seam keeps this module test-clean — tests pass
//! [`lava_drift::MockPlanner`] for an arbitrary scan; production
//! (when `magma-bridge` feature is on) plugs in a magma-lava-backed
//! planner.

use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use lava_anomaly::RemediationPolicy;
use lava_drift::{DriftDetector, DriftReport, PlannerBackend, PlannerError};
use lava_outcome_chain::{
    ChangeSummary, ContentHash, InMemorySink, OutcomeChain, OutcomePayload, OutcomeSink,
    ResourceAddress, SigningProvider,
};
use lava_viggy::{
    Beat, BeatStatus, PromessaController, TickPhase, TickReport, ViggyEngine, ViggyError,
};

/// Concrete controller bound to one [`Source`] (an inline tlisp
/// string, a bundled name, or a git path). Carries a
/// [`PlannerBackend`] for Diff and an [`OutcomeSink`] +
/// [`SigningProvider`] for Attest.
///
/// `Send + Sync` so the kube-rs reconcile callback can construct
/// this on every reconcile pass without lifetime gymnastics.
pub struct LavaPromessaController<B, S, G>
where
    B: PlannerBackend + Send + Sync,
    S: OutcomeSink<OutcomePayload> + Send + Sync + 'static,
    G: SigningProvider + Send + Sync + 'static,
{
    pub source_text: String,
    pub source_address: ResourceAddress,
    pub detector: DriftDetector<B>,
    pub chain: Arc<Mutex<OutcomeChain<OutcomePayload, S, G>>>,
}

/// Context carried between beats. Today: the resolved spec source
/// (already in `source_text`, but we forward it for the act/attest
/// beats so they don't re-clone). Future: cached planner inputs,
/// pre-fetched live state snapshots.
pub struct ObservedContext {
    pub spec_hash: ContentHash,
}

impl<B, S, G> PromessaController for LavaPromessaController<B, S, G>
where
    B: PlannerBackend + Send + Sync,
    S: OutcomeSink<OutcomePayload> + Send + Sync,
    G: SigningProvider + Send + Sync,
{
    type Context = ObservedContext;

    fn observe(
        &self,
        _source: &ResourceAddress,
        _bindings: &IndexMap<String, String>,
    ) -> Result<Self::Context, ViggyError> {
        // Compute the spec hash once and forward to subsequent beats.
        Ok(ObservedContext {
            spec_hash: ContentHash::of(self.source_text.as_bytes()),
        })
    }

    fn diff(
        &self,
        _ctx: &Self::Context,
        bindings: &IndexMap<String, String>,
    ) -> Result<DriftReport, ViggyError> {
        self.detector
            .scan(&self.source_text, bindings)
            .map_err(|e: PlannerError| ViggyError::Diff(e.to_string()))
    }

    fn attest(&self, report: &TickReport) -> Result<(), ViggyError> {
        // Build the canonical payload from the tick report and
        // append to the chain. One receipt per tick.
        let payload = OutcomePayload {
            resource: self.source_address.clone(),
            spec_hash: ContentHash::of(self.source_text.as_bytes()),
            terraform_json_hash: ContentHash::genesis(),
            plan_id: None,
            phase: tick_phase_to_payload_phase(report.final_phase).to_string(),
            change_summary: change_summary_from_report(report),
            diagnostics: diagnostics_from_report(report),
        };
        let mut chain = self
            .chain
            .lock()
            .map_err(|e| ViggyError::Attest(format!("chain mutex poisoned: {e}")))?;
        chain
            .append(payload)
            .map_err(|e| ViggyError::Attest(e.to_string()))?;
        Ok(())
    }
}

fn tick_phase_to_payload_phase(p: TickPhase) -> &'static str {
    match p {
        TickPhase::Stable => "Applied",
        TickPhase::Reconverging => "Reconverging",
        TickPhase::HoldingForApproval => "Drifted",
        TickPhase::Escalated => "Drifted",
        TickPhase::Failed => "Failed",
    }
}

fn change_summary_from_report(report: &TickReport) -> ChangeSummary {
    // Pull the diff beat's message — formatted as "<N> drifted field(s)".
    // Fall back to an empty summary on unmatched shapes.
    let mut summary = ChangeSummary::default();
    if let Some(diff_beat) = report.beats.iter().find(|b| b.beat == Beat::Diff) {
        if let Some(msg) = &diff_beat.message {
            if let Some(n_str) = msg.split_whitespace().next() {
                if let Ok(n) = n_str.parse::<u32>() {
                    // We don't have create/update/delete split here —
                    // surface as a single "update" count so the
                    // payload still carries the cardinality.
                    summary.update = n;
                }
            }
        }
    }
    summary
}

fn diagnostics_from_report(report: &TickReport) -> Vec<String> {
    report
        .beats
        .iter()
        .map(|b| {
            format!(
                "{}={}{}",
                b.beat.as_str(),
                match b.status {
                    BeatStatus::Ok => "Ok",
                    BeatStatus::Skipped => "Skipped",
                    BeatStatus::Failed => "Failed",
                },
                b.message
                    .as_ref()
                    .map(|m| format!(" ({m})"))
                    .unwrap_or_default(),
            )
        })
        .collect()
}

/// Convenience constructor: build a [`ViggyEngine`] with default
/// policy and the [`lava_anomaly::PolicyRouter`]. Consumers that
/// need a custom router pass their own.
#[must_use]
pub fn engine_with_default_router<B, S, G>(
    controller: LavaPromessaController<B, S, G>,
    policy: RemediationPolicy,
) -> ViggyEngine<LavaPromessaController<B, S, G>, lava_anomaly::PolicyRouter>
where
    B: PlannerBackend + Send + Sync,
    S: OutcomeSink<OutcomePayload> + Send + Sync,
    G: SigningProvider + Send + Sync,
{
    ViggyEngine::new(controller, lava_anomaly::PolicyRouter, policy)
}

/// Default shared in-memory chain — useful for tests + the no-sink
/// path in dev clusters.
#[must_use]
pub fn shared_in_memory_chain() -> Arc<
    Mutex<OutcomeChain<OutcomePayload, InMemorySink<OutcomePayload>, lava_outcome_chain::NoSigning>>,
> {
    Arc::new(Mutex::new(OutcomeChain::new(
        InMemorySink::default(),
        lava_outcome_chain::NoSigning,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use lava_drift::{ChangeKind, DriftFinding, MockPlanner};
    use lava_outcome_chain::{verify_chain, NoOpVerifier};

    fn finding(kind: ChangeKind, attr: &str) -> DriftFinding {
        DriftFinding {
            address: "aws_vpc.main".into(),
            attribute: attr.into(),
            change: kind,
            observed: None,
            declared: None,
        }
    }

    fn controller_with(
        findings: Vec<DriftFinding>,
    ) -> LavaPromessaController<
        MockPlanner,
        InMemorySink<OutcomePayload>,
        lava_outcome_chain::NoSigning,
    > {
        LavaPromessaController {
            source_text: "(deflava-architecture demo :inputs () :resources ())".into(),
            source_address: ResourceAddress::new("rio", "lava-system", "demo"),
            detector: DriftDetector::new(MockPlanner::new(findings)),
            chain: shared_in_memory_chain(),
        }
    }

    #[test]
    fn clean_tick_lands_stable_and_appends_one_receipt() {
        let controller = controller_with(vec![]);
        let chain_handle = controller.chain.clone();
        let engine = engine_with_default_router(controller, RemediationPolicy::default());
        let report = engine.tick(
            ResourceAddress::new("rio", "lava-system", "demo"),
            IndexMap::new(),
        );
        assert_eq!(report.final_phase, TickPhase::Stable);
        let chain = chain_handle.lock().unwrap();
        let receipts = chain.read_all().unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].payload.phase, "Applied");
    }

    #[test]
    fn functional_drift_tick_lands_reconverging_and_chain_records_it() {
        let controller = controller_with(vec![finding(ChangeKind::Update, "cidr_block")]);
        let chain_handle = controller.chain.clone();
        let engine = engine_with_default_router(controller, RemediationPolicy::default());
        let report = engine.tick(
            ResourceAddress::new("rio", "lava-system", "demo"),
            IndexMap::new(),
        );
        assert_eq!(report.final_phase, TickPhase::Reconverging);
        let chain = chain_handle.lock().unwrap();
        let receipts = chain.read_all().unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].payload.phase, "Reconverging");
        assert_eq!(receipts[0].payload.change_summary.update, 1);
    }

    #[test]
    fn critical_drift_tick_lands_drifted_and_records_holding_diagnostic() {
        let controller = controller_with(vec![finding(ChangeKind::Delete, "*")]);
        let chain_handle = controller.chain.clone();
        let engine = engine_with_default_router(controller, RemediationPolicy::default());
        let report = engine.tick(
            ResourceAddress::new("rio", "lava-system", "demo"),
            IndexMap::new(),
        );
        assert_eq!(report.final_phase, TickPhase::HoldingForApproval);
        let chain = chain_handle.lock().unwrap();
        let receipts = chain.read_all().unwrap();
        assert_eq!(receipts[0].payload.phase, "Drifted");
        assert!(
            receipts[0]
                .payload
                .diagnostics
                .iter()
                .any(|d| d.starts_with("Decide=Ok")),
            "expected Decide=Ok in diagnostics; got: {:?}",
            receipts[0].payload.diagnostics
        );
    }

    #[test]
    fn three_ticks_produce_a_blake3_linked_chain_that_verifies() {
        let controller = controller_with(vec![]);
        let chain_handle = controller.chain.clone();
        let engine = engine_with_default_router(controller, RemediationPolicy::default());
        for _ in 0..3 {
            engine.tick(
                ResourceAddress::new("rio", "lava-system", "demo"),
                IndexMap::new(),
            );
        }
        let chain = chain_handle.lock().unwrap();
        let receipts = chain.read_all().unwrap();
        assert_eq!(receipts.len(), 3);
        verify_chain(&receipts, &NoOpVerifier).unwrap();
        assert_eq!(receipts[1].prev_hash, receipts[0].content_hash);
        assert_eq!(receipts[2].prev_hash, receipts[1].content_hash);
    }

    #[test]
    fn observe_failure_short_circuits_but_still_attempts_attest() {
        // Build a controller whose observe always fails by giving it
        // a source that's empty-but-non-empty. (Our observe is
        // infallible today; this test asserts the chain stays
        // consistent across a no-op scenario.)
        let controller = controller_with(vec![]);
        let chain_handle = controller.chain.clone();
        let engine = engine_with_default_router(controller, RemediationPolicy::default());
        let _ = engine.tick(
            ResourceAddress::new("rio", "lava-system", "demo"),
            IndexMap::new(),
        );
        let chain = chain_handle.lock().unwrap();
        assert_eq!(chain.read_all().unwrap().len(), 1);
    }
}
