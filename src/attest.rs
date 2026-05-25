//! Attestation bridge — wires the reconcile loop into
//! `lava_outcome_chain` so every phase transition produces a
//! signed BLAKE3-linked receipt.
//!
//! Solid abstraction: the controller never talks to the chain
//! directly. It calls [`record_phase`] with the typed inputs; this
//! module assembles the typed payload + signs + appends. Swapping
//! the sink or signer later means changing the constructor, never
//! the reconcile path.

use indexmap::IndexMap;
use lava_outcome_chain::{
    ChangeSummary, ContentHash, OutcomeChain, OutcomePayload, OutcomeSink, ResourceAddress,
    Signature, SigningProvider,
};

use crate::{Phase, ReconcileOutcome};

/// Build a typed [`OutcomePayload`] from the controller's typed
/// inputs. Kept separate from the append so callers can preview the
/// payload before sealing (useful in dry-run mode).
#[must_use]
pub fn build_payload(
    address: ResourceAddress,
    spec_source: &str,
    outcome: &ReconcileOutcome,
    phase: Phase,
) -> OutcomePayload {
    let spec_hash = ContentHash::of(spec_source.as_bytes());
    let terraform_json_hash = outcome
        .terraform_json
        .as_ref()
        .map(|v| ContentHash::of(serde_json::to_string(v).unwrap_or_default().as_bytes()))
        .unwrap_or_else(ContentHash::genesis);
    let diagnostics: Vec<String> = outcome
        .conditions
        .iter()
        .map(|c| {
            format!(
                "{kind}={status}{}",
                c.message
                    .as_ref()
                    .map(|m| format!(" ({m})"))
                    .unwrap_or_default(),
                kind = c.kind,
                status = c.status,
            )
        })
        .collect();
    OutcomePayload {
        resource: address,
        spec_hash,
        terraform_json_hash,
        plan_id: None, // L3.1 — wire magma plan_id when bridge surfaces it
        phase: phase.as_str().to_string(),
        change_summary: ChangeSummary::default(),
        diagnostics,
    }
}

/// Append one phase-transition receipt to the chain. Encapsulates
/// the typed payload assembly + the chain append.
///
/// # Errors
/// Bubbles up [`lava_outcome_chain::AppendError`].
pub fn record_phase<S, G>(
    chain: &mut OutcomeChain<OutcomePayload, S, G>,
    address: ResourceAddress,
    spec_source: &str,
    outcome: &ReconcileOutcome,
    phase: Phase,
) -> Result<lava_outcome_chain::Receipt<OutcomePayload>, lava_outcome_chain::AppendError>
where
    S: OutcomeSink<OutcomePayload>,
    G: SigningProvider,
{
    let payload = build_payload(address, spec_source, outcome, phase);
    chain.append(payload)
}

/// Public re-export of the [`Signature::None`] discriminant so
/// consumers don't need a separate `use` line.
pub const NO_SIGNATURE: Signature = Signature::None;

/// Convenience type alias: the `(spec_source, bindings)` pair every
/// reconcile callback hands the attestation bridge.
pub type SpecInputs<'a> = (&'a str, &'a IndexMap<String, String>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Condition, Phase};
    use lava_outcome_chain::{verify_chain, InMemorySink, NoOpVerifier, NoSigning};
    use serde_json::json;

    fn sample_outcome() -> ReconcileOutcome {
        ReconcileOutcome {
            phase: Phase::Synthesized,
            conditions: vec![Condition::ok("Synthesized", "RenderOk")],
            terraform_json: Some(json!({ "resource": { "aws_vpc": { "main": {} } } })),
        }
    }

    #[test]
    fn build_payload_populates_spec_hash_from_source() {
        let p1 = build_payload(
            ResourceAddress::new("c", "n", "x"),
            "source-v1",
            &sample_outcome(),
            Phase::Synthesized,
        );
        let p2 = build_payload(
            ResourceAddress::new("c", "n", "x"),
            "source-v2",
            &sample_outcome(),
            Phase::Synthesized,
        );
        assert_ne!(p1.spec_hash, p2.spec_hash);
    }

    #[test]
    fn build_payload_hashes_terraform_json_when_present() {
        let p = build_payload(
            ResourceAddress::new("c", "n", "x"),
            "src",
            &sample_outcome(),
            Phase::Synthesized,
        );
        assert!(!p.terraform_json_hash.is_genesis());
    }

    #[test]
    fn build_payload_uses_genesis_when_terraform_json_absent() {
        let mut o = sample_outcome();
        o.terraform_json = None;
        let p = build_payload(
            ResourceAddress::new("c", "n", "x"),
            "src",
            &o,
            Phase::Failed,
        );
        assert!(p.terraform_json_hash.is_genesis());
    }

    #[test]
    fn record_phase_appends_and_chain_verifies() {
        let mut chain = OutcomeChain::new(InMemorySink::<OutcomePayload>::default(), NoSigning);
        for phase in [
            Phase::Pending,
            Phase::Synthesized,
            Phase::Planned,
            Phase::Applied,
        ] {
            record_phase(
                &mut chain,
                ResourceAddress::new("rio", "infra", "prod"),
                "src",
                &sample_outcome(),
                phase,
            )
            .unwrap();
        }
        let receipts = chain.read_all().unwrap();
        assert_eq!(receipts.len(), 4);
        verify_chain(&receipts, &NoOpVerifier).unwrap();
    }
}
