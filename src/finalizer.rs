//! Finalizer — runs the destroy path before the CR is removed from
//! etcd. Solid abstraction: the controller calls
//! [`run_finalizer`] with a typed `DestroyBackend`; this module
//! never imports kube-rs (lives in lib defaults too).
//!
//! Backends:
//!   - `EmbeddedMagmaBackend` — wraps magma::apply::run_plan with the
//!     Destroy plan; lives in lava-operator's magma-bridge module.
//!   - `MockDestroy` — caller-supplied outcome for tests.

use crate::{Condition, Phase, ReconcileOutcome};

/// Pluggable destroy seam. Production: magma in-process destroy
/// (added behind the `magma-bridge` feature at L3.1). Tests: mock.
pub trait DestroyBackend {
    /// Drive the destroy. `Ok(diagnostics)` on success;
    /// `Err(message)` on failure.
    ///
    /// # Errors
    /// Backend-specific failures surface as a string the controller
    /// folds into a typed `Condition::fail`.
    fn destroy(&self, source: &crate::Source, bindings: &indexmap::IndexMap<String, String>)
        -> Result<Vec<String>, String>;
}

/// Run the finalizer for one LavaArchitecture. Returns a
/// [`ReconcileOutcome`] the controller patches back into status.
#[must_use]
pub fn run_finalizer<B: DestroyBackend>(
    backend: &B,
    source: &crate::Source,
    bindings: &indexmap::IndexMap<String, String>,
) -> ReconcileOutcome {
    match backend.destroy(source, bindings) {
        Ok(diagnostics) => ReconcileOutcome {
            phase: Phase::Finalizing,
            conditions: {
                let mut c = vec![Condition::ok("Destroyed", "BackendOk")];
                for d in diagnostics {
                    c.push(Condition::ok("Diagnostic", d));
                }
                c
            },
            terraform_json: None,
        },
        Err(e) => ReconcileOutcome {
            phase: Phase::Failed,
            conditions: vec![Condition::fail("Destroyed", e)],
            terraform_json: None,
        },
    }
}

/// Test backend — returns the supplied outcome unconditionally.
pub struct MockDestroy {
    pub outcome: Result<Vec<String>, String>,
}

impl MockDestroy {
    #[must_use]
    pub fn ok() -> Self {
        Self {
            outcome: Ok(vec![]),
        }
    }
    #[must_use]
    pub fn with_diagnostics(msgs: Vec<String>) -> Self {
        Self { outcome: Ok(msgs) }
    }
    #[must_use]
    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            outcome: Err(msg.into()),
        }
    }
}

impl DestroyBackend for MockDestroy {
    fn destroy(
        &self,
        _src: &crate::Source,
        _bindings: &indexmap::IndexMap<String, String>,
    ) -> Result<Vec<String>, String> {
        self.outcome.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;

    #[test]
    fn successful_destroy_drives_to_finalizing_phase() {
        let outcome = run_finalizer(
            &MockDestroy::ok(),
            &crate::Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        );
        assert_eq!(outcome.phase, Phase::Finalizing);
        assert_eq!(outcome.conditions[0].status, "True");
    }

    #[test]
    fn destroy_diagnostics_surface_as_conditions() {
        let outcome = run_finalizer(
            &MockDestroy::with_diagnostics(vec!["removed 3 resources".into()]),
            &crate::Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        );
        assert_eq!(outcome.conditions.len(), 2);
        assert_eq!(outcome.conditions[1].kind, "Diagnostic");
    }

    #[test]
    fn failed_destroy_drives_to_failed_phase_with_message() {
        let outcome = run_finalizer(
            &MockDestroy::err("provider unreachable"),
            &crate::Source::Inline { inline: "src".into() },
            &IndexMap::new(),
        );
        assert_eq!(outcome.phase, Phase::Failed);
        assert_eq!(outcome.conditions[0].status, "False");
        assert_eq!(
            outcome.conditions[0].message.as_deref(),
            Some("provider unreachable"),
        );
    }
}
