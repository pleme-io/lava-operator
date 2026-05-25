//! kube-rs controller wrapper for [`crate::LavaArchitectureSpec`].
//!
//! Compiled only when the `controller` feature is enabled. The
//! controller watches `LavaArchitecture` CRs in the cluster and
//! drives each one through [`crate::reconcile`] using a caller-
//! supplied synthesize callback. The callback is the seam through
//! which magma-lava (or the future in-process magma library) is
//! plugged in — keeps this module synthesis-engine-agnostic.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kube::{
    api::{Api, Patch, PatchParams},
    runtime::{controller::Action, watcher::Config, Controller},
    Client, CustomResource, ResourceExt,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::{Condition, LavaArchitectureSpec, Phase, Source};

/// kube-rs typed shape. Mirrors [`crate::LavaArchitectureSpec`]
/// but carries the `#[derive(CustomResource)]` plumbing kube-rs
/// needs to dispatch the watch + patch loop.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "lava.pleme.io",
    version = "v1alpha1",
    kind = "LavaArchitecture",
    namespaced,
    status = "LavaArchitectureStatusCR"
)]
#[serde(rename_all = "camelCase")]
pub struct LavaArchitectureSpecCR {
    pub source: SourceCR,
    #[serde(default)]
    pub bindings: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub gate: Option<String>,
    #[serde(default = "default_engine")]
    pub engine: String,
}

fn default_engine() -> String {
    "embedded".to_string()
}

/// Flat source descriptor — `inline`, `name`, and the three git
/// fields are all optional + the consumer picks the populated one.
/// Flat shape avoids kube-rs JsonSchema discriminated-union pitfalls
/// + matches the YAML authoring shape operators expect:
///
///     source:
///       inline: |
///         (deflava-architecture ...)
///
///     source:
///       name: aws-vpc-network
///
///     source:
///       url:  https://github.com/...
///       rev:  main
///       path: infra/vpc.tlisp
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SourceCR {
    #[serde(default)]
    pub inline: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub rev: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
}

impl SourceCR {
    /// Pick the populated variant. Precedence: inline > name > git.
    /// Returns `None` when the source is empty.
    #[must_use]
    pub fn variant(&self) -> Option<crate::Source> {
        if let Some(inline) = &self.inline {
            Some(crate::Source::Inline { inline: inline.clone() })
        } else if let Some(name) = &self.name {
            Some(crate::Source::Name { name: name.clone() })
        } else if let (Some(url), Some(rev), Some(path)) = (&self.url, &self.rev, &self.path) {
            Some(crate::Source::Git {
                url: url.clone(),
                rev: rev.clone(),
                path: path.clone(),
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LavaArchitectureStatusCR {
    pub phase: Option<String>,
    #[serde(default)]
    pub conditions: Vec<ConditionCR>,
    pub last_synthesized_hash: Option<String>,
    pub last_applied_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConditionCR {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String,
    pub reason: Option<String>,
    pub message: Option<String>,
}

impl From<&Condition> for ConditionCR {
    fn from(c: &Condition) -> Self {
        Self {
            kind: c.kind.clone(),
            status: c.status.clone(),
            reason: c.reason.clone(),
            message: c.message.clone(),
        }
    }
}

// ── RemediationPolicy CR ──────────────────────────────────────────

/// Typed kube-rs CRD for [`lava_anomaly::RemediationPolicy`].
/// Operators attach one of these per cluster (or per environment)
/// and reference it from `LavaArchitectureSpecCR.remediationPolicyRef`.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "lava.pleme.io",
    version = "v1alpha1",
    kind = "RemediationPolicy",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct RemediationPolicySpec {
    pub cosmetic: String,
    pub functional: String,
    pub critical: String,
    #[serde(default)]
    pub escalation: Option<EscalationLadderCR>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EscalationLadderCR {
    pub tiers: Vec<EscalationTierCR>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EscalationTierCR {
    pub target: NotifyTargetCR,
    /// ISO-8601 duration string (`PT15M`, `PT1H`) — parsed at use
    /// site. Kept as String here so the CRD schema stays simple +
    /// JsonSchema-derivable without a chrono::Duration crater.
    #[serde(default = "default_tier_wait")]
    pub wait_before_next: String,
}

fn default_tier_wait() -> String {
    "PT15M".to_string()
}

/// Flat notify-target shape — `kind` is a free string + remaining
/// fields are optional. The lava-anomaly typed enum
/// (`Slack | Ntfy | Pagerduty | Email | Webhook | Custom`) is
/// reconstructed at use-site. Flat layout keeps the JsonSchema
/// derivation simple (kube-rs apiextensions/v1 rejects tagged-enum
/// CRDs with per-variant property schemas).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NotifyTargetCR {
    /// `slack` | `ntfy` | `pagerduty` | `email` | `webhook`.
    pub kind: String,
    #[serde(default)]
    pub webhook_secret_ref: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub service_key_secret_ref: Option<String>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub secret_ref: Option<String>,
}

impl RemediationPolicySpec {
    /// Translate the CRD shape into the typed library policy used by
    /// the reconcile loop. Unknown action strings degrade to
    /// `RemediationAction::Alert` rather than failing the reconcile.
    #[must_use]
    pub fn to_policy(&self) -> lava_anomaly::RemediationPolicy {
        use lava_anomaly::RemediationAction;
        let parse = |s: &str| -> RemediationAction {
            match s {
                "NoOp" => RemediationAction::NoOp,
                "Alert" => RemediationAction::Alert,
                "AutoCorrect" => RemediationAction::AutoCorrect,
                "RequireApproval" => RemediationAction::RequireApproval,
                "Escalate" => RemediationAction::Escalate,
                _ => RemediationAction::Alert,
            }
        };
        lava_anomaly::RemediationPolicy {
            cosmetic: parse(&self.cosmetic),
            functional: parse(&self.functional),
            critical: parse(&self.critical),
            escalation: None, // escalation tier conversion is intentionally
                              // deferred to L4.1 — needs Duration parsing.
        }
    }
}

// ── LavaArchitectureDependency CR ─────────────────────────────────

/// Typed kube-rs CRD for [`lava_dependency::LavaArchitectureDependency`].
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[kube(
    group = "lava.pleme.io",
    version = "v1alpha1",
    kind = "LavaArchitectureDependency",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct LavaArchitectureDependencySpec {
    pub from: ResourceRefCR,
    pub to: ResourceRefCR,
    pub kind: String, // "BlocksOn" | "Influences"
    #[serde(default = "default_require_phase")]
    pub require_phase: String,
}

fn default_require_phase() -> String {
    "Applied".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRefCR {
    pub cluster: String,
    pub namespace: String,
    pub name: String,
}

impl LavaArchitectureDependencySpec {
    /// Translate the CRD shape into the typed library value used by
    /// the dependency resolver.
    #[must_use]
    pub fn to_lib(&self) -> lava_dependency::LavaArchitectureDependency {
        use lava_dependency::{DependencyKind, LavaArchitectureDependency};
        use lava_outcome_chain::ResourceAddress;
        let kind = match self.kind.as_str() {
            "Influences" => DependencyKind::Influences,
            _ => DependencyKind::BlocksOn,
        };
        LavaArchitectureDependency {
            from: ResourceAddress::new(&self.from.cluster, &self.from.namespace, &self.from.name),
            to: ResourceAddress::new(&self.to.cluster, &self.to.namespace, &self.to.name),
            kind,
            require_phase: self.require_phase.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error("kube: {0}")]
    Kube(#[from] kube::Error),
    #[error("finalizer: {0}")]
    Finalizer(String),
}

/// Synthesize callback — caller supplies the real magma-lava bridge.
/// Returning Err is captured into a `Failed` Phase + Condition by the
/// reconcile loop.
pub type SynthesizeFn = Arc<
    dyn Fn(
            &Source,
            &indexmap::IndexMap<String, String>,
            Option<&str>,
        ) -> Result<serde_json::Value, String>
        + Send
        + Sync,
>;

/// Per-controller shared context. The chain handle is `Arc<Mutex>`
/// so every reconcile pass appends through the same sink — across
/// every LavaArchitecture CR on this operator instance, every
/// receipt links to the same monotonic sequence.
///
/// In production this is typically a `FilesystemSink` rooted at
/// `/var/lib/lava-operator/chains/<resource-key>/` with an Ed25519
/// signer loaded from the `lava-operator-signing-key` Secret. The
/// default (`Context::with_in_memory_chain`) uses an `InMemorySink +
/// NoSigning` so the controller is runnable end-to-end without
/// configuration.
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub synthesize: SynthesizeFn,
    pub chain: std::sync::Arc<
        std::sync::Mutex<
            lava_outcome_chain::OutcomeChain<
                lava_outcome_chain::OutcomePayload,
                lava_outcome_chain::InMemorySink<lava_outcome_chain::OutcomePayload>,
                lava_outcome_chain::NoSigning,
            >,
        >,
    >,
}

impl Context {
    /// Construct a context with an in-memory chain + no signing.
    /// Production callers replace `chain` with a typed
    /// `FilesystemSink + Ed25519Signer` after construction.
    #[must_use]
    pub fn with_in_memory_chain(client: Client, synthesize: SynthesizeFn) -> Self {
        Self {
            client,
            synthesize,
            chain: crate::viggy_loop::shared_in_memory_chain(),
        }
    }
}

/// Reconcile one resource. Drives the full 7-beat Viggy tick via
/// `viggy_loop::LavaPromessaController` + `ViggyEngine`, appends a
/// signed receipt to the shared `OutcomeChain`, and patches the
/// resource's `.status` to reflect the typed tick outcome.
///
/// Solid abstraction: the controller never composes beats by hand.
/// The synthesize callback (Context::synthesize) is the only
/// IaC-engine seam; everything else is typed.
pub async fn reconcile_one(
    obj: Arc<LavaArchitecture>,
    ctx: Arc<Context>,
) -> Result<Action, ControllerError> {
    use crate::viggy_loop::{engine_with_default_router, LavaPromessaController};
    use lava_anomaly::RemediationPolicy;
    use lava_drift::{DriftDetector, PlannerBackend, PlannerError};
    use lava_outcome_chain::ResourceAddress;

    let ns = obj.namespace().unwrap_or_else(|| "default".to_string());
    let name = obj.name_any();
    let api: Api<LavaArchitecture> = Api::namespaced(ctx.client.clone(), &ns);

    let spec = to_lib_spec(&obj.spec);
    let address = ResourceAddress::new(
        std::env::var("LAVA_OPERATOR_CLUSTER").unwrap_or_else(|_| "local".into()),
        ns.clone(),
        name.clone(),
    );

    // Resolve source text up-front so the Viggy controller has it
    // available for the spec-hash + the Diff beat.
    let source_text = match &spec.source {
        crate::Source::Inline { inline } => inline.clone(),
        crate::Source::Name { name } => name.clone(),
        crate::Source::Git { path, .. } => path.clone(),
    };

    // PlannerBackend that runs magma's plan engine in-process when
    // the `magma-bridge` feature is on, and falls back to a stub
    // that calls the synthesize callback (which the operator gets
    // even without magma-bridge) otherwise.
    #[cfg(feature = "magma-bridge")]
    let detector = DriftDetector::new(crate::magma_bridge::EmbeddedMagmaPlanner);

    #[cfg(not(feature = "magma-bridge"))]
    let detector = {
        struct CallbackPlanner {
            synthesize: SynthesizeFn,
            source: crate::Source,
        }
        impl PlannerBackend for CallbackPlanner {
            fn plan(
                &self,
                _src: &str,
                bindings: &indexmap::IndexMap<String, String>,
            ) -> Result<Vec<lava_drift::DriftFinding>, PlannerError> {
                (self.synthesize)(&self.source, bindings, None)
                    .map(|_| Vec::new())
                    .map_err(PlannerError::Plan)
            }
        }
        DriftDetector::new(CallbackPlanner {
            synthesize: ctx.synthesize.clone(),
            source: spec.source.clone(),
        })
    };

    let controller_ = LavaPromessaController {
        source_text,
        source_address: address.clone(),
        detector,
        chain: ctx.chain.clone(),
    };

    let engine = engine_with_default_router(controller_, RemediationPolicy::default());
    // SAFETY: bindings move into the tick; we keep a clone for the
    // status patch diagnostic.
    let bindings = spec.bindings.clone();
    let report = engine.tick(address, bindings);

    let conditions: Vec<ConditionCR> = report
        .beats
        .iter()
        .map(|b| ConditionCR {
            kind: format!("Beat.{}", b.beat.as_str()),
            status: match b.status {
                lava_viggy::BeatStatus::Ok => "True".to_string(),
                lava_viggy::BeatStatus::Skipped => "Unknown".to_string(),
                lava_viggy::BeatStatus::Failed => "False".to_string(),
            },
            reason: Some(format!("{:?}", b.status)),
            message: b.message.clone(),
        })
        .collect();

    let status = LavaArchitectureStatusCR {
        phase: Some(report.final_phase.as_str().to_string()),
        conditions,
        last_synthesized_hash: None,
        last_applied_at: Some(report.ended_at.to_rfc3339()),
    };
    let patch = json!({ "status": status });
    api.patch_status(&name, &PatchParams::apply("lava-operator").force(), &Patch::Apply(patch))
        .await?;

    let requeue = report
        .decision
        .as_ref()
        .map(|d| Duration::from_secs(d.requeue_after.num_seconds().max(1) as u64))
        .unwrap_or_else(|| Duration::from_secs(30));
    Ok(Action::requeue(requeue))
}

pub fn error_policy(
    _obj: Arc<LavaArchitecture>,
    _err: &ControllerError,
    _ctx: Arc<Context>,
) -> Action {
    Action::requeue(Duration::from_secs(60))
}

/// Run the controller loop. Caller passes a synthesize callback that
/// bridges to magma-lava (in-process) or shells out to tofu/terraform.
///
/// # Errors
/// Surfaces kube-rs client construction failures.
pub async fn run(synthesize: SynthesizeFn) -> Result<(), kube::Error> {
    let client = Client::try_default().await?;
    let api: Api<LavaArchitecture> = Api::all(client.clone());
    let ctx = Arc::new(Context::with_in_memory_chain(client, synthesize));
    Controller::new(api, Config::default())
        .run(reconcile_one, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::info!(?obj, "reconciled"),
                Err(e) => tracing::warn!(error = %e, "reconcile error"),
            }
        })
        .await;
    Ok(())
}

fn to_lib_spec(spec: &LavaArchitectureSpecCR) -> LavaArchitectureSpec {
    let mut bindings = indexmap::IndexMap::new();
    for (k, v) in &spec.bindings {
        bindings.insert(k.clone(), v.clone());
    }
    LavaArchitectureSpec {
        source: spec.source.variant().unwrap_or_else(|| Source::Inline {
            inline: "(deflava-architecture empty :inputs () :resources ())".to_string(),
        }),
        bindings,
        gate: spec.gate.clone(),
        engine: spec.engine.clone(),
    }
}

/// Re-export of [`Phase::as_str`] kept here for symmetry with the
/// pre-M2 controller API. New code should use `Phase::as_str` directly.
#[must_use]
pub const fn phase_str(p: Phase) -> &'static str {
    p.as_str()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_lib_spec_round_trips_inline_source() {
        let cr = LavaArchitectureSpecCR {
            source: SourceCR {
                inline: Some("(x)".into()),
                ..Default::default()
            },
            bindings: std::collections::BTreeMap::from_iter([(
                "name".to_string(),
                "prod".to_string(),
            )]),
            gate: Some("vpc".into()),
            engine: "embedded".into(),
        };
        let lib = to_lib_spec(&cr);
        assert!(matches!(lib.source, Source::Inline { .. }));
        assert_eq!(lib.bindings["name"], "prod");
        assert_eq!(lib.gate.as_deref(), Some("vpc"));
    }

    #[test]
    fn condition_cr_converts_from_lib_condition() {
        let c = Condition::ok("Synthesized", "RenderOk");
        let cr: ConditionCR = (&c).into();
        assert_eq!(cr.kind, "Synthesized");
        assert_eq!(cr.status, "True");
        assert_eq!(cr.reason.as_deref(), Some("RenderOk"));
    }

    #[test]
    fn remediation_policy_spec_to_policy_parses_action_strings() {
        let spec = RemediationPolicySpec {
            cosmetic: "NoOp".into(),
            functional: "AutoCorrect".into(),
            critical: "Escalate".into(),
            escalation: None,
        };
        let p = spec.to_policy();
        assert_eq!(p.cosmetic, lava_anomaly::RemediationAction::NoOp);
        assert_eq!(p.functional, lava_anomaly::RemediationAction::AutoCorrect);
        assert_eq!(p.critical, lava_anomaly::RemediationAction::Escalate);
    }

    #[test]
    fn remediation_policy_spec_degrades_unknown_action_to_alert() {
        let spec = RemediationPolicySpec {
            cosmetic: "Moonwalk".into(),
            functional: "Yodel".into(),
            critical: "Apply".into(),
            escalation: None,
        };
        let p = spec.to_policy();
        assert_eq!(p.cosmetic, lava_anomaly::RemediationAction::Alert);
        assert_eq!(p.functional, lava_anomaly::RemediationAction::Alert);
        assert_eq!(p.critical, lava_anomaly::RemediationAction::Alert);
    }

    #[test]
    fn dependency_spec_to_lib_maps_kind_and_addresses() {
        let spec = LavaArchitectureDependencySpec {
            from: ResourceRefCR {
                cluster: "rio".into(),
                namespace: "lava-system".into(),
                name: "app".into(),
            },
            to: ResourceRefCR {
                cluster: "rio".into(),
                namespace: "lava-system".into(),
                name: "vpc".into(),
            },
            kind: "Influences".into(),
            require_phase: "Applied".into(),
        };
        let d = spec.to_lib();
        assert_eq!(d.kind, lava_dependency::DependencyKind::Influences);
        assert_eq!(d.from.namespace, "lava-system");
        assert_eq!(d.to.name, "vpc");
    }

    #[test]
    fn dependency_spec_defaults_kind_to_blocks_on_for_unknown_kind() {
        let spec = LavaArchitectureDependencySpec {
            from: ResourceRefCR {
                cluster: "rio".into(),
                namespace: "lava-system".into(),
                name: "app".into(),
            },
            to: ResourceRefCR {
                cluster: "rio".into(),
                namespace: "lava-system".into(),
                name: "vpc".into(),
            },
            kind: "unrecognized".into(),
            require_phase: "Applied".into(),
        };
        let d = spec.to_lib();
        assert_eq!(d.kind, lava_dependency::DependencyKind::BlocksOn);
    }

    #[test]
    fn phase_str_covers_every_variant() {
        for p in [
            Phase::Pending,
            Phase::Synthesized,
            Phase::Planned,
            Phase::Applied,
            Phase::Drifted,
            Phase::Reconverging,
            Phase::Finalizing,
            Phase::Failed,
        ] {
            assert!(!phase_str(p).is_empty(), "{p:?}");
        }
        assert_eq!(phase_str(Phase::Pending), "Pending");
        assert_eq!(phase_str(Phase::Synthesized), "Synthesized");
        assert_eq!(phase_str(Phase::Planned), "Planned");
        assert_eq!(phase_str(Phase::Applied), "Applied");
        assert_eq!(phase_str(Phase::Failed), "Failed");
    }
}
