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

use crate::{reconcile, Condition, LavaArchitectureSpec, Phase, Source};

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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum SourceCR {
    Inline { inline: String },
    Name { name: String },
    Git { url: String, rev: String, path: String },
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

#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub synthesize: SynthesizeFn,
}

/// Reconcile one resource. Renders via the synthesize callback +
/// patches the resource's `.status` to reflect the typed outcome.
pub async fn reconcile_one(
    obj: Arc<LavaArchitecture>,
    ctx: Arc<Context>,
) -> Result<Action, ControllerError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".to_string());
    let name = obj.name_any();
    let api: Api<LavaArchitecture> = Api::namespaced(ctx.client.clone(), &ns);

    let spec = to_lib_spec(&obj.spec);
    let synthesize = ctx.synthesize.clone();
    let outcome = reconcile(&spec, |src, bindings, gate| {
        (synthesize)(src, bindings, gate)
    })
    .map_err(|e| ControllerError::Finalizer(e.to_string()))?;

    let status = LavaArchitectureStatusCR {
        phase: Some(phase_str(outcome.phase).to_string()),
        conditions: outcome.conditions.iter().map(ConditionCR::from).collect(),
        last_synthesized_hash: outcome
            .terraform_json
            .as_ref()
            .map(|v| blake3_short(&serde_json::to_string(v).unwrap_or_default())),
        last_applied_at: None,
    };
    let patch = json!({ "status": status });
    api.patch_status(&name, &PatchParams::apply("lava-operator").force(), &Patch::Apply(patch))
        .await?;

    Ok(Action::requeue(Duration::from_secs(30)))
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
    let ctx = Arc::new(Context { client, synthesize });
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
        source: match &spec.source {
            SourceCR::Inline { inline } => Source::Inline { inline: inline.clone() },
            SourceCR::Name { name } => Source::Name { name: name.clone() },
            SourceCR::Git { url, rev, path } => Source::Git {
                url: url.clone(),
                rev: rev.clone(),
                path: path.clone(),
            },
        },
        bindings,
        gate: spec.gate.clone(),
        engine: spec.engine.clone(),
    }
}

const fn phase_str(p: Phase) -> &'static str {
    match p {
        Phase::Pending => "Pending",
        Phase::Synthesized => "Synthesized",
        Phase::Planned => "Planned",
        Phase::Applied => "Applied",
        Phase::Failed => "Failed",
    }
}

/// Short content-address of the rendered terraform.json — used as a
/// drift heuristic on the next reconcile pass. (Pure stdlib hash —
/// real BLAKE3 lands when the magma-lava bridge owns this surface.)
fn blake3_short(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_lib_spec_round_trips_inline_source() {
        let cr = LavaArchitectureSpecCR {
            source: SourceCR::Inline { inline: "(x)".into() },
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
    fn phase_str_covers_every_variant() {
        assert_eq!(phase_str(Phase::Pending), "Pending");
        assert_eq!(phase_str(Phase::Synthesized), "Synthesized");
        assert_eq!(phase_str(Phase::Planned), "Planned");
        assert_eq!(phase_str(Phase::Applied), "Applied");
        assert_eq!(phase_str(Phase::Failed), "Failed");
    }
}
