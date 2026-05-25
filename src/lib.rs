//! lava-operator — typed Kubernetes controller for the LavaArchitecture
//! CRD. Pangea-operator analog for the lava + tatara-lisp stack.
//!
//! ## Shape
//!
//! Operator authors a LavaArchitecture manifest:
//!
//! ```yaml
//! apiVersion: lava.pleme.io/v1alpha1
//! kind: LavaArchitecture
//! metadata: { name: prod-vpc, namespace: infra }
//! spec:
//!   source:
//!     # Either inline tlisp source ...
//!     inline: |
//!       (deflava-architecture demo-vpc
//!         :inputs ((:cidr "10.42.0.0/16"))
//!         :resources ((aws-vpc "main" :cidr-block "{cidr}")))
//!     # ... or a registry-hosted reference:
//!     # name: aws-vpc-network  (looks up bundled architecture)
//!   bindings:
//!     name: prod
//!     cidr: 10.42.0.0/16
//!   gate: aws-vpc-network              # optional typed Interface gate
//!   engine: embedded                   # embedded | tofu | terraform
//! status:
//!   phase: Applied
//!   conditions:
//!     - type: Synthesized | Planned | Applied | Failed
//!       status: True
//! ```
//!
//! The controller's Reconcile loop:
//!   1. Resolve source (inline or bundled name)
//!   2. magma-lava::synthesize → typed Architecture + terraform.json
//!   3. (Optional) typed-interface gate
//!   4. engine: embedded (in-process) OR shell out to tofu/terraform
//!   5. Status update with typed Conditions
//!
//! ## Status
//!
//! This crate ships the **typed CRD schema** + **reconcile state machine**
//! (typed Phase + Condition + RenderRecord values). The kube-rs
//! controller binary is the next-milestone (M1) wrapper — keeps this
//! crate dependency-light + testable without a live cluster.

#![allow(clippy::module_name_repetitions)]

#[cfg(feature = "controller")]
pub mod controller;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Top-level CRD spec + status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LavaArchitecture {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: LavaArchitectureSpec,
    #[serde(default)]
    pub status: LavaArchitectureStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub labels: IndexMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LavaArchitectureSpec {
    pub source: Source,
    #[serde(default)]
    pub bindings: IndexMap<String, String>,
    #[serde(default)]
    pub gate: Option<String>,
    #[serde(default = "default_engine")]
    pub engine: String,
}

fn default_engine() -> String {
    "embedded".to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Source {
    /// .tlisp source text embedded directly.
    Inline { inline: String },
    /// Bundled architecture name (looked up via lava-architectures).
    Name { name: String },
    /// Remote git ref + path.
    Git {
        url: String,
        rev: String,
        path: String,
    },
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct LavaArchitectureStatus {
    #[serde(default)]
    pub phase: Option<Phase>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
    #[serde(default)]
    pub last_synthesized_hash: Option<String>,
    #[serde(default)]
    pub last_applied_at: Option<String>,
}

/// Typed phase the controller drives the resource through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Phase {
    /// Initial state; nothing rendered yet.
    Pending,
    /// magma-lava produced typed Architecture + terraform.json.
    Synthesized,
    /// engine plan succeeded; no apply yet.
    Planned,
    /// engine apply succeeded; live resources match desired state.
    Applied,
    /// Last reconcile failed (see conditions for details).
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    #[serde(rename = "type")]
    pub kind: String,
    pub status: String, // True | False | Unknown
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub last_transition_time: Option<String>,
}

impl Condition {
    #[must_use]
    pub fn ok(kind: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            status: "True".into(),
            reason: Some(reason.into()),
            message: None,
            last_transition_time: None,
        }
    }
    #[must_use]
    pub fn fail(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            status: "False".into(),
            reason: Some("Error".into()),
            message: Some(message.into()),
            last_transition_time: None,
        }
    }
}

/// Result of one reconcile pass. The controller serializes this back
/// into the resource's `.status` block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconcileOutcome {
    pub phase: Phase,
    pub conditions: Vec<Condition>,
    pub terraform_json: Option<serde_json::Value>,
}

/// Pure state-machine: given the current resource + a synthesize
/// callback, advance the phase. The kube-rs wrapper supplies the
/// callback (which routes through magma-lava); this function stays
/// kube-free + unit-testable.
pub fn reconcile<F>(
    spec: &LavaArchitectureSpec,
    synthesize_fn: F,
) -> Result<ReconcileOutcome, ReconcileError>
where
    F: FnOnce(&Source, &IndexMap<String, String>, Option<&str>)
        -> Result<serde_json::Value, String>,
{
    let synthesized = synthesize_fn(&spec.source, &spec.bindings, spec.gate.as_deref());
    match synthesized {
        Ok(tf_json) => Ok(ReconcileOutcome {
            phase: Phase::Synthesized,
            conditions: vec![Condition::ok("Synthesized", "RenderOk")],
            terraform_json: Some(tf_json),
        }),
        Err(e) => Ok(ReconcileOutcome {
            phase: Phase::Failed,
            conditions: vec![Condition::fail("Synthesized", e)],
            terraform_json: None,
        }),
    }
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("synthesize: {0}")]
    Synthesize(String),
    #[error("apply: {0}")]
    Apply(String),
}

/// Render the LavaArchitecture CRD YAML for `kubectl apply`.
pub fn crd_yaml() -> String {
    // Typed CRD value via serde_yaml — no format!() of YAML syntax.
    let names = serde_yaml::Mapping::from_iter([
        ("kind".into(), "LavaArchitecture".into()),
        ("listKind".into(), "LavaArchitectureList".into()),
        ("plural".into(), "lavaarchitectures".into()),
        ("singular".into(), "lavaarchitecture".into()),
    ]);
    let version = serde_yaml::Mapping::from_iter([
        ("name".into(), "v1alpha1".into()),
        ("served".into(), serde_yaml::Value::Bool(true)),
        ("storage".into(), serde_yaml::Value::Bool(true)),
    ]);
    let crd_spec = serde_yaml::Mapping::from_iter([
        ("group".into(), "lava.pleme.io".into()),
        ("scope".into(), "Namespaced".into()),
        ("names".into(), serde_yaml::Value::Mapping(names)),
        (
            "versions".into(),
            serde_yaml::Value::Sequence(vec![serde_yaml::Value::Mapping(version)]),
        ),
    ]);
    let crd = serde_yaml::Mapping::from_iter([
        ("apiVersion".into(), "apiextensions.k8s.io/v1".into()),
        ("kind".into(), "CustomResourceDefinition".into()),
        (
            "metadata".into(),
            serde_yaml::Value::Mapping(serde_yaml::Mapping::from_iter([(
                "name".into(),
                "lavaarchitectures.lava.pleme.io".into(),
            )])),
        ),
        ("spec".into(), serde_yaml::Value::Mapping(crd_spec)),
    ]);
    serde_yaml::to_string(&serde_yaml::Value::Mapping(crd)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_spec() -> LavaArchitectureSpec {
        LavaArchitectureSpec {
            source: Source::Inline {
                inline: "(deflava-architecture demo :inputs () :resources ())".into(),
            },
            bindings: IndexMap::new(),
            gate: None,
            engine: "embedded".into(),
        }
    }

    #[test]
    fn reconcile_drives_to_synthesized_when_synthesize_callback_succeeds() {
        let outcome = reconcile(&sample_spec(), |_src, _b, _g| {
            Ok(serde_json::json!({"resource": {}}))
        })
        .unwrap();
        assert_eq!(outcome.phase, Phase::Synthesized);
        assert_eq!(outcome.conditions[0].status, "True");
        assert!(outcome.terraform_json.is_some());
    }

    #[test]
    fn reconcile_drives_to_failed_when_synthesize_callback_errors() {
        let outcome = reconcile(&sample_spec(), |_src, _b, _g| {
            Err("render failed: bad input".to_string())
        })
        .unwrap();
        assert_eq!(outcome.phase, Phase::Failed);
        assert_eq!(outcome.conditions[0].status, "False");
        assert!(outcome.terraform_json.is_none());
    }

    #[test]
    fn condition_ok_and_fail_constructors_set_status_correctly() {
        let ok = Condition::ok("Synthesized", "RenderOk");
        assert_eq!(ok.status, "True");
        assert_eq!(ok.reason.as_deref(), Some("RenderOk"));
        let fail = Condition::fail("Applied", "tofu apply exited non-zero");
        assert_eq!(fail.status, "False");
        assert_eq!(fail.message.as_deref(), Some("tofu apply exited non-zero"));
    }

    #[test]
    fn lava_architecture_round_trips_through_serde_yaml() {
        let la = LavaArchitecture {
            api_version: "lava.pleme.io/v1alpha1".into(),
            kind: "LavaArchitecture".into(),
            metadata: ObjectMeta {
                name: "prod-vpc".into(),
                namespace: Some("infra".into()),
                labels: IndexMap::new(),
            },
            spec: sample_spec(),
            status: LavaArchitectureStatus::default(),
        };
        let yaml = serde_yaml::to_string(&la).unwrap();
        let parsed: LavaArchitecture = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(la, parsed);
    }

    #[test]
    fn source_variants_round_trip_through_serde() {
        for src in [
            Source::Inline { inline: "(x)".into() },
            Source::Name { name: "aws-vpc".into() },
            Source::Git {
                url: "https://github.com/x/y".into(),
                rev: "main".into(),
                path: "infra/vpc.tlisp".into(),
            },
        ] {
            let json = serde_json::to_string(&src).unwrap();
            let parsed: Source = serde_json::from_str(&json).unwrap();
            assert_eq!(src, parsed);
        }
    }

    #[test]
    fn crd_yaml_emits_valid_two_doc_apiextensions_yaml() {
        let yaml = crd_yaml();
        assert!(yaml.contains("apiVersion: apiextensions.k8s.io/v1"));
        assert!(yaml.contains("kind: CustomResourceDefinition"));
        assert!(yaml.contains("name: lavaarchitectures.lava.pleme.io"));
        assert!(yaml.contains("group: lava.pleme.io"));
        assert!(yaml.contains("v1alpha1"));
    }

    #[test]
    fn phase_variants_serialize_as_pascal_case_strings() {
        let p = Phase::Synthesized;
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(json, "\"Synthesized\"");
    }
}
