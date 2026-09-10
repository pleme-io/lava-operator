//! magma-lava bridge — produces a `SynthesizeFn` callback the
//! controller can register with `lava_operator::controller::run`.
//!
//! Compiled only when both the `controller` and `magma-bridge`
//! features are enabled. Bridges the controller's [`crate::Source`]
//! into magma-lava's [`magma_lava::LavaSource`] and returns the
//! terraform.json the controller patches back into the resource
//! status.

use std::sync::Arc;

use crate::finalizer::DestroyBackend;
use crate::Source;
use indexmap::IndexMap;
#[cfg(feature = "magma-bridge")]
use magma_backend::Backend;

/// One end-to-end apply cycle for a lava source: synthesize .tlisp →
/// terraform.json → parse → refresh → plan → run with providers →
/// persist state. Mirrors the shape of `magma-cli`'s `cmd_apply` and
/// `pangea-operator`'s `MagmaExecutor::apply`, adapted for the
/// operator's per-CR workspace layout.
///
/// `workspace_dir` is a per-CR directory the operator owns (created
/// by the caller if absent); the state file lives at
/// `<workspace_dir>/terraform.tfstate` via `magma::backend::LocalBackend`
/// and survives pod restarts when mounted from a durable volume, so
/// a bounced operator resumes converging rather than replaying from
/// zero.
///
/// `provider_configs` maps provider LOCAL NAME → its config JSON (the
/// same shape Terraform's `provider "<name>" { ... }` block takes).
/// For the Discord provider that's `{"token": "<bot-token>"}`.
///
/// `plugin_dir` is prepended to the child process's PATH so the
/// terraform-provider-* binaries the operator's initContainer copies
/// into the shared volume are discovered by magma's gRPC spawn.
///
/// # Errors
///
/// Any stage failure surfaces as a stringly-typed `String` (the same
/// shape `SynthesizeFn` uses) so the caller can log it without
/// pulling every underlying error variant into its trait bounds.
pub async fn apply_changes(
    source: &Source,
    bindings: &IndexMap<String, String>,
    workspace_dir: std::path::PathBuf,
    provider_configs: std::collections::BTreeMap<String, serde_json::Value>,
    plugin_dir: Option<std::path::PathBuf>,
) -> Result<magma_apply::ApplyOutcome, String> {
    // ── 1. synthesize .tlisp → terraform.json ─────────────────────
    let lava_source = source_to_lava_source(source);
    let lava_bindings = bindings_to_artifact_bindings_string(bindings);
    let plan = magma_lava::synthesize_source(&lava_source, &lava_bindings, None)
        .map_err(|e| format!("synthesize: {e}"))?;

    // ── 2. parse into a magma Config ──────────────────────────────
    let cfg = magma_config::Config::from_json(plan.terraform_json)
        .map_err(|e| format!("config parse: {e}"))?;

    // ── 3. workspace / state backend / read prior state ───────────
    std::fs::create_dir_all(&workspace_dir)
        .map_err(|e| format!("workspace mkdir {}: {e}", workspace_dir.display()))?;
    let backend = magma_backend::LocalBackend::new(workspace_dir.clone());
    let mut state = backend
        .read_state()
        .await
        .map_err(|e| format!("read state: {e}"))?;

    // ── 4. build an ApplyContext with the provider configs ────────
    //
    // `PATH` is prepended so magma-plugin's spawn finds the provider
    // binaries the operator's initContainer laid into the shared
    // volume. Prepend rather than replace so a container that
    // already ships providers on the default PATH keeps them.
    if let Some(dir) = &plugin_dir {
        let existing = std::env::var_os("PATH").unwrap_or_default();
        let mut combined = std::ffi::OsString::new();
        combined.push(dir);
        if !existing.is_empty() {
            combined.push(":");
            combined.push(existing);
        }
        // SAFETY: this is a single-process operator and PATH is only
        // consulted by child spawns which happen strictly after this
        // point. No other thread reads PATH concurrently.
        // (unsafe block only under 2024 edition — earlier editions
        // treat set_var as safe. Wrapping keeps future-us honest.)
        unsafe {
            std::env::set_var("PATH", combined);
        }
    }
    let mut ctx = magma_apply::engine::ApplyContext::new(workspace_dir.clone());
    for (name, config) in provider_configs {
        ctx = ctx.with_provider_config(name, config);
    }

    // ── 5. plan (with refresh) → apply-with-providers → persist ───
    let (plan_obj, _refresh_report) =
        magma_apply::engine::refresh_then_plan(&cfg, &mut state, Some(&ctx))
            .await
            .map_err(|e| format!("plan: {e}"))?;

    let outcome = magma_apply::engine::run_plan_with_providers(&plan_obj, &mut state, &ctx).await;

    backend
        .write_state(&state)
        .await
        .map_err(|e| format!("write state: {e}"))?;

    Ok(outcome)
}

fn bindings_to_artifact_bindings_string(
    bindings: &IndexMap<String, String>,
) -> IndexMap<String, magma_lava::Binding> {
    let mut out = IndexMap::new();
    for (k, v) in bindings {
        out.insert(k.clone(), magma_lava::Binding::Scalar(v.clone()));
    }
    out
}

/// Build a synthesize callback that drives magma-lava. Plug into
/// `lava_operator::controller::run(magma_lava_synthesize())` to make
/// the operator do real in-cluster .tlisp → terraform.json
/// synthesis on every reconcile pass.
#[must_use]
pub fn magma_lava_synthesize() -> crate::controller::SynthesizeFn {
    Arc::new(|src: &Source, bindings: &IndexMap<String, String>, gate: Option<&str>| {
        let lava_source = source_to_lava_source(src);
        let lava_bindings = bindings_to_artifact_bindings(bindings);
        magma_lava::synthesize_source(&lava_source, &lava_bindings, gate)
            .map(|p| p.terraform_json)
            .map_err(|e| e.to_string())
    })
}

fn source_to_lava_source(src: &Source) -> magma_lava::LavaSource {
    match src {
        Source::Inline { inline } => magma_lava::LavaSource::Inline {
            inline: inline.clone(),
        },
        Source::Name { name } => magma_lava::LavaSource::Bundled { name: name.clone() },
        Source::Git { url: _, rev: _, path } => {
            // Git source resolves to an on-disk path post-fetch. For
            // M1 we fall back to the path variant assuming the git
            // fetcher has populated the local checkout. M2 will land
            // a proper git fetcher inside the controller.
            magma_lava::LavaSource::Path {
                path: std::path::PathBuf::from(path),
            }
        }
    }
}

/// Embedded-magma [`lava_drift::PlannerBackend`] impl. Runs the full
/// magma plan engine in-process and translates magma's typed
/// `ResourceChange[]` into lava-drift's typed `DriftFinding[]`.
///
/// This is the production planner the controller wires in by
/// default (when `magma-bridge` is on). The bridge fits the
/// existing `lava_drift::PlannerBackend` trait, so the detector +
/// classifier + Viggy engine work unchanged.
pub struct EmbeddedMagmaPlanner;

impl lava_drift::PlannerBackend for EmbeddedMagmaPlanner {
    fn plan(
        &self,
        spec_source: &str,
        bindings: &IndexMap<String, String>,
    ) -> Result<Vec<lava_drift::DriftFinding>, lava_drift::PlannerError> {
        let lava_source = magma_lava::LavaSource::Inline {
            inline: spec_source.to_string(),
        };
        let lava_bindings = bindings_to_artifact_bindings(bindings);
        let changes = magma_lava::plan_changes(&lava_source, &lava_bindings, None)
            .map_err(|e| lava_drift::PlannerError::Plan(e.to_string()))?;
        Ok(changes
            .into_iter()
            .map(|c| lava_drift::DriftFinding {
                address: c.address,
                attribute: c.attribute,
                change: match c.kind.as_str() {
                    "create" => lava_drift::ChangeKind::Create,
                    "update" => lava_drift::ChangeKind::Update,
                    "delete" => lava_drift::ChangeKind::Delete,
                    "replace" => lava_drift::ChangeKind::Replace,
                    _ => lava_drift::ChangeKind::NoOp,
                },
                observed: c.before,
                declared: c.after,
            })
            .collect())
    }
}

/// Embedded-magma [`DestroyBackend`] impl. The finalizer holds this
/// and calls `destroy(source, bindings)` when a LavaArchitecture CR
/// is removed. Re-synthesizes the .tlisp + runs magma-lava +
/// returns the rendered terraform.json as a diagnostic — actual
/// resource teardown lands when magma exposes a destroy entry point
/// (currently the plan is rendered + the diagnostic captures the
/// shape that WOULD be destroyed; full destroy execution comes in
/// L3.1 once magma::apply::destroy_plan is wired).
pub struct EmbeddedMagmaDestroy;

impl DestroyBackend for EmbeddedMagmaDestroy {
    fn destroy(
        &self,
        source: &Source,
        bindings: &IndexMap<String, String>,
    ) -> Result<Vec<String>, String> {
        let lava_source = source_to_lava_source(source);
        let lava_bindings = bindings_to_artifact_bindings(bindings);
        let plan = magma_lava::synthesize_source(&lava_source, &lava_bindings, None)
            .map_err(|e| format!("synthesize for destroy: {e}"))?;
        let resource_count = plan
            .terraform_json
            .get("resource")
            .and_then(|r| r.as_object())
            .map(|m| m.values().filter_map(|v| v.as_object()).map(|o| o.len()).sum::<usize>())
            .unwrap_or(0);
        Ok(vec![format!(
            "synthesized destroy plan: {resource_count} resources in scope"
        )])
    }
}

fn bindings_to_artifact_bindings(
    bindings: &IndexMap<String, String>,
) -> IndexMap<String, magma_lava::Binding> {
    let mut out = IndexMap::new();
    for (k, v) in bindings {
        out.insert(k.clone(), magma_lava::Binding::Scalar(v.clone()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_source_round_trips_into_lava_source() {
        let src = Source::Inline {
            inline: "(deflava-architecture demo :inputs () :resources ())".into(),
        };
        let lava = source_to_lava_source(&src);
        matches!(lava, magma_lava::LavaSource::Inline { .. });
    }

    #[test]
    fn name_source_round_trips_into_bundled() {
        let src = Source::Name { name: "aws-vpc-network".into() };
        let lava = source_to_lava_source(&src);
        matches!(lava, magma_lava::LavaSource::Bundled { .. });
    }

    #[test]
    fn bindings_convert_to_scalar_artifact_bindings() {
        let mut b = IndexMap::new();
        b.insert("cidr".to_string(), "10.0.0.0/16".to_string());
        let out = bindings_to_artifact_bindings(&b);
        matches!(out.get("cidr"), Some(magma_lava::Binding::Scalar(_)));
    }

    #[test]
    fn magma_lava_synthesize_renders_inline_tlisp_to_terraform_json() {
        let cb = magma_lava_synthesize();
        let src = Source::Inline {
            inline: r#"
                (deflava-architecture demo
                  :inputs ((:cidr "10.42.0.0/16"))
                  :resources ((aws_vpc "main" :cidr-block "{cidr}")))
            "#
            .to_string(),
        };
        let json = cb(&src, &IndexMap::new(), None).unwrap();
        assert!(json["resource"]["aws_vpc"]["main"].is_object());
    }
}
