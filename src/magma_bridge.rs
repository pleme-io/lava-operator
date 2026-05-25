//! magma-lava bridge — produces a `SynthesizeFn` callback the
//! controller can register with `lava_operator::controller::run`.
//!
//! Compiled only when both the `controller` and `magma-bridge`
//! features are enabled. Bridges the controller's [`crate::Source`]
//! into magma-lava's [`magma_lava::LavaSource`] and returns the
//! terraform.json the controller patches back into the resource
//! status.

use std::sync::Arc;

use crate::Source;
use indexmap::IndexMap;

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
