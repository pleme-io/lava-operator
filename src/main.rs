//! lava-operator binary.
//!
//! Subcommands:
//!   - `crd`  — emit the LavaArchitecture CRD YAML to stdout
//!   - `run`  — start the kube-rs controller loop (requires the
//!              `controller` feature at build time)
//!
//! With no args, defaults to `crd` for backward compatibility with
//! `lava-operator > crd.yaml` pipelines.

fn main() {
    let cmd = std::env::args().nth(1).unwrap_or_else(|| "crd".to_string());
    match cmd.as_str() {
        "crd" => print!("{}", lava_operator::crd_yaml()),
        "run" => run_controller(),
        other => {
            eprintln!("unknown subcommand `{other}` (expected: crd | run)");
            std::process::exit(2);
        }
    }
}

#[cfg(feature = "controller")]
fn run_controller() {
    use std::sync::Arc;
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let synthesize: lava_operator::controller::SynthesizeFn =
        Arc::new(|_src, _b, _g| {
            // Placeholder: the magma-lava bridge plugs in here. Until
            // that crate ships, the controller stubs out as an
            // empty terraform.json so the reconcile loop is exercisable
            // end-to-end against a real cluster.
            Ok(serde_json::json!({ "resource": {} }))
        });
    if let Err(e) = rt.block_on(lava_operator::controller::run(synthesize)) {
        eprintln!("controller exited with error: {e}");
        std::process::exit(1);
    }
}

#[cfg(not(feature = "controller"))]
fn run_controller() {
    eprintln!(
        "lava-operator was built without the `controller` feature. \
         Rebuild with `cargo build --features controller` to enable the \
         kube-rs reconcile loop."
    );
    std::process::exit(2);
}
