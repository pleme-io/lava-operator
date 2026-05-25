//! lava-operator binary — emits the CRD manifest to stdout. The
//! kube-rs controller loop arrives in M1 when this crate gains the
//! kube-rs dep + magma-lava import for in-cluster synthesis.

fn main() {
    print!("{}", lava_operator::crd_yaml());
}
