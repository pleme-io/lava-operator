# lava-operator

Typed Kubernetes controller for the `LavaArchitecture` CRD. The
pangea-operator analog for the lava + tatara-lisp stack.

## Apply the CRD

```bash
lava-operator | kubectl apply -f -
```

## Apply a LavaArchitecture

```yaml
apiVersion: lava.pleme.io/v1alpha1
kind: LavaArchitecture
metadata:
  name: prod-vpc
  namespace: infra
spec:
  source:
    inline: |
      (deflava-architecture demo-vpc
        :inputs ((:cidr "10.42.0.0/16"))
        :resources ((aws-vpc "main" :cidr-block "{cidr}")))
  bindings:
    cidr: 10.42.0.0/16
  gate: aws-vpc-network
  engine: embedded
```

## Reconcile loop

1. Resolve `spec.source` (inline / bundled name / git ref).
2. Render via `magma-lava::synthesize` (in-memory, embedded).
3. Optional typed-Interface gate before evaluation.
4. Engine: embedded magma (default) or shell out to tofu/terraform.
5. Update `.status` with typed `Phase` + `Condition`s.

## What this crate ships today (M0)

- Typed CRD spec + status (`LavaArchitecture`, `Spec`, `Status`,
  `Source`, `Phase`, `Condition`)
- Pure-Rust state machine `reconcile(spec, synthesize_fn)` —
  testable without a live cluster
- `crd_yaml()` emits the CustomResourceDefinition manifest

M1 wraps this with `kube-rs` + the magma-lava synthesize callback
+ engine dispatch + `kubectl apply` integration.
