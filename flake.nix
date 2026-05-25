{
  description = "lava-operator — typed Kubernetes controller for the LavaArchitecture CRD. Embedded magma + drift detection + OutcomeChain + AnomalyController + 7-beat Viggy tick.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  # rust-tool-image-flake wires:
  #   - per-arch container image build via dockerTools
  #   - `nix run .#release` — push every arch image to
  #     ghcr.io/pleme-io/lava-operator via forge
  #
  # The `controller` feature is enabled by default so the published
  # image carries the kube-rs reconcile loop (without it the binary
  # only emits CRDs). magma-bridge is enabled so .tlisp synthesis
  # happens fully in-process per the embedded-magma directive.
  outputs = { self, nixpkgs, crate2nix, flake-utils, fenix, substrate, forge, ... }:
    (import "${substrate}/lib/build/rust/tool-image-flake.nix" {
      inherit nixpkgs crate2nix flake-utils fenix forge;
    }) {
      toolName = "lava-operator";
      src = self;
      repo = "pleme-io/lava-operator";
      tag = "0.5.1";
      architectures = [ "amd64" "arm64" ];
      cargoFeatures = [ "controller" "magma-bridge" ];
    };
}
