{
  description = "lava-operator — typed Kubernetes controller for the LavaArchitecture CRD. Embedded magma + drift detection + OutcomeChain + AnomalyController + 7-beat Viggy tick.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
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

  # Image built via gen (genBuild = true → substrate's lockfile-builder),
  # NOT crate2nix. Git-dep source hashes come from Cargo.gen.lock — the
  # hashes the real fetcher computes — so the image build is immune to the
  # crate2nix-vs-fetchgit hash drift that froze :latest at 2026-05-27. The
  # `controller` + `magma-bridge` Cargo.toml default features carry the
  # kube-rs reconcile loop + in-process magma synthesis into the image.
  #
  #   nix run .#release — push every arch image to ghcr.io/pleme-io/lava-operator
  outputs = { self, nixpkgs, flake-utils, fenix, substrate, forge, ... }:
    (import "${substrate}/lib/build/rust/tool-image-flake.nix" {
      inherit nixpkgs flake-utils fenix forge;
    }) {
      toolName = "lava-operator";
      src = self;
      repo = "pleme-io/lava-operator";
      tag = "0.5.1";
      architectures = [ "amd64" "arm64" ];
      genBuild = true;
    };
}
