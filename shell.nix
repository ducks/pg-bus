let
  # oxalica/rust-overlay, pinned: rust-toolchain.toml's version comes from
  # its manifests. Bump the rev (and sha256) to reach a newer release.
  rust-overlay = import (fetchTarball {
    url = "https://github.com/oxalica/rust-overlay/archive/368fee9beaab04ca6fe7af28db63caa9badb22fa.tar.gz";
    sha256 = "153wynqcjizxi46vh9xxf1rw5z7b59jhchma5mnxzv3iyhvwrgg6";
  });
in

{ pkgs ? import <nixpkgs> { overlays = [ rust-overlay ]; } }:

let
  rust = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
  postgres = import ./nix/postgres.nix {
    inherit pkgs;
    port = 5443;
  };
in

pkgs.mkShell {
  buildInputs = [
    rust
    pkgs.cargo-nextest
  ] ++ postgres.buildInputs;

  shellHook = ''
    ${postgres.shellHook}

    echo "pg-bus: $(rustc --version), $(postgres --version)"
    echo "  db_start / db_stop / db_status   local PostgreSQL (tests need it running)"
  '';
}
