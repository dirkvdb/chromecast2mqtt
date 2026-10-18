{ pkgs, ... }:

{
  dotenv.enable = true;

  # Devenv exports the provider/profile selected in devenv.yaml. Do not map
  # config.secretspec.secrets into env: the application reads values via the SDK.

  languages.rust = {
    enable = true;
    channel = "stable";
  };

  packages = with pkgs; [
    cargo-nextest
    just
    rust-analyzer
    curl
    patch
    gnutar
    gzip
    util-linux
  ];

  scripts.build.exec = "just build";
  scripts.check.exec = "just check";
  scripts.test.exec = "just test";
  scripts.run.exec = ''just run "$@"'';

  enterShell = ''
    sh scripts/prepare-rust-cast.sh
    echo "chromecast2mqtt development environment ready"
    echo "Rust: $(rustc --version)"
    echo "Cargo: $(cargo --version)"
    echo ""
    echo "Run 'just check' for the Rust quality gates."
    echo "Run 'just run' to start the bridge with SecretSpec credentials."
  '';

  enterTest = "just check";
}
