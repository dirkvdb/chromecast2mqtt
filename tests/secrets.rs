use std::{
    fs,
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

use tempfile::{tempdir, TempDir};

const PASSWORD: &str = "sdk-regression-fixture-not-a-real-secret";

struct Fixture {
    directory: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            directory: tempdir().unwrap(),
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_chromecast2mqtt"));
        command
            .env_clear()
            .current_dir(self.directory.path())
            .env("HOME", self.directory.path())
            .env("XDG_CONFIG_HOME", self.directory.path().join("config"))
            .env("XDG_DATA_HOME", self.directory.path().join("data"))
            .env("XDG_STATE_HOME", self.directory.path().join("state"))
            .env("XDG_CACHE_HOME", self.directory.path().join("cache"))
            .env("SECRETSPEC_PROVIDER", "env")
            .env("SECRETSPEC_PROFILE", "default")
            // Successful configuration must stop here, before any broker or
            // Chromecast connection. The SDK runs before TLS options are built.
            .arg("--mqtt-ca-cert")
            .arg(self.directory.path().join("missing-ca.pem"));
        command
    }

    fn dotenv_provider(&self, contents: &str) -> String {
        let path = self.directory.path().join("provider.env");
        fs::write(&path, contents).unwrap();
        format!("dotenv:{}", path.display())
    }
}

fn output(command: &mut Command) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("SecretSpec startup test exceeded five seconds");
        }
        thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(!String::from_utf8_lossy(&output.stdout).contains(PASSWORD));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(PASSWORD));
    output
}

fn assert_reached_tls_validation(output: &Output) {
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("missing-ca.pem"), "{error}");
    assert!(
        !error.contains("configuring MQTT authentication"),
        "{error}"
    );
}

#[test]
fn sdk_resolves_from_a_provider_without_a_wrapper_or_source_checkout() {
    let fixture = Fixture::new();
    let provider = fixture.dotenv_provider(&format!("C2M_MQTT_PASS={PASSWORD}\n"));
    for profile in ["default", "development"] {
        let mut command = fixture.command();
        command
            .env("SECRETSPEC_PROVIDER", &provider)
            .env("SECRETSPEC_PROFILE", profile)
            .args(["--mqtt-user", "fixture-user"]);
        assert_reached_tls_validation(&output(&mut command));
    }
}

#[test]
fn sdk_honors_external_manifest_and_reads_the_environment_provider() {
    let fixture = Fixture::new();
    let manifest = fixture.directory.path().join("external.toml");
    fs::write(
        &manifest,
        r#"
[project]
name = "chromecast2mqtt-sdk-test"
revision = "1.0"
[profiles.default]
C2M_MQTT_PASS = { description = "MQTT fixture password", required = true, ref = { item = "SDK_TEST_MQTT_PASSWORD" } }
"#,
    )
    .unwrap();
    let mut command = fixture.command();
    command
        .env("SECRETSPEC_FILE", manifest)
        .env("SDK_TEST_MQTT_PASSWORD", PASSWORD)
        .args(["--mqtt-user", "fixture-user"]);
    assert_reached_tls_validation(&output(&mut command));
}

#[test]
fn missing_credentials_fail_without_network_or_interactive_fallback() {
    let fixture = Fixture::new();
    let mut command = fixture.command();
    command.args(["--mqtt-user", "fixture-user"]);
    let result = output(&mut command);
    assert!(!result.status.success());
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(error.contains("C2M_MQTT_PASS is missing"), "{error}");
    assert!(!error.contains("missing-ca.pem"), "{error}");
}

#[test]
fn invalid_sdk_profile_is_not_silently_replaced() {
    let fixture = Fixture::new();
    let mut command = fixture.command();
    command
        .env("SECRETSPEC_PROFILE", "nonexistent")
        .args(["--mqtt-user", "fixture-user"]);
    let result = output(&mut command);
    assert!(!result.status.success());
    let error = String::from_utf8_lossy(&result.stderr);
    assert!(error.contains("nonexistent"), "{error}");
    assert!(!error.contains("missing-ca.pem"), "{error}");
}

#[test]
fn explicit_cli_and_environment_passwords_bypass_the_sdk() {
    let fixture = Fixture::new();
    for password in [PASSWORD, ""] {
        let mut command = fixture.command();
        command
            .env("SECRETSPEC_PROVIDER", "nonexistent-provider")
            .env("SECRETSPEC_PROFILE", "nonexistent")
            .args(["--mqtt-user", "fixture-user", "--mqtt-pass", password]);
        assert_reached_tls_validation(&output(&mut command));
    }
    let mut command = fixture.command();
    command
        .env("SECRETSPEC_PROVIDER", "nonexistent-provider")
        .env("SECRETSPEC_PROFILE", "nonexistent")
        .env("C2M_MQTT_PASS", PASSWORD)
        .args(["--mqtt-user", "fixture-user"]);
    assert_reached_tls_validation(&output(&mut command));
}

#[test]
fn existing_ini_password_bypasses_the_sdk() {
    let fixture = Fixture::new();
    let config = fixture.directory.path().join("legacy.ini");
    fs::write(
        &config,
        format!("[mqtt]\nuse_auth=true\nusername=fixture-user\npassword={PASSWORD}\n"),
    )
    .unwrap();
    let mut command = fixture.command();
    command
        .env("SECRETSPEC_PROVIDER", "nonexistent-provider")
        .env("SECRETSPEC_PROFILE", "nonexistent")
        .arg("--config")
        .arg(config);
    assert_reached_tls_validation(&output(&mut command));
}

#[test]
fn anonymous_mqtt_and_help_do_not_access_secret_providers() {
    let fixture = Fixture::new();
    let mut command = fixture.command();
    command
        .env("SECRETSPEC_PROVIDER", "nonexistent-provider")
        .env("SECRETSPEC_PROFILE", "nonexistent");
    assert_reached_tls_validation(&output(&mut command));

    for flag in ["--help", "--version"] {
        let mut command = fixture.command();
        command
            .env("SECRETSPEC_PROVIDER", "nonexistent-provider")
            .env("SECRETSPEC_PROFILE", "nonexistent")
            .args(["--mqtt-user", "fixture-user", flag]);
        assert!(output(&mut command).status.success());
    }
}
