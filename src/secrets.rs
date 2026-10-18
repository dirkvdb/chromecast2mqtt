//! SecretSpec integration for credentials not supplied through CLI, environment,
//! or the legacy INI file. Values are never exported into the process environment.

use std::{env, path::Path};

use anyhow::{bail, ensure, Context, Result};
use secretspec::{CallerContext, NamedResolution, Secrets, Spec};

const MANIFEST: &str = include_str!("../secretspec.toml");
const MQTT_PASSWORD: &str = "C2M_MQTT_PASS";

/// Resolve the MQTT password using the SecretSpec Rust SDK.
///
/// The embedded manifest makes installed binaries and containers independent of
/// the source checkout. An explicit SECRETSPEC_FILE can override that manifest.
/// Provider, profile, scope, reason, and user configuration follow SDK policy.
/// Missing credentials fail without interactive provisioning.
pub fn mqtt_password() -> Result<String> {
    let resolver = match env::var_os("SECRETSPEC_FILE") {
        Some(path) => Secrets::load_from(Path::new(&path)),
        None => Secrets::from_spec(Spec::from_toml(MANIFEST)?),
    }
    .context("loading SecretSpec configuration")?
    .with_caller(
        CallerContext::new(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_operation("mqtt_connect"),
    );

    match resolver
        .resolve_named_bytes(MQTT_PASSWORD)
        .context("resolving MQTT password through SecretSpec")?
    {
        NamedResolution::Resolved(secret) => {
            let value = secret
                .value
                .context("C2M_MQTT_PASS must be an inline SecretSpec value, not as_path")?;
            let password = value.try_as_utf8_for(MQTT_PASSWORD)?;
            ensure!(
                !password.is_empty(),
                "SecretSpec C2M_MQTT_PASS must not be empty"
            );
            Ok(password.to_owned())
        }
        NamedResolution::Missing { .. } => bail!(
            "C2M_MQTT_PASS is missing from the selected SecretSpec provider/profile; \
             provision it with `secretspec set C2M_MQTT_PASS`, or supply --mqtt-pass/C2M_MQTT_PASS"
        ),
        NamedResolution::Undeclared => {
            bail!("C2M_MQTT_PASS is not declared in the selected SecretSpec profile/scope")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_manifest_declares_password_in_both_supported_profiles() {
        let spec = Spec::from_toml(MANIFEST).unwrap();
        for profile in ["default", "development"] {
            let schema = spec.schema_json(Some(profile)).unwrap();
            let schema: serde_json::Value = serde_json::from_str(&schema).unwrap();
            assert!(schema["properties"].get(MQTT_PASSWORD).is_some());
            assert!(schema["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|name| name == MQTT_PASSWORD));
        }
    }
}
