//! One-shot desktop-to-harness delivery for signing and authorization inputs.
//!
//! The desktop writes this document to the harness's stdin before any ACP
//! adapter is spawned. Only a non-secret marker is carried in the process
//! environment, so model-controlled children cannot recover the signer from
//! the harness's original environment block.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::io::Read;
use std::path::PathBuf;
use zeroize::Zeroize;

pub(crate) const MARKER_ENV: &str = "BUZZ_ACP_STARTUP_STDIN";
pub(crate) const SCHEMA_VERSION: &str = "buzz.acp-startup.v1";
const MAX_PAYLOAD_BYTES: u64 = 1024 * 1024;

const AMBIGUOUS_ENV: &[&str] = &[
    "BUZZ_PRIVATE_KEY",
    "BUZZ_ACP_PRIVATE_KEY",
    "NOSTR_PRIVATE_KEY",
    "BUZZ_AUTH_TAG",
    "BUZZ_ACP_JOB_GRANTS_JSON",
    "BUZZ_ACP_JOB_GRANTS_FILE",
    "BUZZ_ACP_JOB_LEDGER_DIR",
    "BUZZ_ACP_OWNER_GITHUB_LOGIN",
    "BUZZ_ACP_ALLOW_INSECURE_LOOPBACK_JOBS",
    "BUZZ_ACP_SETUP_PAYLOAD",
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartupInputs {
    schema_version: String,
    private_key: String,
    auth_tag: Option<String>,
    job_grants_json: Option<String>,
    job_grants_file: Option<PathBuf>,
    job_ledger_dir: Option<PathBuf>,
    owner_github_login: Option<String>,
    allow_insecure_loopback_jobs: bool,
    setup_payload: Option<String>,
}

impl StartupInputs {
    pub(crate) fn read() -> Result<Option<Self>> {
        match std::env::var(MARKER_ENV).as_deref() {
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Ok("1") => {}
            Ok(_) => bail!("{MARKER_ENV} must be exactly 1"),
            Err(error) => bail!("invalid {MARKER_ENV}: {error}"),
        }
        let conflicts = conflicting_names(AMBIGUOUS_ENV, |name| std::env::var_os(name).is_some());
        if !conflicts.is_empty() {
            bail!(
                "secure stdin startup cannot be combined with legacy startup environment fields: {}",
                conflicts.join(", ")
            );
        }

        let mut bytes = zeroize::Zeroizing::new(Vec::new());
        std::io::stdin()
            .lock()
            .take(MAX_PAYLOAD_BYTES + 1)
            .read_to_end(&mut bytes)
            .context("failed to read secure startup payload from stdin")?;
        if bytes.len() as u64 > MAX_PAYLOAD_BYTES {
            bail!("secure startup payload exceeds {MAX_PAYLOAD_BYTES} bytes");
        }
        Self::parse(&bytes).map(Some)
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        if bytes.len() as u64 > MAX_PAYLOAD_BYTES {
            bail!("secure startup payload exceeds {MAX_PAYLOAD_BYTES} bytes");
        }
        let payload: Self = serde_json::from_slice(bytes)
            .context("secure startup payload is not valid buzz.acp-startup.v1 JSON")?;
        if payload.schema_version != SCHEMA_VERSION {
            bail!("unsupported secure startup schema");
        }
        if payload.private_key.is_empty() || payload.private_key.len() > 256 {
            bail!("secure startup private key has an invalid length");
        }
        if payload
            .auth_tag
            .as_ref()
            .is_some_and(|value| value.len() > 64 * 1024)
        {
            bail!("secure startup auth tag is too large");
        }
        if payload
            .job_grants_json
            .as_ref()
            .is_some_and(|value| value.len() > 768 * 1024)
        {
            bail!("secure startup grant document is too large");
        }
        Ok(payload)
    }

    pub(crate) fn take_private_key(&mut self) -> String {
        std::mem::take(&mut self.private_key)
    }

    pub(crate) fn take_auth_tag(&mut self) -> Option<String> {
        self.auth_tag.take()
    }

    pub(crate) fn take_job_grants_json(&mut self) -> Option<String> {
        self.job_grants_json.take()
    }

    pub(crate) fn job_grants_file(&self) -> Option<PathBuf> {
        self.job_grants_file.clone()
    }

    pub(crate) fn job_ledger_dir(&self) -> Option<PathBuf> {
        self.job_ledger_dir.clone()
    }

    pub(crate) fn take_owner_github_login(&mut self) -> Option<String> {
        self.owner_github_login.take()
    }

    pub(crate) fn allow_insecure_loopback_jobs(&self) -> bool {
        self.allow_insecure_loopback_jobs
    }

    pub(crate) fn take_setup_payload(&mut self) -> Option<String> {
        self.setup_payload.take()
    }
}

fn conflicting_names<'a>(
    names: &'a [&'a str],
    mut is_present: impl FnMut(&str) -> bool,
) -> Vec<&'a str> {
    names
        .iter()
        .copied()
        .filter(|name| is_present(name))
        .collect()
}

impl Drop for StartupInputs {
    fn drop(&mut self) {
        self.private_key.zeroize();
        if let Some(value) = self.auth_tag.as_mut() {
            value.zeroize();
        }
        if let Some(value) = self.job_grants_json.as_mut() {
            value.zeroize();
        }
        if let Some(value) = self.owner_github_login.as_mut() {
            value.zeroize();
        }
        if let Some(value) = self.setup_payload.as_mut() {
            value.zeroize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> Vec<u8> {
        br#"{"schema_version":"buzz.acp-startup.v1","private_key":"nsec1test","auth_tag":null,"job_grants_json":null,"job_grants_file":null,"job_ledger_dir":null,"owner_github_login":null,"allow_insecure_loopback_jobs":false,"setup_payload":null}"#.to_vec()
    }

    #[test]
    fn normal_payload_parses_without_legacy_environment() {
        let mut inputs = StartupInputs::parse(&valid()).expect("valid startup payload");
        assert_eq!(inputs.take_private_key(), "nsec1test");
        assert!(inputs.take_auth_tag().is_none());
        assert!(inputs.take_setup_payload().is_none());
    }

    #[test]
    fn malformed_unknown_and_wrong_version_payloads_fail_closed() {
        for bytes in [
            b"not-json".as_slice(),
            br#"{"schema_version":"buzz.acp-startup.v0","private_key":"nsec1test","auth_tag":null,"job_grants_json":null,"job_grants_file":null,"job_ledger_dir":null,"owner_github_login":null,"allow_insecure_loopback_jobs":false,"setup_payload":null}"#,
            br#"{"schema_version":"buzz.acp-startup.v1","private_key":"nsec1test","auth_tag":null,"job_grants_json":null,"job_grants_file":null,"job_ledger_dir":null,"owner_github_login":null,"allow_insecure_loopback_jobs":false,"setup_payload":null,"extra":true}"#,
        ] {
            assert!(StartupInputs::parse(bytes).is_err());
        }
    }

    #[test]
    fn empty_key_and_oversized_fields_fail_closed() {
        let empty = String::from_utf8(valid()).unwrap().replace("nsec1test", "");
        assert!(StartupInputs::parse(empty.as_bytes()).is_err());
        assert!(StartupInputs::parse(&vec![b'x'; MAX_PAYLOAD_BYTES as usize + 1]).is_err());

        let oversized_auth = serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "private_key": "nsec1test",
            "auth_tag": "x".repeat(64 * 1024 + 1),
            "job_grants_json": null,
            "job_grants_file": null,
            "job_ledger_dir": null,
            "owner_github_login": null,
            "allow_insecure_loopback_jobs": false,
            "setup_payload": null,
        });
        assert!(StartupInputs::parse(&serde_json::to_vec(&oversized_auth).unwrap()).is_err());
    }

    #[test]
    fn setup_payload_round_trips_as_an_opaque_private_field() {
        let mut value: serde_json::Value = serde_json::from_slice(&valid()).unwrap();
        value["setup_payload"] =
            serde_json::Value::String("{\"agent_name\":\"Codexitron\"}".into());
        let mut inputs = StartupInputs::parse(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            inputs.take_setup_payload().as_deref(),
            Some("{\"agent_name\":\"Codexitron\"}")
        );
    }

    #[test]
    fn any_legacy_startup_field_is_an_ambiguity_conflict() {
        for expected in AMBIGUOUS_ENV {
            assert_eq!(
                conflicting_names(AMBIGUOUS_ENV, |name| name == *expected),
                vec![*expected]
            );
        }
    }
}
