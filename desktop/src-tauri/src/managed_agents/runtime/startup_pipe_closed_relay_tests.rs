//! Real closed-relay regression for desktop-minted managed-agent credentials.

use std::{
    fs::File,
    io::{Read as _, Write as _},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

use nostr::Keys;
use zeroize::Zeroizing;

use super::StartupPayload;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_HARNESS_LOG_BYTES: u64 = 256 * 1024;

struct ReapingChild(Child);

impl Drop for ReapingChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

struct EnvGuard {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(name: &'static str, value: &std::path::Path) -> Self {
        let previous = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.name, value),
            None => std::env::remove_var(self.name),
        }
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for this ignored test"))
}

fn read_bounded_log(path: &std::path::Path) -> Result<String, String> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|error| format!("open harness log: {error}"))?
        .take(MAX_HARNESS_LOG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read harness log: {error}"))?;
    if bytes.len() as u64 > MAX_HARNESS_LOG_BYTES {
        return Err(format!(
            "harness log exceeded the {MAX_HARNESS_LOG_BYTES}-byte test bound"
        ));
    }
    String::from_utf8(bytes).map_err(|error| format!("harness log was not UTF-8: {error}"))
}

fn run_real_harness(
    acp_bin: &std::path::Path,
    relay_url: &str,
    private_key_nsec: &str,
    auth_tag: &str,
    log_path: &std::path::Path,
) -> Result<(ExitStatus, String), String> {
    let stdout = File::create(log_path).map_err(|error| format!("create harness log: {error}"))?;
    let stderr = stdout
        .try_clone()
        .map_err(|error| format!("clone harness log: {error}"))?;
    let mut command = Command::new(acp_bin);
    command
        .env("BUZZ_RELAY_URL", relay_url)
        .env("BUZZ_ACP_LAZY_POOL", "true")
        .env("BUZZ_ACP_EXIT_AFTER_INACTIVITY", "1")
        .env("BUZZ_ACP_HEARTBEAT", "0")
        .env("BUZZ_ACP_PRESENCE", "false")
        .env("BUZZ_ACP_TYPING", "false")
        .env("RUST_LOG", "buzz_acp=info")
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    StartupPayload::configure_command(&mut command);
    let payload = StartupPayload::capture(private_key_nsec, Some(auth_tag), None)?;
    let mut child = ReapingChild(
        command
            .spawn()
            .map_err(|error| format!("spawn real buzz-acp: {error}"))?,
    );
    payload.deliver(&mut child.0)?;

    let deadline = Instant::now() + PROCESS_TIMEOUT;
    let status = loop {
        if let Some(status) = child
            .0
            .try_wait()
            .map_err(|error| format!("inspect buzz-acp: {error}"))?
        {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "buzz-acp did not exit within {} seconds",
                PROCESS_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let log = read_bounded_log(log_path)?;
    Ok((status, log))
}

fn direct_membership_and_owner(
    database_url: &str,
    relay_host: &str,
    agent_pubkey: &str,
    log_path: &std::path::Path,
) -> Result<(bool, Option<String>), String> {
    let query = "SELECT \
      (EXISTS(SELECT 1 FROM relay_members rm JOIN communities c ON c.id = rm.community_id \
        WHERE lower(c.host) = lower(:'relay_host') AND rm.pubkey = :'agent_pubkey'))::int::text \
      || '|' || COALESCE((SELECT encode(u.agent_owner_pubkey, 'hex') \
        FROM users u JOIN communities c ON c.id = u.community_id \
        WHERE lower(c.host) = lower(:'relay_host') \
          AND u.pubkey = decode(:'agent_pubkey', 'hex')), '')";
    let stdout = File::create(log_path).map_err(|error| format!("create psql log: {error}"))?;
    let stderr = stdout
        .try_clone()
        .map_err(|error| format!("clone psql log: {error}"))?;
    let relay_variable = format!("relay_host={relay_host}");
    let agent_variable = format!("agent_pubkey={agent_pubkey}");
    let mut command = Command::new("psql");
    command
        .args([
            "-qtAX",
            "-v",
            "ON_ERROR_STOP=1",
            "-v",
            relay_variable.as_str(),
            "-v",
            agent_variable.as_str(),
            database_url,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut child = ReapingChild(
        command
            .spawn()
            .map_err(|error| format!("spawn psql query: {error}"))?,
    );
    child
        .0
        .stdin
        .take()
        .ok_or_else(|| "psql stdin pipe missing".to_string())?
        .write_all(query.as_bytes())
        .map_err(|error| format!("write psql query: {error}"))?;
    let deadline = Instant::now() + PROCESS_TIMEOUT;
    let status = loop {
        if let Some(status) = child
            .0
            .try_wait()
            .map_err(|error| format!("inspect psql query: {error}"))?
        {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "psql query did not exit within {} seconds",
                PROCESS_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let output = read_bounded_log(log_path)?;
    if !status.success() {
        return Err(format!("psql query failed: {output}"));
    }
    let (direct, owner) = output
        .trim()
        .split_once('|')
        .ok_or_else(|| format!("unexpected psql result: {output:?}"))?;
    let direct = match direct {
        "0" => false,
        "1" => true,
        _ => return Err(format!("unexpected membership result: {direct:?}")),
    };
    let owner = (!owner.is_empty()).then(|| owner.to_string());
    Ok((direct, owner))
}

/// Run with a real relay started by `scripts/start-relay-for-tests.sh`, with
/// membership and NIP-OA enabled. The no-default-features gate prevents this
/// test from touching a developer's OS keyring while exercising save/reload.
#[test]
#[ignore = "requires psql, Postgres, Redis, a real closed buzz-relay, and a built buzz-acp"]
fn desktop_minted_agent_uses_owner_membership_without_a_direct_roster_row() {
    assert!(
        !cfg!(feature = "system-keyring"),
        "run this ignored test with --no-default-features"
    );
    let _process_env = crate::managed_agents::lock_env_mutex();
    let temp = tempfile::tempdir().expect("temporary closed-relay test directory");
    let home = temp.path().join("home");
    std::fs::create_dir_all(&home).expect("create isolated test home");
    let _home = EnvGuard::set("HOME", &home);
    let _xdg = EnvGuard::set("XDG_DATA_HOME", &home);

    let relay_url = required_env("RELAY_URL");
    let database_url = required_env("BUZZ_TEST_DATABASE_URL");
    let acp_bin = std::path::PathBuf::from(required_env("BUZZ_TEST_ACP_BIN"));
    assert!(acp_bin.is_file(), "BUZZ_TEST_ACP_BIN must name a real file");
    let relay = url::Url::parse(&relay_url).expect("valid RELAY_URL");
    let relay_host = relay[url::Position::BeforeHost..url::Position::AfterPort].to_string();

    let owner_secret = Zeroizing::new(required_env("BUZZ_TEST_OWNER_PRIVATE_KEY"));
    let owner_keys = Keys::parse(owner_secret.as_str()).expect("valid test owner key");
    assert_eq!(
        owner_keys.public_key().to_hex(),
        required_env("RELAY_OWNER_PUBKEY"),
        "test signer must be the relay's configured member owner"
    );

    // This is the production credential seam called by create_managed_agent.
    let agent_keys = Keys::generate();
    let minted = crate::attest_managed_agent_identity(&owner_keys, &agent_keys)
        .expect("desktop managed-agent identity mint");

    // Persist and reload the exact credentials before launching the harness.
    let app = tauri::test::mock_builder()
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .expect("mock desktop app builds headless");
    let mut record = super::super::test_fixtures::fixture(
        crate::managed_agents::RespondTo::OwnerOnly,
        Vec::new(),
        Some(minted.auth_tag.clone()),
    );
    record.pubkey = minted.pubkey.clone();
    record.name = "closed-relay-managed-agent".into();
    record.private_key_nsec = minted.private_key_nsec.clone();
    crate::managed_agents::save_managed_agents(app.handle(), &[record])
        .expect("persist desktop managed agent");
    let persisted = crate::managed_agents::load_managed_agents(app.handle())
        .expect("reload desktop managed agent")
        .into_iter()
        .find(|record| record.pubkey == minted.pubkey)
        .expect("persisted agent exists");
    assert_eq!(
        persisted.auth_tag.as_deref(),
        Some(minted.auth_tag.as_str())
    );
    assert_eq!(persisted.private_key_nsec, minted.private_key_nsec);

    let accepted_log = temp.path().join("accepted.log");
    let (accepted_status, accepted_output) = run_real_harness(
        &acp_bin,
        &relay_url,
        &persisted.private_key_nsec,
        persisted.auth_tag.as_deref().expect("persisted auth tag"),
        &accepted_log,
    )
    .unwrap_or_else(|error| panic!("accepted managed-agent harness failed: {error}"));
    assert!(
        accepted_status.success(),
        "owner-backed managed agent must connect; harness log: {accepted_output}"
    );
    assert!(
        accepted_output.contains("connected to relay"),
        "real buzz-acp never reported a successful relay connection: {accepted_output}"
    );

    let (direct_member, durable_owner) = direct_membership_and_owner(
        &database_url,
        &relay_host,
        &minted.pubkey,
        &temp.path().join("accepted-db.log"),
    )
    .expect("inspect accepted managed-agent admission");
    assert!(
        !direct_member,
        "owner-backed admission must not create an agent relay_members row"
    );
    let owner_pubkey = owner_keys.public_key().to_hex();
    assert_eq!(durable_owner.as_deref(), Some(owner_pubkey.as_str()));

    let nonmember_owner = Keys::generate();
    let denied_agent_keys = Keys::generate();
    let denied = crate::attest_managed_agent_identity(&nonmember_owner, &denied_agent_keys)
        .expect("mint valid credentials for nonmember owner");
    let denied_log = temp.path().join("denied.log");
    let (denied_status, denied_output) = run_real_harness(
        &acp_bin,
        &relay_url,
        &denied.private_key_nsec,
        &denied.auth_tag,
        &denied_log,
    )
    .unwrap_or_else(|error| panic!("denied managed-agent harness failed: {error}"));
    assert!(
        !denied_status.success(),
        "nonmember owner's agent was admitted"
    );
    assert!(
        denied_output.contains("restricted: not a relay member"),
        "closed relay did not return the membership rejection: {denied_output}"
    );
    let (denied_direct_member, denied_durable_owner) = direct_membership_and_owner(
        &database_url,
        &relay_host,
        &denied.pubkey,
        &temp.path().join("denied-db.log"),
    )
    .expect("inspect rejected managed-agent admission");
    assert!(!denied_direct_member);
    assert!(denied_durable_owner.is_none());
}
