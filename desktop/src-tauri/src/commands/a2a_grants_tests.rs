use super::*;

#[cfg(unix)]
fn executable_script(root: &Path, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;

    let path = root.join("fake-git");
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("write fake git");
    let mut permissions = path.metadata().expect("fake git metadata").permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&path, permissions).expect("make fake git executable");
    path
}

#[cfg(unix)]
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\\''"))
}

#[derive(Default)]
struct MemoryAuthorityStore {
    value: Mutex<Option<String>>,
}

impl AuthorityStore for MemoryAuthorityStore {
    fn load(&self) -> Result<Option<Zeroizing<String>>, String> {
        self.value
            .lock()
            .map_err(|_| "test authority lock".to_string())
            .map(|value| value.clone().map(Zeroizing::new))
    }

    fn store_verified(&self, value: &str) -> Result<(), String> {
        *self
            .value
            .lock()
            .map_err(|_| "test authority lock".to_string())? = Some(value.to_string());
        Ok(())
    }
}

fn scope(root: &Path) -> A2aGrantScopeInput {
    A2aGrantScopeInput {
        repos_dir: root.parent().map(|path| path.display().to_string()),
        project_dtag: "nemo".into(),
        project_address: format!("30621:{}:nemo", "a".repeat(64)),
        home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
        repository: "https://github.com/mysteropodes/nemo".into(),
    }
}

fn stored(root: &Path) -> StoredGrant {
    let root = root.canonicalize().expect("canonical checkout root");
    StoredGrant {
        project_address: format!("30621:{}:nemo", "a".repeat(64)),
        home_channel: "3580ca9b-47b4-4af9-b22a-1068778f26c6".into(),
        repository: "https://github.com/mysteropodes/nemo".into(),
        requester_pubkeys: vec!["b".repeat(64)],
        capabilities: vec!["rust.review".into()],
        path_prefixes: vec!["src".into()],
        base_sha: "c".repeat(40),
        branch: "codex/test".into(),
        worktree_id: "nemo-review".into(),
        checkout_root: root,
    }
}

#[test]
fn grant_mutation_scope_must_match_the_workspace_project_exactly() {
    let temp = tempfile::tempdir().expect("temp root");
    let root = temp.path().join("nemo");
    let requested = scope(&root);
    let project = crate::managed_agents::WorkspaceProject {
        project_address: requested.project_address.clone(),
        home_channel: requested.home_channel.clone(),
        repository: requested.repository.clone(),
        display_name: "Nemo".into(),
        instruction_revision: "d".repeat(40),
    };
    assert!(workspace_project_matches_scope(&project, &requested));
    let mut mismatched = scope(&root);
    mismatched.repository = "https://github.com/another/repository".into();
    assert!(!workspace_project_matches_scope(&project, &mismatched));
}

#[test]
fn github_remote_canonicalization_accepts_https_and_ssh_only() {
    for input in [
        "https://github.com/Mysteropodes/Nemo.git",
        "git@github.com:Mysteropodes/Nemo.git",
    ] {
        assert_eq!(
            canonical_github_remote(input).as_deref(),
            Ok("https://github.com/mysteropodes/nemo")
        );
    }
    for input in [
        "http://github.com/mysteropodes/nemo",
        "https://github.com/mysteropodes/nemo/issues",
        "https://token@github.com/mysteropodes/nemo",
        "https://github.com.evil.test/mysteropodes/nemo",
    ] {
        assert!(canonical_github_remote(input).is_err(), "{input}");
    }
}

#[cfg(unix)]
#[test]
fn local_git_inspection_kills_descendants_at_its_deadline() {
    let temp = tempfile::tempdir().expect("temp root");
    let marker = temp.path().join("escaped-descendant");
    let script = executable_script(
        temp.path(),
        &format!(
            "(sleep 1; printf escaped > {}) &\nsleep 30",
            shell_quote(&marker)
        ),
    );
    let started = std::time::Instant::now();

    let error = git_output_with_limits(&script, temp.path(), &[], Duration::from_millis(75), 1024)
        .expect_err("deadline must stop fake git");

    assert_eq!(
        error,
        "local Git inspection exceeded its wall-clock deadline"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
    std::thread::sleep(Duration::from_millis(1_100));
    assert!(!marker.exists(), "the background descendant survived");
}

#[cfg(unix)]
#[test]
fn local_git_inspection_stops_streaming_output_at_its_capture_limit() {
    let temp = tempfile::tempdir().expect("temp root");
    let script = executable_script(temp.path(), "while :; do printf 0123456789abcdef; done");

    let error = git_output_with_limits(&script, temp.path(), &[], Duration::from_secs(2), 512)
        .expect_err("capture limit must stop fake git");

    assert_eq!(error, "local Git inspection exceeded its output limit");
}

#[test]
fn scope_requires_exact_project_coordinate_and_canonical_repository() {
    let temp = tempfile::tempdir().expect("temp root");
    let root = temp.path().join("nemo");
    let mut candidate = scope(&root);
    assert!(validate_scope(&candidate).is_ok());
    candidate.project_dtag = "other".into();
    assert!(validate_scope(&candidate).is_err());
    candidate = scope(&root);
    candidate.repository.push_str(".git");
    assert!(validate_scope(&candidate).is_err());
}

#[test]
fn new_capability_and_worktree_ids_are_narrow_tokens() {
    for value in ["rust", "rust.review", "coord_smoke-1"] {
        assert!(validate_new_capability(value).is_ok(), "{value}");
    }
    for value in ["", "Rust", "rust review", "../review"] {
        assert!(validate_new_capability(value).is_err(), "{value}");
    }
    assert!(validate_worktree_id("nemo-review.1").is_ok());
    assert!(validate_worktree_id("../nemo").is_err());
}

#[test]
fn stored_document_serializes_the_exact_consumer_schema() {
    let temp = tempfile::tempdir().expect("temp root");
    let root = temp.path().join("repo");
    std::fs::create_dir(&root).expect("checkout root");
    std::fs::create_dir(root.join("src")).expect("path prefix");
    let document = GrantDocument {
        version: 1,
        grants: vec![stored(&root)],
    };
    let value = serde_json::to_value(document).expect("serialize grant document");
    let grant = &value["grants"][0];
    let keys = grant
        .as_object()
        .expect("grant object")
        .keys()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    assert_eq!(
        keys,
        HashSet::from([
            "project_address",
            "home_channel",
            "repository",
            "requester_pubkeys",
            "capabilities",
            "path_prefixes",
            "base_sha",
            "branch",
            "worktree_id",
            "checkout_root",
        ])
    );
}

#[test]
fn atomic_store_roundtrips_and_is_owner_only_on_unix() {
    let temp = tempfile::tempdir().expect("temp root");
    let checkout = temp.path().join("repo");
    std::fs::create_dir(&checkout).expect("checkout root");
    std::fs::create_dir(checkout.join("src")).expect("path prefix");
    let path = temp.path().join("settings/a2a/agent-job-grants.json");
    std::fs::create_dir(path.parent().and_then(Path::parent).expect("settings root"))
        .expect("settings root");
    let store = MemoryAuthorityStore::default();
    let (_, authority) = load_authorized_document(&store, &path).expect("initialize authority");
    let document = GrantDocument {
        version: 1,
        grants: vec![stored(&checkout)],
    };
    write_authorized_document(&store, &path, &document, authority)
        .expect("atomic authenticated grant write");
    let (loaded, _) =
        load_authorized_document(&store, &path).expect("load authenticated grant document");
    assert_eq!(loaded.grants.len(), 1);
    assert_eq!(loaded.grants[0].requester_pubkeys, vec!["b".repeat(64)]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            path.metadata()
                .expect("grant metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn model_writable_file_tampering_never_changes_authority() {
    let temp = tempfile::tempdir().expect("temp root");
    let checkout = temp.path().join("repo");
    std::fs::create_dir(&checkout).expect("checkout root");
    std::fs::create_dir(checkout.join("src")).expect("path prefix");
    let path = temp.path().join("settings/a2a/agent-job-grants.json");
    std::fs::create_dir(path.parent().and_then(Path::parent).expect("settings root"))
        .expect("settings root");
    let store = MemoryAuthorityStore::default();
    let (_, authority) = load_authorized_document(&store, &path).expect("initialize authority");
    let document = GrantDocument {
        version: 1,
        grants: vec![stored(&checkout)],
    };
    write_authorized_document(&store, &path, &document, authority)
        .expect("write authenticated grant");

    let mut forged = document;
    forged.grants[0].requester_pubkeys = vec!["d".repeat(64)];
    write_document_file(&path, &forged).expect("simulate same-user file tamper");
    let error = load_authorized_document(&store, &path)
        .err()
        .expect("tampered file must fail closed");
    assert!(error.contains("modified or rolled back"), "{error}");
}

#[test]
fn revoked_grant_cannot_be_restored_by_replaying_its_old_file() {
    let temp = tempfile::tempdir().expect("temp root");
    let checkout = temp.path().join("repo");
    std::fs::create_dir(&checkout).expect("checkout root");
    std::fs::create_dir(checkout.join("src")).expect("path prefix");
    let path = temp.path().join("settings/a2a/agent-job-grants.json");
    std::fs::create_dir(path.parent().and_then(Path::parent).expect("settings root"))
        .expect("settings root");
    let store = MemoryAuthorityStore::default();
    let (_, initial) = load_authorized_document(&store, &path).expect("initialize authority");
    let granted = GrantDocument {
        version: 1,
        grants: vec![stored(&checkout)],
    };
    write_authorized_document(&store, &path, &granted, initial).expect("write grant revision");
    let (_, granted_authority) =
        load_authorized_document(&store, &path).expect("load grant revision");
    write_authorized_document(&store, &path, &empty_document(), granted_authority)
        .expect("revoke grant");

    write_document_file(&path, &granted).expect("replay old grant file");
    assert!(load_authorized_document(&store, &path).is_err());
}

#[test]
fn pending_keychain_revision_recovers_without_widening_access() {
    let temp = tempfile::tempdir().expect("temp root");
    let checkout = temp.path().join("repo");
    std::fs::create_dir(&checkout).expect("checkout root");
    std::fs::create_dir(checkout.join("src")).expect("path prefix");
    let path = temp.path().join("settings/a2a/agent-job-grants.json");
    std::fs::create_dir(path.parent().and_then(Path::parent).expect("settings root"))
        .expect("settings root");
    let store = MemoryAuthorityStore::default();
    let (_, mut authority) = load_authorized_document(&store, &path).expect("initialize authority");
    let document = GrantDocument {
        version: 1,
        grants: vec![stored(&checkout)],
    };
    let pending = slot_for(
        &authority.secret_hex,
        authority.current.revision + 1,
        &document,
    )
    .expect("pending slot");
    authority.pending = Some(pending.clone());
    persist_authority(&store, &authority).expect("persist pending authority");
    write_document_file(&path, &document).expect("simulate crash after file commit");

    let (recovered, recovered_authority) =
        load_authorized_document(&store, &path).expect("recover pending commit");
    assert_eq!(recovered.grants.len(), 1);
    assert_eq!(recovered_authority.current.revision, pending.revision);
    assert!(recovered_authority.pending.is_none());
}

#[cfg(unix)]
#[test]
fn allowed_paths_reject_symlinks_even_when_the_target_is_inside_checkout() {
    use std::os::unix::fs::symlink;

    let temp = tempfile::tempdir().expect("temp root");
    let root = temp.path().join("repo");
    std::fs::create_dir(&root).expect("checkout root");
    std::fs::create_dir(root.join("real")).expect("real path");
    symlink(root.join("real"), root.join("linked")).expect("path symlink");
    assert!(validate_safe_paths(&root, &["real".into()]).is_ok());
    assert!(validate_safe_paths(&root, &["linked".into()]).is_err());
    assert!(validate_safe_paths(&root, &["missing".into()]).is_err());
    assert!(validate_safe_paths(&root, &[".git".into()]).is_err());
}

#[test]
fn peer_must_have_a_verified_owner_and_project_channel_membership() {
    let channel = "3580ca9b-47b4-4af9-b22a-1068778f26c6";
    let pubkey = "b".repeat(64);
    let mut peer = RelayAgentInfo {
        pubkey: pubkey.clone(),
        owner_pubkey: Some("c".repeat(64)),
        name: "Reviewer".into(),
        agent_type: "agent".into(),
        channels: Vec::new(),
        channel_ids: vec![channel.into()],
        capabilities: Vec::new(),
        status: "online".into(),
        respond_to: None,
        respond_to_allowlist: Vec::new(),
    };
    assert!(verify_selected_peer(&[peer.clone()], &pubkey, channel).is_ok());
    peer.owner_pubkey = None;
    assert!(verify_selected_peer(&[peer.clone()], &pubkey, channel).is_err());
    peer.owner_pubkey = Some("c".repeat(64));
    peer.channel_ids.clear();
    assert!(verify_selected_peer(&[peer], &pubkey, channel).is_err());
}
