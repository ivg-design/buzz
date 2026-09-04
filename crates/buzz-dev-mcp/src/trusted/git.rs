//! Grant-bound Git operations whose signer stays outside the model shell.

use std::sync::Arc;

use nostr::ToBech32;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::scope::{path_allowed, TrustedGitCheckout};
use super::{
    PrivilegedGitDisposition, PrivilegedGitOperationReceipt, PrivilegedOperationOutcome,
    ProjectGitOperation, TrustedRelay,
};
use process::{
    fetch_branch, git_output, git_output_with_input, local_ref, push_commit, text,
    update_local_ref_reconciled, RefMutationOutcome,
};

mod process;

pub(super) use process::{
    capture_operator_github_credentials, inspect_checkout_git, GitHubCredentialStore,
};

const MAX_COMMIT_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_PUSH_COMMITS: usize = 256;
const GIT_RECEIPT_SCHEMA_VERSION: &str = "buzz.git-operation-receipt.v1";

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectGitCommitParams {
    /// Commit message for the already-staged, grant-contained changes.
    pub message: String,
}

#[derive(Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct ProjectGitParams {}

pub(super) struct GitOperationExecution {
    pub(super) result: CallToolResult,
    pub(super) outcome: PrivilegedOperationOutcome,
    pub(super) receipt: PrivilegedGitOperationReceipt,
}

pub(super) fn operation_receipt(
    relay: &TrustedRelay,
    operation: ProjectGitOperation,
    invocation_id: Uuid,
) -> Result<PrivilegedGitOperationReceipt, String> {
    if operation == ProjectGitOperation::Handoff {
        return Err("handoff does not use a Git operation receipt".into());
    }
    Ok(PrivilegedGitOperationReceipt {
        schema_version: GIT_RECEIPT_SCHEMA_VERSION.to_owned(),
        invocation_id,
        operation,
        session_channel_id: relay
            .session_channel_id
            .clone()
            .ok_or_else(|| "Project Git requires an exact session channel binding".to_owned())?,
        operation_id: relay
            .job_operation_id
            .clone()
            .ok_or_else(|| "Project Git requires an exact operation binding".to_owned())?,
        request_event_id: relay
            .job_request_event_id
            .clone()
            .ok_or_else(|| "Project Git requires an exact request binding".to_owned())?,
        worker_pubkey: relay.signer_pubkey(),
        scope_digest: None,
        repository: None,
        branch_ref: None,
        previous_object: None,
        intended_object: None,
        observed_object: None,
        disposition: PrivilegedGitDisposition::NotApplied,
    })
}

pub async fn commit(
    relay: &Arc<TrustedRelay>,
    params: ProjectGitCommitParams,
    cancellation: CancellationToken,
    mut receipt: PrivilegedGitOperationReceipt,
) -> GitOperationExecution {
    if let Err(error) = validate_commit_message(&params.message) {
        return preparation_failure(receipt, error, &cancellation);
    }
    let (checkout, request) =
        match resolve_checkout(relay, ProjectGitOperation::Commit, &cancellation).await {
            Ok(resolved) => resolved,
            Err(error) => return preparation_failure(receipt, error, &cancellation),
        };
    let branch_ref = format!("refs/heads/{}", checkout.branch);
    if let Err(error) = bind_receipt(
        &mut receipt,
        &request,
        &checkout,
        &branch_ref,
        Some(checkout.head_sha.clone()),
        None,
    ) {
        return preparation_failure(receipt, error, &cancellation);
    }

    let prepared = async {
        ensure_safe_local_config(relay, &checkout, &cancellation).await?;
        ensure_commit_range(relay, &checkout, &request, &cancellation).await?;

        // Snapshot the index into an immutable tree, then validate that exact
        // tree. A model-controlled shell may keep running beside this tool, so
        // validating `git diff --cached` and later running `git commit` would
        // leave a check/use window in which the index could be replaced.
        let tree = git_output(relay, &checkout, &["write-tree"], &cancellation).await?;
        let tree = text(&tree.stdout)?.to_owned();
        validate_object_id(&tree)?;
        let parent_tree = git_output(
            relay,
            &checkout,
            &["rev-parse", &format!("{}^{{tree}}", checkout.head_sha)],
            &cancellation,
        )
        .await?;
        if text(&parent_tree.stdout)? == tree {
            return Err("trusted Git commit requires staged changes".to_owned());
        }
        let staged = git_output(
            relay,
            &checkout,
            &[
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-z",
                "-r",
                &checkout.head_sha,
                &tree,
            ],
            &cancellation,
        )
        .await?;
        let paths = parse_nul_paths(&staged.stdout)?;
        if paths.is_empty() {
            return Err("trusted Git commit requires staged changes".to_owned());
        }
        ensure_paths_allowed(&paths, &checkout.path_prefixes, checkout.repository_wide)?;

        let message = signed_commit_message(relay, &request, &params.message)?;
        let unsigned = git_output(
            relay,
            &checkout,
            &[
                "commit-tree",
                &tree,
                "-p",
                &checkout.head_sha,
                "--no-gpg-sign",
                "-m",
                &message,
            ],
            &cancellation,
        )
        .await?;
        let unsigned_id = text(&unsigned.stdout)?.to_owned();
        validate_object_id(&unsigned_id)?;
        let raw_unsigned = git_output(
            relay,
            &checkout,
            &["cat-file", "commit", &unsigned_id],
            &cancellation,
        )
        .await?;
        let signed_object = sign_commit_payload(relay, &raw_unsigned.stdout)?;
        let created = git_output_with_input(
            relay,
            &checkout,
            &["hash-object", "-t", "commit", "-w", "--stdin"],
            signed_object,
            &cancellation,
        )
        .await?;
        let commit = text(&created.stdout)?.to_owned();
        validate_object_id(&commit)?;
        let mut created_checkout = checkout.clone();
        created_checkout.head_sha.clone_from(&commit);
        ensure_commit_range(relay, &created_checkout, &request, &cancellation).await?;

        Ok::<_, String>(commit)
    }
    .await;
    let commit = match prepared {
        Ok(commit) => commit,
        Err(error) => return preparation_failure(receipt, error, &cancellation),
    };
    let mutation = update_local_ref_reconciled(
        relay,
        &checkout,
        &branch_ref,
        Some(checkout.head_sha.clone()),
        commit.clone(),
        "commit: Buzz managed-agent change",
        &cancellation,
    )
    .await;
    mutation_execution(
        receipt,
        mutation,
        serde_json::json!({
            "accepted": true,
            "commit": commit,
            "branch": checkout.branch,
            "signed": true,
            "signer_pubkey": relay.signer_pubkey(),
        }),
        &cancellation,
    )
}

pub async fn fetch(
    relay: &Arc<TrustedRelay>,
    _params: ProjectGitParams,
    cancellation: CancellationToken,
    mut receipt: PrivilegedGitOperationReceipt,
) -> GitOperationExecution {
    let (checkout, request) =
        match resolve_checkout(relay, ProjectGitOperation::Fetch, &cancellation).await {
            Ok(resolved) => resolved,
            Err(error) => return preparation_failure(receipt, error, &cancellation),
        };
    let branch_ref = format!("refs/remotes/origin/{}", checkout.branch);
    if let Err(error) = ensure_safe_local_config(relay, &checkout, &cancellation).await {
        return preparation_failure(receipt, error, &cancellation);
    }
    let previous = match local_ref(relay, &checkout, &branch_ref, &cancellation).await {
        Ok(previous) => previous,
        Err(error) => return preparation_failure(receipt, error, &cancellation),
    };
    if let Err(error) = bind_receipt(
        &mut receipt,
        &request,
        &checkout,
        &branch_ref,
        previous.clone(),
        None,
    ) {
        return preparation_failure(receipt, error, &cancellation);
    }
    let mutation = match fetch_branch(relay, &checkout, &branch_ref, previous, &cancellation).await
    {
        Ok(mutation) => mutation,
        Err(error) => return preparation_failure(receipt, error, &cancellation),
    };
    let fetched_head = mutation.intended_object.clone();
    mutation_execution(
        receipt,
        mutation,
        serde_json::json!({
            "accepted": true,
            "branch": checkout.branch,
            "fetched_head": fetched_head,
        }),
        &cancellation,
    )
}

pub async fn push(
    relay: &Arc<TrustedRelay>,
    _params: ProjectGitParams,
    cancellation: CancellationToken,
    mut receipt: PrivilegedGitOperationReceipt,
) -> GitOperationExecution {
    let (checkout, request) =
        match resolve_checkout(relay, ProjectGitOperation::Push, &cancellation).await {
            Ok(resolved) => resolved,
            Err(error) => return preparation_failure(receipt, error, &cancellation),
        };
    let branch_ref = format!("refs/heads/{}", checkout.branch);
    if let Err(error) = bind_receipt(
        &mut receipt,
        &request,
        &checkout,
        &branch_ref,
        None,
        Some(checkout.head_sha.clone()),
    ) {
        return preparation_failure(receipt, error, &cancellation);
    }
    let prepared = async {
        ensure_safe_local_config(relay, &checkout, &cancellation).await?;
        ensure_commit_range(relay, &checkout, &request, &cancellation).await?;
        push_commit(relay, &checkout, &cancellation).await
    }
    .await;
    let mutation = match prepared {
        Ok(mutation) => mutation,
        Err(error) => return preparation_failure(receipt, error, &cancellation),
    };
    mutation_execution(
        receipt,
        mutation,
        serde_json::json!({
            "accepted": true,
            "commit": checkout.head_sha,
            "branch": checkout.branch,
            "forced": false,
        }),
        &cancellation,
    )
}

async fn resolve_checkout(
    relay: &TrustedRelay,
    operation: ProjectGitOperation,
    cancellation: &CancellationToken,
) -> Result<(TrustedGitCheckout, buzz_core::job::JobRequest), String> {
    let operation_id = relay.job_operation_id.as_deref().ok_or_else(|| {
        "Project Git tools require a receiver-verified one-shot job session".to_owned()
    })?;
    let request_event_id = relay.job_request_event_id.as_deref().ok_or_else(|| {
        "Project Git tools require a receiver-verified one-shot job session".to_owned()
    })?;
    // Re-read the immutable signed request named by the session. This narrows
    // a reusable operator grant to the exact repository paths accepted for
    // this job, rather than trusting model-supplied tool parameters or the
    // grant's potentially broader path prefixes.
    let events = relay
        .query_job_events(Some(request_event_id), 1, cancellation)
        .await?;
    let request_event = events
        .iter()
        .find(|event| event.id.to_hex() == request_event_id)
        .ok_or_else(|| "the bound Project job request is unavailable".to_owned())?;
    let job = buzz_core::job::JobEvent::parse(request_event)
        .map_err(|_| "the bound Project job request is invalid".to_owned())?;
    let buzz_core::job::JobEvent::Request(request) = job else {
        return Err("the bound Project event is not a request".into());
    };
    if request.common.operation_id != operation_id
        || request.common.recipient_pubkey != relay.signer_pubkey()
        || !relay.grants.allows_event(
            &buzz_core::job::JobEvent::Request(request.clone()),
            &relay.signer_pubkey(),
        )
    {
        return Err("the bound Project job request is outside the local grant".into());
    }
    let checkout = relay
        .grants
        .trusted_git_checkout(
            relay.session_channel_id.as_deref(),
            relay.session_working_directory.as_deref(),
            &request,
            operation,
        )
        .await?;
    Ok((checkout, request))
}

fn validate_commit_message(message: &str) -> Result<(), String> {
    if message.trim().is_empty()
        || message.len() > MAX_COMMIT_MESSAGE_BYTES
        || message.as_bytes().contains(&0)
    {
        return Err(format!(
            "commit message must contain 1-{MAX_COMMIT_MESSAGE_BYTES} bytes and no NUL"
        ));
    }
    if message.lines().any(|line| {
        line.trim_start()
            .split_once(':')
            .is_some_and(|(key, _)| key.trim().to_ascii_lowercase().starts_with("buzz-"))
    }) {
        return Err("commit message contains a reserved Buzz-* trailer key".into());
    }
    Ok(())
}

fn validate_object_id(value: &str) -> Result<(), String> {
    if matches!(value.len(), 40 | 64)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err("trusted Git returned an invalid object ID".into())
    }
}

fn dco_trailer(relay: &TrustedRelay) -> String {
    let npub = relay
        .keys
        .public_key()
        .to_bech32()
        .unwrap_or_else(|_| relay.signer_pubkey());
    format!(
        "Signed-off-by: {npub} <{}@{}>",
        relay.signer_pubkey(),
        relay.relay_host
    )
}

fn job_binding_trailers(
    relay: &TrustedRelay,
    request: &buzz_core::job::JobRequest,
) -> Result<Vec<String>, String> {
    let request_event_id = relay
        .job_request_event_id
        .as_deref()
        .ok_or_else(|| "Project Git requires an exact request binding".to_owned())?;
    let digest = buzz_core::job::semantic_request_digest(request)
        .map_err(|_| "Project Git request digest is invalid".to_owned())?;
    Ok(vec![
        format!("Buzz-Job-Request: {request_event_id}"),
        format!("Buzz-Job-Operation: {}", request.common.operation_id),
        format!("Buzz-Job-Scope: {digest}"),
        format!("Buzz-Project: {}", request.common.project.address),
        format!("Buzz-Worker: {}", relay.signer_pubkey()),
        format!("Buzz-Relay: {}", relay.relay_host),
    ])
}

fn signed_commit_message(
    relay: &TrustedRelay,
    request: &buzz_core::job::JobRequest,
    message: &str,
) -> Result<String, String> {
    let mut trailers = vec![dco_trailer(relay)];
    trailers.extend(job_binding_trailers(relay, request)?);
    Ok(format!("{}\n\n{}", message.trim_end(), trailers.join("\n")))
}

fn sign_commit_payload(relay: &TrustedRelay, payload: &[u8]) -> Result<Vec<u8>, String> {
    let private_key = zeroize::Zeroizing::new(relay.keys.secret_key().to_secret_hex());
    let signed = git_sign_nostr::sign_payload(
        payload,
        private_key.as_str(),
        relay.auth_tag_json.as_deref(),
    );
    let signed = signed.map_err(|_| "failed to create in-process NIP-GS signature".to_owned())?;
    if signed.signer_public_key != relay.signer_pubkey() {
        return Err("in-process NIP-GS signer did not match the managed agent".into());
    }
    insert_commit_signature(payload, &signed.armored_signature)
}

fn insert_commit_signature(payload: &[u8], armor: &str) -> Result<Vec<u8>, String> {
    let boundary = payload
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| "unsigned Git commit object was malformed".to_owned())?;
    if armor.is_empty() || armor.contains(['\r', '\0']) {
        return Err("NIP-GS signature armor was malformed".into());
    }
    let lines = armor.trim_end_matches('\n').split('\n').collect::<Vec<_>>();
    if lines.is_empty() || lines.iter().any(|line| line.is_empty()) {
        return Err("NIP-GS signature armor was malformed".into());
    }
    let mut signed = Vec::with_capacity(payload.len() + armor.len() + lines.len() + 8);
    signed.extend_from_slice(&payload[..boundary + 1]);
    signed.extend_from_slice(b"gpgsig ");
    signed.extend_from_slice(lines[0].as_bytes());
    signed.push(b'\n');
    for line in &lines[1..] {
        signed.push(b' ');
        signed.extend_from_slice(line.as_bytes());
        signed.push(b'\n');
    }
    signed.push(b'\n');
    signed.extend_from_slice(&payload[boundary + 2..]);
    Ok(signed)
}

fn split_commit_signature(payload: &[u8]) -> Result<(Vec<u8>, String), String> {
    let boundary = payload
        .windows(2)
        .position(|window| window == b"\n\n")
        .ok_or_else(|| "signed Git commit object was malformed".to_owned())?;
    let headers = &payload[..boundary + 1];
    let mut unsigned_headers = Vec::with_capacity(headers.len());
    let mut armor_lines = Vec::new();
    let mut in_signature = false;
    for line in headers.split_inclusive(|byte| *byte == b'\n') {
        let content = line.strip_suffix(b"\n").unwrap_or(line);
        if let Some(first) = content.strip_prefix(b"gpgsig ") {
            if !armor_lines.is_empty() {
                return Err("Git commit contains multiple signatures".into());
            }
            let first = std::str::from_utf8(first)
                .map_err(|_| "Git commit signature was not UTF-8".to_owned())?;
            armor_lines.push(first.to_owned());
            in_signature = true;
        } else if in_signature && content.starts_with(b" ") {
            let continuation = std::str::from_utf8(&content[1..])
                .map_err(|_| "Git commit signature was not UTF-8".to_owned())?;
            armor_lines.push(continuation.to_owned());
        } else {
            in_signature = false;
            unsigned_headers.extend_from_slice(line);
        }
    }
    if armor_lines.is_empty() {
        return Err("Git commit does not contain a NIP-GS signature".into());
    }
    let mut unsigned = unsigned_headers;
    unsigned.push(b'\n');
    unsigned.extend_from_slice(&payload[boundary + 2..]);
    Ok((unsigned, format!("{}\n", armor_lines.join("\n"))))
}

fn verify_commit_signature(relay: &TrustedRelay, payload: &[u8]) -> Result<(), String> {
    let (unsigned, armor) = split_commit_signature(payload)?;
    let verified = git_sign_nostr::verify_payload(&unsigned, &armor)
        .map_err(|_| "Project Git range contains an invalid NIP-GS signature".to_owned())?;
    if verified.signer_public_key != relay.signer_pubkey() {
        return Err("Project Git range contains a commit not signed by this managed agent".into());
    }
    match (&relay.auth_tag_json, verified.owner_authorization_status) {
        (Some(_), git_sign_nostr::OwnerAuthorizationStatus::Valid)
            if verified.owner_public_key.as_deref() == Some(relay.owner_pubkey.as_str()) => {}
        (None, git_sign_nostr::OwnerAuthorizationStatus::Absent) => {}
        _ => {
            return Err(
                "Project Git range contains a commit without the expected owner authorization"
                    .into(),
            )
        }
    }
    Ok(())
}

fn parse_nul_paths(bytes: &[u8]) -> Result<Vec<String>, String> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| {
            std::str::from_utf8(path)
                .map(str::to_owned)
                .map_err(|_| "Git path output must be UTF-8".to_owned())
        })
        .collect()
}

fn ensure_paths_allowed(
    paths: &[String],
    prefixes: &[String],
    repository_wide: bool,
) -> Result<(), String> {
    if paths.iter().all(|path| {
        if repository_wide {
            path_allowed(path, std::slice::from_ref(path))
        } else {
            path_allowed(path, prefixes)
        }
    }) {
        Ok(())
    } else {
        Err("staged or committed changes extend outside the local Project path grant".into())
    }
}

async fn ensure_commit_range(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    request: &buzz_core::job::JobRequest,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let range = format!("{}..{}", checkout.base_sha, checkout.head_sha);
    let commits = git_output(
        relay,
        checkout,
        &["rev-list", "--reverse", &range],
        cancellation,
    )
    .await?;
    let commits = text(&commits.stdout)?
        .lines()
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if commits.len() > MAX_PUSH_COMMITS {
        return Err(format!(
            "trusted Git range exceeds {MAX_PUSH_COMMITS} commits"
        ));
    }
    let mut required_trailers = vec![dco_trailer(relay)];
    required_trailers.extend(job_binding_trailers(relay, request)?);
    let mut expected_parent = checkout.base_sha.clone();
    for commit in commits {
        let parents = git_output(
            relay,
            checkout,
            &["rev-list", "--parents", "-n", "1", &commit],
            cancellation,
        )
        .await?;
        let parents = text(&parents.stdout)?
            .split_whitespace()
            .collect::<Vec<_>>();
        if parents.len() != 2 || parents[0] != commit || parents[1] != expected_parent {
            return Err("Project Git range must be one linear job-bound commit chain".into());
        }
        let changed = git_output(
            relay,
            checkout,
            &[
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-z",
                "-r",
                &expected_parent,
                &commit,
            ],
            cancellation,
        )
        .await?;
        ensure_paths_allowed(
            &parse_nul_paths(&changed.stdout)?,
            &checkout.path_prefixes,
            checkout.repository_wide,
        )?;
        let raw_commit = git_output(
            relay,
            checkout,
            &["cat-file", "commit", &commit],
            cancellation,
        )
        .await?;
        verify_commit_signature(relay, &raw_commit.stdout)?;
        let body = git_output(
            relay,
            checkout,
            &["show", "-s", "--format=%B", &commit],
            cancellation,
        )
        .await?;
        let body = text(&body.stdout)?;
        for trailer in &required_trailers {
            if body.lines().filter(|line| *line == trailer).count() != 1 {
                return Err(
                    "Project Git range contains a commit without exact job-bound trailers".into(),
                );
            }
        }
        expected_parent = commit;
    }
    Ok(())
}

async fn ensure_safe_local_config(
    relay: &TrustedRelay,
    checkout: &TrustedGitCheckout,
    cancellation: &CancellationToken,
) -> Result<(), String> {
    let output = git_output(
        relay,
        checkout,
        &[
            "config",
            "--includes",
            "--null",
            "--list",
            "--show-origin",
            "--show-scope",
        ],
        cancellation,
    )
    .await?;
    let fields = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|field| !field.is_empty())
        .collect::<Vec<_>>();
    if fields.len() % 3 != 0 {
        return Err("local checkout config metadata was malformed".into());
    }
    for entry in fields.chunks_exact(3) {
        if entry[0] != b"local" && entry[0] != b"worktree" {
            continue;
        }
        let raw = std::str::from_utf8(entry[2])
            .map_err(|_| "local Git config must be UTF-8".to_owned())?;
        let key = raw
            .split_once('\n')
            .map(|(key, _)| key)
            .unwrap_or(raw)
            .to_ascii_lowercase();
        if disallowed_local_config_key(&key) {
            return Err(format!(
                "local checkout configures disallowed setting {key}"
            ));
        }
    }
    Ok(())
}

fn disallowed_local_config_key(key: &str) -> bool {
    let allowed_origin = matches!(key, "remote.origin.url" | "remote.origin.fetch");
    key == "http"
        || key.starts_with("http.")
        || key == "credential"
        || key.starts_with("credential.")
        || key == "url"
        || key.starts_with("url.")
        || key == "include"
        || key.starts_with("include.")
        || key == "includeif"
        || key.starts_with("includeif.")
        || key == "filter"
        || key.starts_with("filter.")
        || key == "submodule"
        || key.starts_with("submodule.")
        || key == "core.gitproxy"
        || key == "core.hookspath"
        || key == "core.attributesfile"
        || key == "core.fsmonitor"
        || key == "core.sshcommand"
        || key == "user.signingkey"
        || key == "commit.gpgsign"
        || key == "tag.gpgsign"
        || key == "gpg"
        || key.starts_with("gpg.")
        || (key.starts_with("remote.") && !allowed_origin)
}

fn bind_receipt(
    receipt: &mut PrivilegedGitOperationReceipt,
    request: &buzz_core::job::JobRequest,
    checkout: &TrustedGitCheckout,
    branch_ref: &str,
    previous_object: Option<String>,
    intended_object: Option<String>,
) -> Result<(), String> {
    receipt.scope_digest = Some(
        buzz_core::job::semantic_request_digest(request)
            .map_err(|_| "Project Git request digest is invalid".to_owned())?,
    );
    receipt.repository = Some(checkout.repository.clone());
    receipt.branch_ref = Some(branch_ref.to_owned());
    receipt.previous_object = previous_object;
    receipt.intended_object = intended_object;
    Ok(())
}

fn preparation_failure(
    mut receipt: PrivilegedGitOperationReceipt,
    error: String,
    cancellation: &CancellationToken,
) -> GitOperationExecution {
    receipt.disposition = PrivilegedGitDisposition::NotApplied;
    let result = CallToolResult::error(vec![rmcp::model::Content::text(
        serde_json::json!({
            "accepted": false,
            "disposition": "not_applied",
            "operation": receipt.operation.as_str(),
            "error": error,
        })
        .to_string(),
    )]);
    GitOperationExecution {
        result,
        outcome: if cancellation.is_cancelled() {
            PrivilegedOperationOutcome::Cancelled
        } else {
            PrivilegedOperationOutcome::Failed
        },
        receipt,
    }
}

fn mutation_execution(
    mut receipt: PrivilegedGitOperationReceipt,
    mutation: RefMutationOutcome,
    mut success: serde_json::Value,
    cancellation: &CancellationToken,
) -> GitOperationExecution {
    receipt.previous_object = mutation.previous_object;
    receipt.intended_object = Some(mutation.intended_object);
    receipt.observed_object = mutation.observed_object;
    receipt.disposition = mutation.disposition;
    let reconciled_after_error = mutation.command_error.is_some();
    let command_was_cancelled = mutation
        .command_error
        .as_deref()
        .is_some_and(process::is_cancellation_error);

    match receipt.disposition {
        PrivilegedGitDisposition::Applied => {
            if let Some(object) = success.as_object_mut() {
                object.insert("disposition".into(), serde_json::json!("applied"));
                object.insert(
                    "reconciled_after_uncertain_result".into(),
                    serde_json::json!(reconciled_after_error),
                );
                object.insert("target_ref".into(), serde_json::json!(receipt.branch_ref));
                object.insert(
                    "observed_object".into(),
                    serde_json::json!(receipt.observed_object),
                );
            }
            GitOperationExecution {
                result: CallToolResult::success(vec![rmcp::model::Content::text(
                    success.to_string(),
                )]),
                outcome: PrivilegedOperationOutcome::Completed,
                receipt,
            }
        }
        PrivilegedGitDisposition::NotApplied => {
            let error = mutation
                .command_error
                .unwrap_or_else(|| "trusted Git ref update was not applied".to_owned());
            GitOperationExecution {
                result: mutation_error_result(&receipt, &error, false),
                outcome: if command_was_cancelled || cancellation.is_cancelled() {
                    PrivilegedOperationOutcome::Cancelled
                } else {
                    PrivilegedOperationOutcome::Failed
                },
                receipt,
            }
        }
        PrivilegedGitDisposition::Ambiguous => {
            let error = mutation.command_error.unwrap_or_else(|| {
                "trusted Git outcome is ambiguous after exact ref reconciliation".to_owned()
            });
            GitOperationExecution {
                result: mutation_error_result(&receipt, &error, true),
                // Cancellation never overrides an unknown side-effect state.
                outcome: PrivilegedOperationOutcome::Indeterminate,
                receipt,
            }
        }
    }
}

fn mutation_error_result(
    receipt: &PrivilegedGitOperationReceipt,
    error: &str,
    retryable_false: bool,
) -> CallToolResult {
    let mut value = serde_json::json!({
        "accepted": false,
        "disposition": match receipt.disposition {
            PrivilegedGitDisposition::Applied => "applied",
            PrivilegedGitDisposition::NotApplied => "not_applied",
            PrivilegedGitDisposition::Ambiguous => "ambiguous",
        },
        "operation": receipt.operation.as_str(),
        "repository": receipt.repository,
        "target_ref": receipt.branch_ref,
        "previous_object": receipt.previous_object,
        "intended_object": receipt.intended_object,
        "observed_object": receipt.observed_object,
        "error": error,
    });
    if retryable_false {
        value
            .as_object_mut()
            .expect("JSON object")
            .insert("retryable".into(), serde_json::json!(false));
    }
    CallToolResult::error(vec![rmcp::model::Content::text(value.to_string())])
}

#[cfg(test)]
mod tests {
    use super::{
        disallowed_local_config_key, insert_commit_signature, sign_commit_payload,
        split_commit_signature, validate_commit_message, verify_commit_signature,
    };
    use crate::trusted::{GrantSet, TrustedConfig, TrustedRelay};

    fn test_commit_payload() -> &'static [u8] {
        b"tree 1111111111111111111111111111111111111111\n\
parent 2222222222222222222222222222222222222222\n\
author Buzz <buzz@example.invalid> 1 +0000\n\
committer Buzz <buzz@example.invalid> 1 +0000\n\nmessage\n"
    }

    fn relay_with_owner_authorization(conditions: &str) -> TrustedRelay {
        let keys = nostr::Keys::generate();
        let owner_keys = nostr::Keys::generate();
        let auth_tag_json =
            buzz_sdk::nip_oa::compute_auth_tag(&owner_keys, &keys.public_key(), conditions)
                .expect("owner authorization");
        let auth_tag =
            buzz_sdk::nip_oa::parse_auth_tag(&auth_tag_json).expect("parsed authorization");
        TrustedRelay::new(TrustedConfig {
            relay_url: "http://127.0.0.1:1".to_owned(),
            keys,
            auth_tag: Some(auth_tag),
            auth_tag_json: Some(auth_tag_json),
            owner_pubkey: owner_keys.public_key().to_hex(),
            owner_github_login: None,
            grants: GrantSet::default(),
            a2a_channel_id: None,
            session_channel_id: None,
            session_thread_root_id: None,
            job_operation_id: None,
            job_request_event_id: None,
            session_working_directory: None,
            github_credentials: Default::default(),
            allow_insecure_loopback: true,
        })
        .expect("trusted relay")
    }

    #[test]
    fn in_process_signature_header_round_trips_the_exact_payload() {
        let payload = test_commit_payload();
        let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
        let signature =
            git_sign_nostr::sign_payload(payload, private_key, None).expect("in-process signature");
        let signed = insert_commit_signature(payload, &signature.armored_signature)
            .expect("insert signature header");
        let (restored, armor) = split_commit_signature(&signed).expect("split signature header");
        assert_eq!(restored, payload);
        let verified = git_sign_nostr::verify_payload(&restored, &armor).expect("verify signature");
        assert_eq!(verified.signer_public_key, signature.signer_public_key);
    }

    #[test]
    fn trusted_git_accepts_unconditional_owner_authorization() {
        let relay = relay_with_owner_authorization("");
        let signed = sign_commit_payload(&relay, test_commit_payload()).expect("signed commit");

        verify_commit_signature(&relay, &signed)
            .expect("unconditional owner authorization should be accepted");
    }

    #[test]
    fn trusted_git_rejects_kind_constrained_owner_authorization() {
        let relay = relay_with_owner_authorization("kind=9");
        let signed = sign_commit_payload(&relay, test_commit_payload()).expect("signed commit");
        let error = verify_commit_signature(&relay, &signed)
            .expect_err("a Git commit cannot satisfy a NIP-OA kind constraint");

        assert_eq!(
            error,
            "Project Git range contains a commit without the expected owner authorization"
        );
    }

    #[test]
    fn signature_parser_rejects_missing_or_duplicate_headers() {
        let unsigned = b"tree 1111111111111111111111111111111111111111\n\nmessage\n";
        assert!(split_commit_signature(unsigned).is_err());
        let duplicate = b"tree 1111111111111111111111111111111111111111\n\
gpgsig first\n\
gpgsig second\n\nmessage\n";
        assert!(split_commit_signature(duplicate).is_err());
    }

    #[test]
    fn commit_message_rejects_reserved_buzz_trailer_keys() {
        for message in [
            "change\n\nBuzz-Job-Request: forged",
            "change\n\n  buzz-worker : forged",
            "change\nBUZZ-Future-Key: forged",
        ] {
            assert_eq!(
                validate_commit_message(message),
                Err("commit message contains a reserved Buzz-* trailer key".into())
            );
        }
        validate_commit_message("Make the Buzz-worthy behavior clearer: safely")
            .expect("ordinary body text is not a reserved trailer");
    }

    #[test]
    fn conditional_git_includes_are_rejected() {
        assert!(disallowed_local_config_key("includeif"));
        assert!(disallowed_local_config_key(
            "includeif.gitdir:/tmp/project.path"
        ));
    }
}
