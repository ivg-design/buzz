use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use nostr::Event;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::lifecycle::LifecycleStore;

const LEDGER_VERSION: u32 = 2;
const MAX_LEDGER_RECORDS: usize = 4096;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("job ledger I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("job ledger record is invalid: {0}")]
    Invalid(String),
    #[error("job ledger serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("job ledger blocking task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredClaim {
    version: u32,
    pub community: String,
    pub requester: String,
    pub idempotency_key: String,
    pub digest: String,
    pub request_event_id: String,
    pub request_event: Event,
    pub processed: Event,
    pub accepted: Event,
}

impl StoredClaim {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        community: String,
        requester: String,
        idempotency_key: String,
        digest: String,
        request_event_id: String,
        request_event: Event,
        processed: Event,
        accepted: Event,
    ) -> Self {
        Self {
            version: LEDGER_VERSION,
            community,
            requester,
            idempotency_key,
            digest,
            request_event_id,
            request_event,
            processed,
            accepted,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClaimDecision {
    New(StoredClaim),
    Replay(StoredClaim),
    Conflict { existing_digest: String },
}

#[derive(Debug, Clone)]
pub struct JobLedger {
    root: PathBuf,
}

#[derive(Clone, Copy)]
pub enum ReceiptKind {
    Processed,
    Accepted,
}

impl JobLedger {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn lifecycle_store(&self, claim: &StoredClaim) -> LifecycleStore {
        LifecycleStore::new(
            &self.root,
            &ledger_key(&claim.community, &claim.requester, &claim.idempotency_key),
        )
    }

    pub async fn claim(&self, candidate: StoredClaim) -> Result<ClaimDecision, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.claim_blocking(candidate)).await?
    }

    pub async fn mark_prompt_started(&self, claim: &StoredClaim) -> Result<bool, LedgerError> {
        let ledger = self.clone();
        let claim = claim.clone();
        tokio::task::spawn_blocking(move || ledger.mark_prompt_started_blocking(&claim)).await?
    }

    pub async fn prompt_started(&self, claim: &StoredClaim) -> Result<bool, LedgerError> {
        let ledger = self.clone();
        let claim = claim.clone();
        tokio::task::spawn_blocking(move || ledger.prompt_started_blocking(&claim)).await?
    }

    pub async fn mark_receipt_acked(
        &self,
        claim: &StoredClaim,
        kind: ReceiptKind,
    ) -> Result<(), LedgerError> {
        let ledger = self.clone();
        let claim = claim.clone();
        tokio::task::spawn_blocking(move || ledger.mark_receipt_acked_blocking(&claim, kind))
            .await?
    }

    pub async fn receipt_acked(
        &self,
        claim: &StoredClaim,
        kind: ReceiptKind,
    ) -> Result<bool, LedgerError> {
        let ledger = self.clone();
        let claim = claim.clone();
        tokio::task::spawn_blocking(move || ledger.receipt_acked_blocking(&claim, kind)).await?
    }

    pub async fn pending_claims(&self) -> Result<Vec<StoredClaim>, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.pending_claims_blocking()).await?
    }

    pub async fn claims(&self) -> Result<Vec<StoredClaim>, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.claims_blocking()).await?
    }

    pub async fn claim_for_request(
        &self,
        request_event_id: &str,
    ) -> Result<Option<StoredClaim>, LedgerError> {
        let request_event_id = request_event_id.to_owned();
        Ok(self
            .claims()
            .await?
            .into_iter()
            .find(|claim| claim.request_event_id == request_event_id))
    }

    fn claim_blocking(&self, candidate: StoredClaim) -> Result<ClaimDecision, LedgerError> {
        self.prepare_root()?;
        let key = ledger_key(
            &candidate.community,
            &candidate.requester,
            &candidate.idempotency_key,
        );
        let path = self.root.join(format!("{key}.json"));
        if path.exists() {
            return compare_existing(&path, &candidate);
        }
        if count_records(&self.root)? >= MAX_LEDGER_RECORDS {
            return Err(LedgerError::Invalid(format!(
                "ledger contains the maximum {MAX_LEDGER_RECORDS} records"
            )));
        }

        let temporary = self.root.join(format!(".{key}.{}.tmp", Uuid::new_v4()));
        write_private_file(&temporary, &serde_json::to_vec(&candidate)?)?;
        match std::fs::hard_link(&temporary, &path) {
            Ok(()) => {
                sync_directory(&self.root)?;
                std::fs::remove_file(&temporary)?;
                sync_directory(&self.root)?;
                Ok(ClaimDecision::New(candidate))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(&temporary)?;
                compare_existing(&path, &candidate)
            }
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                Err(error.into())
            }
        }
    }

    fn mark_prompt_started_blocking(&self, claim: &StoredClaim) -> Result<bool, LedgerError> {
        self.prepare_root()?;
        let key = ledger_key(&claim.community, &claim.requester, &claim.idempotency_key);
        let marker = self.root.join(format!("{key}.prompt-started"));
        match create_private_new(&marker) {
            Ok(mut file) => {
                file.write_all(claim.request_event_id.as_bytes())?;
                file.sync_all()?;
                sync_directory(&self.root)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn prompt_started_blocking(&self, claim: &StoredClaim) -> Result<bool, LedgerError> {
        let key = ledger_key(&claim.community, &claim.requester, &claim.idempotency_key);
        Ok(self.root.join(format!("{key}.prompt-started")).exists())
    }

    fn mark_receipt_acked_blocking(
        &self,
        claim: &StoredClaim,
        kind: ReceiptKind,
    ) -> Result<(), LedgerError> {
        self.prepare_root()?;
        let path = self.receipt_ack_path(claim, kind);
        let event_id = receipt_event(claim, kind).id.to_hex();
        match create_private_new(&path) {
            Ok(mut file) => {
                file.write_all(event_id.as_bytes())?;
                file.sync_all()?;
                sync_directory(&self.root)?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if std::fs::read_to_string(path)? == event_id {
                    Ok(())
                } else {
                    Err(LedgerError::Invalid(
                        "receipt acknowledgement marker has the wrong event id".into(),
                    ))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn receipt_acked_blocking(
        &self,
        claim: &StoredClaim,
        kind: ReceiptKind,
    ) -> Result<bool, LedgerError> {
        let path = self.receipt_ack_path(claim, kind);
        match std::fs::read_to_string(path) {
            Ok(stored) if stored == receipt_event(claim, kind).id.to_hex() => Ok(true),
            Ok(_) => Err(LedgerError::Invalid(
                "receipt acknowledgement marker has the wrong event id".into(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn receipt_ack_path(&self, claim: &StoredClaim, kind: ReceiptKind) -> PathBuf {
        let key = ledger_key(&claim.community, &claim.requester, &claim.idempotency_key);
        let suffix = match kind {
            ReceiptKind::Processed => "processed-acked",
            ReceiptKind::Accepted => "accepted-acked",
        };
        self.root.join(format!("{key}.{suffix}"))
    }

    fn pending_claims_blocking(&self) -> Result<Vec<StoredClaim>, LedgerError> {
        let claims = self.claims_blocking()?;
        let mut pending = Vec::new();
        for claim in claims {
            if !self.prompt_started_blocking(&claim)? {
                pending.push(claim);
            }
        }
        Ok(pending)
    }

    fn claims_blocking(&self) -> Result<Vec<StoredClaim>, LedgerError> {
        self.prepare_root()?;
        let mut claims = Vec::new();
        for entry in std::fs::read_dir(&self.root)?.take(MAX_LEDGER_RECORDS + 1) {
            let path = entry?.path();
            if !is_claim_record_path(&path) {
                continue;
            }
            let claim: StoredClaim = serde_json::from_slice(&std::fs::read(&path)?)?;
            if claim.version != LEDGER_VERSION {
                return Err(LedgerError::Invalid(
                    "unsupported ledger record version".into(),
                ));
            }
            claims.push(claim);
        }
        Ok(claims)
    }

    fn prepare_root(&self) -> Result<(), LedgerError> {
        std::fs::create_dir_all(&self.root)?;
        Ok(())
    }
}

fn receipt_event(claim: &StoredClaim, kind: ReceiptKind) -> &Event {
    match kind {
        ReceiptKind::Processed => &claim.processed,
        ReceiptKind::Accepted => &claim.accepted,
    }
}

fn compare_existing(path: &Path, candidate: &StoredClaim) -> Result<ClaimDecision, LedgerError> {
    let raw = std::fs::read(path)?;
    let existing: StoredClaim = serde_json::from_slice(&raw)?;
    if existing.version != LEDGER_VERSION
        || existing.community != candidate.community
        || existing.requester != candidate.requester
        || existing.idempotency_key != candidate.idempotency_key
    {
        return Err(LedgerError::Invalid(
            "record key fields do not match its file name".into(),
        ));
    }
    if existing.digest == candidate.digest {
        Ok(ClaimDecision::Replay(existing))
    } else {
        Ok(ClaimDecision::Conflict {
            existing_digest: existing.digest,
        })
    }
}

fn ledger_key(community: &str, requester: &str, idempotency_key: &str) -> String {
    let mut digest = Sha256::new();
    for part in [community, requester, idempotency_key] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    hex::encode(digest.finalize())
}

fn count_records(root: &Path) -> Result<usize, std::io::Error> {
    Ok(std::fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter(|entry| is_claim_record_path(&entry.path()))
        .take(MAX_LEDGER_RECORDS + 1)
        .count())
}

fn is_claim_record_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(key) = name.strip_suffix(".json") else {
        return false;
    };
    key.len() == 64
        && key
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
    }
    // Windows does not provide a portable directory fsync through std. File
    // contents are still flushed; production crash-durability is guaranteed on
    // the Unix targets supported by Buzz desktop and relay deployments.
    Ok(())
}

fn write_private_file(path: &Path, content: &[u8]) -> Result<(), std::io::Error> {
    let mut file = create_private_new(path)?;
    file.write_all(content)?;
    file.sync_all()?;
    Ok(())
}

fn create_private_new(path: &Path) -> Result<File, std::io::Error> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::{EventBuilder, Keys, Kind};

    fn claim(root: &Path, digest: &str) -> (JobLedger, StoredClaim) {
        let keys = Keys::generate();
        let event = EventBuilder::new(Kind::TextNote, "receipt")
            .sign_with_keys(&keys)
            .expect("sign");
        (
            JobLedger::new(root.to_path_buf()),
            StoredClaim::new(
                "wss://example.test".into(),
                keys.public_key().to_hex(),
                "idem".into(),
                digest.into(),
                event.id.to_hex(),
                event.clone(),
                event.clone(),
                event,
            ),
        )
    }

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("buzz-acp-job-ledger-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn same_key_replays_and_changed_digest_conflicts() {
        let root = temp_root();
        let (ledger, first) = claim(&root, &"a".repeat(64));
        assert!(matches!(
            ledger.claim(first.clone()).await.expect("claim"),
            ClaimDecision::New(_)
        ));
        assert!(matches!(
            ledger.claim(first.clone()).await.expect("replay"),
            ClaimDecision::Replay(_)
        ));
        let (_, mut changed) = claim(&root, &"b".repeat(64));
        changed.requester = first.requester;
        assert!(matches!(
            ledger.claim(changed).await.expect("conflict"),
            ClaimDecision::Conflict { .. }
        ));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn reconnect_preserves_claim_and_prompt_marker() {
        let root = temp_root();
        let (first_process, stored) = claim(&root, &"a".repeat(64));
        first_process.claim(stored.clone()).await.expect("claim");
        assert!(first_process
            .mark_prompt_started(&stored)
            .await
            .expect("mark"));
        let second_process = JobLedger::new(root.clone());
        assert!(matches!(
            second_process.claim(stored.clone()).await.expect("replay"),
            ClaimDecision::Replay(_)
        ));
        assert!(!second_process
            .mark_prompt_started(&stored)
            .await
            .expect("already marked"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn concurrent_claim_and_prompt_start_have_one_winner() {
        let root = temp_root();
        let (ledger, stored) = claim(&root, &"a".repeat(64));
        let (left, right) =
            tokio::join!(ledger.claim(stored.clone()), ledger.claim(stored.clone()));
        let decisions = [left.expect("left"), right.expect("right")];
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| matches!(decision, ClaimDecision::New(_)))
                .count(),
            1
        );
        let (left, right) = tokio::join!(
            ledger.mark_prompt_started(&stored),
            ledger.mark_prompt_started(&stored)
        );
        assert_eq!(
            usize::from(left.expect("left")) + usize::from(right.expect("right")),
            1
        );
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn lifecycle_files_are_never_scanned_or_counted_as_claims() {
        let root = temp_root();
        let (ledger, stored) = claim(&root, &"a".repeat(64));
        ledger.claim(stored.clone()).await.expect("claim");
        let lifecycle = ledger.lifecycle_store(&stored);
        lifecycle
            .initialize(stored.accepted.id.to_hex())
            .await
            .expect("initialize lifecycle");
        let progress = EventBuilder::new(Kind::TextNote, "progress")
            .sign_with_keys(&Keys::generate())
            .expect("sign progress");
        lifecycle
            .stage(progress.clone(), false, stored.accepted.id.to_hex())
            .await
            .expect("stage outbox");

        let reopened = JobLedger::new(root.clone());
        let claims = reopened.claims().await.expect("scan claims");
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].request_event_id, stored.request_event_id);
        assert_eq!(count_records(&root).expect("count claims"), 1);

        lifecycle
            .confirm(progress.id.to_hex())
            .await
            .expect("confirm terminal-independent outbox");
        assert_eq!(
            reopened.claims().await.expect("scan after confirm").len(),
            1
        );
        assert_eq!(count_records(&root).expect("count after confirm"), 1);
        std::fs::remove_dir_all(root).ok();
    }
}
