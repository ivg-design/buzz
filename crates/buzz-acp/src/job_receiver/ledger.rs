use std::fs::File;
#[cfg(not(windows))]
use std::fs::OpenOptions;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredDecline {
    version: u32,
    pub community: String,
    pub requester: String,
    pub idempotency_key: String,
    pub digest: String,
    pub request_event_id: String,
    pub request_event: Event,
    pub declined: Event,
}

impl StoredDecline {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        community: String,
        requester: String,
        idempotency_key: String,
        digest: String,
        request_event_id: String,
        request_event: Event,
        declined: Event,
    ) -> Self {
        Self {
            version: LEDGER_VERSION,
            community,
            requester,
            idempotency_key,
            digest,
            request_event_id,
            request_event,
            declined,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StoredRecord {
    Claim(StoredClaim),
    Decline(StoredDecline),
}

#[derive(Debug, Clone)]
pub enum ClaimDecision {
    New(StoredClaim),
    Replay(StoredClaim),
    Declined(StoredDecline),
    Conflict { existing_digest: String },
}

#[derive(Debug, Clone)]
pub enum DeclineDecision {
    New(StoredDecline),
    Replay(StoredDecline),
    Claimed,
    Conflict { existing_digest: String },
}

#[derive(Debug, Clone)]
pub enum DeclineLookup {
    Absent,
    Claimed,
    Replay(StoredDecline),
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

    pub async fn decline(&self, candidate: StoredDecline) -> Result<DeclineDecision, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.decline_blocking(candidate)).await?
    }

    pub async fn lookup_decline(
        &self,
        community: &str,
        requester: &str,
        idempotency_key: &str,
        digest: &str,
    ) -> Result<DeclineLookup, LedgerError> {
        let ledger = self.clone();
        let community = community.to_owned();
        let requester = requester.to_owned();
        let idempotency_key = idempotency_key.to_owned();
        let digest = digest.to_owned();
        tokio::task::spawn_blocking(move || {
            ledger.lookup_decline_blocking(&community, &requester, &idempotency_key, &digest)
        })
        .await?
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

    pub async fn mark_decline_acked(&self, decline: &StoredDecline) -> Result<(), LedgerError> {
        let ledger = self.clone();
        let decline = decline.clone();
        tokio::task::spawn_blocking(move || ledger.mark_decline_acked_blocking(&decline)).await?
    }

    pub async fn decline_acked(&self, decline: &StoredDecline) -> Result<bool, LedgerError> {
        let ledger = self.clone();
        let decline = decline.clone();
        tokio::task::spawn_blocking(move || ledger.decline_acked_blocking(&decline)).await?
    }

    pub async fn pending_claims(&self) -> Result<Vec<StoredClaim>, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.pending_claims_blocking()).await?
    }

    pub async fn claims(&self) -> Result<Vec<StoredClaim>, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.claims_blocking()).await?
    }

    pub async fn declines(&self) -> Result<Vec<StoredDecline>, LedgerError> {
        let ledger = self.clone();
        tokio::task::spawn_blocking(move || ledger.declines_blocking()).await?
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

    /// Re-read the immutable claim record addressed by the frozen binding.
    ///
    /// Privileged-operation admission uses this instead of trusting the
    /// in-memory dispatch clone retained since request handling.
    pub(super) async fn reload_claim(
        &self,
        frozen: &StoredClaim,
    ) -> Result<StoredClaim, LedgerError> {
        let ledger = self.clone();
        let frozen = frozen.clone();
        tokio::task::spawn_blocking(move || ledger.reload_claim_blocking(&frozen)).await?
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

    fn decline_blocking(&self, candidate: StoredDecline) -> Result<DeclineDecision, LedgerError> {
        self.prepare_root()?;
        let key = ledger_key(
            &candidate.community,
            &candidate.requester,
            &candidate.idempotency_key,
        );
        let path = self.root.join(format!("{key}.json"));
        if path.exists() {
            return compare_existing_decline(&path, &candidate);
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
                Ok(DeclineDecision::New(candidate))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                std::fs::remove_file(&temporary)?;
                compare_existing_decline(&path, &candidate)
            }
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                Err(error.into())
            }
        }
    }

    fn lookup_decline_blocking(
        &self,
        community: &str,
        requester: &str,
        idempotency_key: &str,
        digest: &str,
    ) -> Result<DeclineLookup, LedgerError> {
        self.prepare_root()?;
        let key = ledger_key(community, requester, idempotency_key);
        let path = self.root.join(format!("{key}.json"));
        let record = match read_record(&path) {
            Ok(record) => record,
            Err(LedgerError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(DeclineLookup::Absent)
            }
            Err(error) => return Err(error),
        };
        validate_record_key(&record, community, requester, idempotency_key)?;
        let (existing_digest, decline) = match record {
            StoredRecord::Claim(claim) => (claim.digest, None),
            StoredRecord::Decline(decline) => (decline.digest.clone(), Some(decline)),
        };
        if existing_digest != digest {
            return Ok(DeclineLookup::Conflict { existing_digest });
        }
        Ok(match decline {
            Some(decline) => DeclineLookup::Replay(decline),
            None => DeclineLookup::Claimed,
        })
    }

    fn reload_claim_blocking(&self, frozen: &StoredClaim) -> Result<StoredClaim, LedgerError> {
        let key = ledger_key(
            &frozen.community,
            &frozen.requester,
            &frozen.idempotency_key,
        );
        let path = self.root.join(format!("{key}.json"));
        let stored: StoredClaim = serde_json::from_slice(&read_private_bytes(&path)?)?;
        if !claims_match_exactly(&stored, frozen) {
            return Err(LedgerError::Invalid(
                "durable claim changed after admission".into(),
            ));
        }
        Ok(stored)
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
                if read_private_string(&path)? == event_id {
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
        match read_private_string(&path) {
            Ok(stored) if stored == receipt_event(claim, kind).id.to_hex() => Ok(true),
            Ok(_) => Err(LedgerError::Invalid(
                "receipt acknowledgement marker has the wrong event id".into(),
            )),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn mark_decline_acked_blocking(&self, decline: &StoredDecline) -> Result<(), LedgerError> {
        self.prepare_root()?;
        let path = self.decline_ack_path(decline);
        let event_id = decline.declined.id.to_hex();
        match create_private_new(&path) {
            Ok(mut file) => {
                file.write_all(event_id.as_bytes())?;
                file.sync_all()?;
                sync_directory(&self.root)?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if read_private_string(&path)? == event_id {
                    Ok(())
                } else {
                    Err(LedgerError::Invalid(
                        "decline acknowledgement marker has the wrong event id".into(),
                    ))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    fn decline_acked_blocking(&self, decline: &StoredDecline) -> Result<bool, LedgerError> {
        let path = self.decline_ack_path(decline);
        match read_private_string(&path) {
            Ok(stored) if stored == decline.declined.id.to_hex() => Ok(true),
            Ok(_) => Err(LedgerError::Invalid(
                "decline acknowledgement marker has the wrong event id".into(),
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

    fn decline_ack_path(&self, decline: &StoredDecline) -> PathBuf {
        let key = ledger_key(
            &decline.community,
            &decline.requester,
            &decline.idempotency_key,
        );
        self.root.join(format!("{key}.declined-acked"))
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
            if let StoredRecord::Claim(claim) = read_record(&path)? {
                claims.push(claim);
            }
        }
        Ok(claims)
    }

    fn declines_blocking(&self) -> Result<Vec<StoredDecline>, LedgerError> {
        self.prepare_root()?;
        let mut declines = Vec::new();
        for entry in std::fs::read_dir(&self.root)?.take(MAX_LEDGER_RECORDS + 1) {
            let path = entry?.path();
            if !is_claim_record_path(&path) {
                continue;
            }
            if let StoredRecord::Decline(decline) = read_record(&path)? {
                declines.push(decline);
            }
        }
        Ok(declines)
    }

    fn prepare_root(&self) -> Result<(), LedgerError> {
        std::fs::create_dir_all(&self.root)?;
        #[cfg(windows)]
        super::windows_private::secure_directory(&self.root)?;
        Ok(())
    }
}

fn receipt_event(claim: &StoredClaim, kind: ReceiptKind) -> &Event {
    match kind {
        ReceiptKind::Processed => &claim.processed,
        ReceiptKind::Accepted => &claim.accepted,
    }
}

fn claims_match_exactly(left: &StoredClaim, right: &StoredClaim) -> bool {
    left.version == right.version
        && left.community == right.community
        && left.requester == right.requester
        && left.idempotency_key == right.idempotency_key
        && left.digest == right.digest
        && left.request_event_id == right.request_event_id
        && left.request_event == right.request_event
        && left.processed == right.processed
        && left.accepted == right.accepted
}

fn compare_existing(path: &Path, candidate: &StoredClaim) -> Result<ClaimDecision, LedgerError> {
    let record = read_record(path)?;
    validate_record_key(
        &record,
        &candidate.community,
        &candidate.requester,
        &candidate.idempotency_key,
    )?;
    match record {
        StoredRecord::Claim(existing) if existing.digest == candidate.digest => {
            Ok(ClaimDecision::Replay(existing))
        }
        StoredRecord::Decline(existing) if existing.digest == candidate.digest => {
            Ok(ClaimDecision::Declined(existing))
        }
        StoredRecord::Claim(existing) => Ok(ClaimDecision::Conflict {
            existing_digest: existing.digest,
        }),
        StoredRecord::Decline(existing) => Ok(ClaimDecision::Conflict {
            existing_digest: existing.digest,
        }),
    }
}

fn compare_existing_decline(
    path: &Path,
    candidate: &StoredDecline,
) -> Result<DeclineDecision, LedgerError> {
    let record = read_record(path)?;
    validate_record_key(
        &record,
        &candidate.community,
        &candidate.requester,
        &candidate.idempotency_key,
    )?;
    match record {
        StoredRecord::Decline(existing) if existing.digest == candidate.digest => {
            Ok(DeclineDecision::Replay(existing))
        }
        StoredRecord::Claim(existing) if existing.digest == candidate.digest => {
            Ok(DeclineDecision::Claimed)
        }
        StoredRecord::Claim(existing) => Ok(DeclineDecision::Conflict {
            existing_digest: existing.digest,
        }),
        StoredRecord::Decline(existing) => Ok(DeclineDecision::Conflict {
            existing_digest: existing.digest,
        }),
    }
}

fn read_record(path: &Path) -> Result<StoredRecord, LedgerError> {
    let record: StoredRecord = serde_json::from_slice(&read_private_bytes(path)?)?;
    let version = match &record {
        StoredRecord::Claim(claim) => claim.version,
        StoredRecord::Decline(decline) => decline.version,
    };
    if version != LEDGER_VERSION {
        return Err(LedgerError::Invalid(
            "unsupported ledger record version".into(),
        ));
    }
    Ok(record)
}

fn validate_record_key(
    record: &StoredRecord,
    community: &str,
    requester: &str,
    idempotency_key: &str,
) -> Result<(), LedgerError> {
    let matches = match record {
        StoredRecord::Claim(claim) => {
            claim.community == community
                && claim.requester == requester
                && claim.idempotency_key == idempotency_key
        }
        StoredRecord::Decline(decline) => {
            decline.community == community
                && decline.requester == requester
                && decline.idempotency_key == idempotency_key
        }
    };
    if matches {
        Ok(())
    } else {
        Err(LedgerError::Invalid(
            "record key fields do not match its file name".into(),
        ))
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

fn sync_directory(_path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        File::open(_path)?.sync_all()?;
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

#[cfg(unix)]
fn create_private_new(path: &Path) -> Result<File, std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(windows)]
fn create_private_new(path: &Path) -> Result<File, std::io::Error> {
    super::windows_private::create_private_new(path)
}

#[cfg(all(not(unix), not(windows)))]
fn create_private_new(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

#[cfg(windows)]
fn read_private_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    use std::io::Read as _;
    let mut file = super::windows_private::open_private_read(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

#[cfg(not(windows))]
fn read_private_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
    std::fs::read(path)
}

fn read_private_string(path: &Path) -> Result<String, std::io::Error> {
    String::from_utf8(read_private_bytes(path)?)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
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

    fn decline_from(claim: &StoredClaim) -> StoredDecline {
        StoredDecline::new(
            claim.community.clone(),
            claim.requester.clone(),
            claim.idempotency_key.clone(),
            claim.digest.clone(),
            claim.request_event_id.clone(),
            claim.request_event.clone(),
            claim.accepted.clone(),
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
    async fn decline_is_immutable_replayable_and_excludes_claim() {
        let root = temp_root();
        let (ledger, claim) = claim(&root, &"a".repeat(64));
        let decline = decline_from(&claim);
        assert!(matches!(
            ledger.decline(decline.clone()).await.expect("decline"),
            DeclineDecision::New(_)
        ));
        assert!(matches!(
            ledger
                .lookup_decline(
                    &decline.community,
                    &decline.requester,
                    &decline.idempotency_key,
                    &decline.digest,
                )
                .await
                .expect("lookup"),
            DeclineLookup::Replay(_)
        ));
        assert!(matches!(
            ledger.claim(claim).await.expect("claim excluded"),
            ClaimDecision::Declined(_)
        ));
        assert!(ledger.claims().await.expect("claims").is_empty());
        assert_eq!(ledger.declines().await.expect("declines").len(), 1);
        assert!(!ledger.decline_acked(&decline).await.expect("unacked"));
        ledger
            .mark_decline_acked(&decline)
            .await
            .expect("mark acked");
        assert!(ledger.decline_acked(&decline).await.expect("acked"));
        std::fs::remove_dir_all(root).ok();
    }

    #[tokio::test]
    async fn claim_and_changed_decline_keep_one_atomic_outcome() {
        let root = temp_root();
        let (ledger, claim) = claim(&root, &"a".repeat(64));
        ledger.claim(claim.clone()).await.expect("claim");
        let mut decline = decline_from(&claim);
        decline.digest = "b".repeat(64);
        assert!(matches!(
            ledger.decline(decline).await.expect("conflict"),
            DeclineDecision::Conflict { .. }
        ));
        assert_eq!(ledger.claims().await.expect("claims").len(), 1);
        assert!(ledger.declines().await.expect("declines").is_empty());
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
