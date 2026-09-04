use std::fs::{File, OpenOptions};
use std::path::Path;

use fs2::FileExt;

use super::LedgerError;

/// Process-lifetime singleton lease for one tenant/agent job ledger.
pub struct ReceiverLease {
    file: File,
}

impl ReceiverLease {
    pub fn acquire(root: &Path) -> Result<Self, LedgerError> {
        std::fs::create_dir_all(root)?;
        let path = root.join(".receiver.lock");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        file.try_lock_exclusive().map_err(|error| {
            LedgerError::Invalid(format!(
                "another ACP receiver already owns this tenant/agent ledger: {error}"
            ))
        })?;
        Ok(Self { file })
    }
}

impl Drop for ReceiverLease {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn only_one_receiver_process_can_hold_a_ledger() {
        let root = std::env::temp_dir().join(format!("buzz-receiver-lease-{}", Uuid::new_v4()));
        let first = ReceiverLease::acquire(&root).expect("first lease");
        assert!(ReceiverLease::acquire(&root).is_err());
        drop(first);
        assert!(ReceiverLease::acquire(&root).is_ok());
        std::fs::remove_dir_all(root).ok();
    }
}
