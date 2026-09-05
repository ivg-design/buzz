//! Short-lived enrolled-agent classification cache for unaddressed chat.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone)]
pub(super) struct PeerAuthorCache {
    entries: Arc<Mutex<HashMap<String, Entry>>>,
    ttl: Duration,
    capacity: usize,
}

#[derive(Clone, Copy)]
struct Entry {
    enrolled: bool,
    expires_at: tokio::time::Instant,
}

impl PeerAuthorCache {
    pub(super) fn new(ttl: Duration, capacity: usize) -> Self {
        debug_assert!(!ttl.is_zero());
        debug_assert!(capacity > 0);
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            ttl,
            capacity,
        }
    }

    pub(super) async fn get_or_try_insert_with<F, Fut>(
        &self,
        pubkey: &str,
        lookup: F,
    ) -> Result<bool, String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<bool, String>>,
    {
        let now = tokio::time::Instant::now();
        if let Some(enrolled) = self.cached(pubkey, now)? {
            return Ok(enrolled);
        }

        // The signed-directory lookup may perform several network requests.
        // Never retain the synchronous cache lock while it is in flight.
        let enrolled = lookup().await?;
        self.insert(pubkey, enrolled, tokio::time::Instant::now())?;
        Ok(enrolled)
    }

    fn cached(&self, pubkey: &str, now: tokio::time::Instant) -> Result<Option<bool>, String> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "peer author cache is unavailable".to_owned())?;
        entries.retain(|_, entry| entry.expires_at > now);
        Ok(entries.get(pubkey).map(|entry| entry.enrolled))
    }

    fn insert(
        &self,
        pubkey: &str,
        enrolled: bool,
        now: tokio::time::Instant,
    ) -> Result<(), String> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "peer author cache is unavailable".to_owned())?;
        entries.retain(|_, entry| entry.expires_at > now);
        if entries.len() >= self.capacity && !entries.contains_key(pubkey) {
            if let Some(oldest) = entries
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(pubkey, _)| pubkey.clone())
            {
                entries.remove(&oldest);
            }
        }
        entries.insert(
            pubkey.to_owned(),
            Entry {
                enrolled,
                expires_at: now + self.ttl,
            },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn positive_and_negative_results_are_cached_until_expiry() {
        let cache = PeerAuthorCache::new(Duration::from_secs(30), 1_024);
        let lookups = AtomicUsize::new(0);

        for expected in [true, false] {
            let pubkey = if expected { "positive" } else { "negative" };
            for _ in 0..2 {
                assert_eq!(
                    cache
                        .get_or_try_insert_with(pubkey, || async {
                            lookups.fetch_add(1, Ordering::SeqCst);
                            Ok(expected)
                        })
                        .await,
                    Ok(expected)
                );
            }
        }
        assert_eq!(lookups.load(Ordering::SeqCst), 2);

        tokio::time::advance(Duration::from_secs(30)).await;
        assert_eq!(
            cache
                .get_or_try_insert_with("positive", || async {
                    lookups.fetch_add(1, Ordering::SeqCst);
                    Ok(false)
                })
                .await,
            Ok(false)
        );
        assert_eq!(lookups.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn lookup_errors_are_not_cached() {
        let cache = PeerAuthorCache::new(Duration::from_secs(30), 1_024);
        let lookups = AtomicUsize::new(0);
        let failed = cache
            .get_or_try_insert_with("peer", || async {
                lookups.fetch_add(1, Ordering::SeqCst);
                Err("directory unavailable".to_owned())
            })
            .await;
        assert_eq!(failed, Err("directory unavailable".to_owned()));
        assert_eq!(
            cache
                .get_or_try_insert_with("peer", || async {
                    lookups.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                })
                .await,
            Ok(true)
        );
        assert_eq!(lookups.load(Ordering::SeqCst), 2);
    }
}
