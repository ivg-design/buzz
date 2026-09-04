//! Signed, channel-scoped agent job events (kinds 43001-43006).
//!
//! The relay event is the durable transport record. Its signature and routing
//! tags are authoritative; the JSON body repeats those values so offline tools
//! can reject a body/tag substitution before projecting job state.

/// Exact JSON protocol discriminator for agent jobs.
pub const JOB_SCHEMA_VERSION: &str = "buzz.jobs.v1";
/// Largest accepted opaque idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;
/// Maximum lifetime accepted by the relay from receipt time (seven days).
pub const MAX_JOB_TTL_SECONDS: i64 = 7 * 24 * 60 * 60;
/// Grace after expiry for signed terminal audit closure (24 hours).
pub const JOB_TERMINAL_AUDIT_GRACE_SECONDS: i64 = 24 * 60 * 60;
const MAX_SHORT_TEXT_BYTES: usize = 512;
const MAX_MESSAGE_BYTES: usize = 8 * 1024;
const MAX_LIST_ITEMS: usize = 256;

mod event;
mod model;
#[cfg(test)]
mod tests;
mod validation;

pub use event::{build_job_tags, semantic_request_digest};
pub use model::*;
pub(crate) use validation::{parse_strict_json, validate_project_address, validate_repository};
