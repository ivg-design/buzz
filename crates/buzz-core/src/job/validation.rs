//! Strict body, reference, and duplicate-key validation for job envelopes.

mod body;
mod primitives;
mod references;
mod wire;

pub(super) use body::{
    github_repository_tag, make_tag, require_prior, validate_allowed_tags, validate_common,
    validate_exact_tag, validate_followup, validate_no_secret_material,
    validate_optional_exact_tag,
};
pub(crate) use body::{validate_project_address, validate_repository};
pub(super) use primitives::{
    validate_event_id, validate_hex, validate_list, validate_machine_token, validate_pubkey,
    validate_text,
};
pub(super) use references::validate_inert_references;
#[cfg(test)]
pub(super) use references::validate_portable_references;
pub(crate) use wire::parse_strict_json;
pub(super) use wire::{validate_wire_keys, UniqueValue};
