//! Authenticated, server-resolved tenant context returned to durable clients.

use serde::{Deserialize, Serialize};

use crate::tenant::{normalize_host, relay_url_authority};

/// Exact schema discriminator for [`CommunityContext`].
pub const COMMUNITY_CONTEXT_SCHEMA_VERSION: &str = "buzz.context.v1";

/// Tenant identity resolved from the request host and authenticated principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommunityContext {
    /// Exact [`COMMUNITY_CONTEXT_SCHEMA_VERSION`] value.
    pub schema_version: String,
    /// Canonical server-side tenant UUID.
    pub community_id: String,
    /// Canonical host bound by the relay.
    pub host: String,
    /// Authenticated lowercase-hex request principal.
    pub pubkey: String,
}

impl CommunityContext {
    /// Validate the relay-returned context without trusting caller input.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != COMMUNITY_CONTEXT_SCHEMA_VERSION {
            return Err("relay context schema_version is unsupported".into());
        }
        let community = uuid::Uuid::parse_str(&self.community_id)
            .map_err(|_| "relay context community_id is not a UUID".to_owned())?;
        if community.is_nil() || community.to_string() != self.community_id {
            return Err("relay context community_id is not canonical".into());
        }
        let pubkey = nostr::PublicKey::parse(&self.pubkey)
            .map_err(|_| "relay context pubkey is not a public key".to_owned())?;
        if pubkey.to_hex() != self.pubkey {
            return Err("relay context pubkey is not canonical".into());
        }
        if self.host.is_empty()
            || normalize_host(&self.host) != self.host
            || relay_url_authority(&format!("https://{}", self.host)) != self.host
        {
            return Err("relay context host is empty or non-canonical".into());
        }
        Ok(())
    }

    /// Validate this context against the client-selected relay and signer.
    pub fn validate_binding(
        &self,
        expected_host: &str,
        expected_pubkey: &nostr::PublicKey,
    ) -> Result<(), String> {
        self.validate()?;
        let expected = normalize_host(expected_host);
        if expected.is_empty() || expected != self.host {
            return Err("relay context host does not match the configured relay".into());
        }
        if self.pubkey != expected_pubkey.to_hex() {
            return Err("relay context pubkey does not match the client identity".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> CommunityContext {
        CommunityContext {
            schema_version: COMMUNITY_CONTEXT_SCHEMA_VERSION.into(),
            community_id: "12345678-1234-4234-8234-123456789abc".into(),
            host: "relay.example:8443".into(),
            pubkey: nostr::Keys::generate().public_key().to_hex(),
        }
    }

    #[test]
    fn validates_canonical_server_context() {
        assert!(context().validate().is_ok());
    }

    #[test]
    fn rejects_malformed_server_context_fields() {
        for (field, value) in [
            ("schema", "buzz.context.v2"),
            ("community", "not-a-uuid"),
            ("community", "00000000-0000-0000-0000-000000000000"),
            ("host", "bad host/"),
            ("host", "relay.example/extra"),
            ("pubkey", "not-a-pubkey"),
        ] {
            let mut candidate = context();
            match field {
                "schema" => candidate.schema_version = value.into(),
                "community" => candidate.community_id = value.into(),
                "host" => candidate.host = value.into(),
                "pubkey" => candidate.pubkey = value.into(),
                _ => unreachable!(),
            }
            assert!(candidate.validate().is_err(), "accepted {field}={value}");
        }
    }

    #[test]
    fn binding_rejects_wrong_host_or_signer() {
        let candidate = context();
        let signer = nostr::PublicKey::parse(&candidate.pubkey).expect("pubkey");
        assert!(candidate
            .validate_binding("relay.example:8443", &signer)
            .is_ok());
        assert!(candidate
            .validate_binding("other.example", &signer)
            .is_err());
        assert!(candidate
            .validate_binding("relay.example:8443", &nostr::Keys::generate().public_key())
            .is_err());
    }
}
