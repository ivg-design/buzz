//! Verified peer discovery for the dedicated Nemo A2A workspace.

use std::collections::{BTreeSet, HashMap};

use nostr::{Event, Kind, PublicKey};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::TrustedRelay;

const MAX_PEERS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VerifiedPeer {
    pub name: String,
    pub pubkey: String,
    /// Verified owner used internally for inherited channel membership checks.
    /// Tool responses omit it because callers address the agent identity.
    #[serde(skip_serializing)]
    pub owner_pubkey: String,
}

/// Discover managed agents whose owners are currently authorized for the exact
/// Nemo community. Every returned identity has three independent pieces of
/// evidence: an agent-signed NIP-OA profile, an active direct-member owner and
/// an owner-signed managed-agent definition. The dedicated Nemo runtime binds
/// every enrolled agent to HOME, so a stale or missing cosmetic channel-roster
/// row must not hide an otherwise authorized peer. Model input cannot widen any
/// of these query scopes.
pub async fn discover(
    relay: &TrustedRelay,
    cancellation: &CancellationToken,
) -> Result<Vec<VerifiedPeer>, String> {
    let self_pubkey = relay.signer_pubkey();
    discover_with_exclusion(relay, cancellation, Some(&self_pubkey)).await
}

/// Resolve the same signed enrolled-agent directory while retaining the
/// current signer. Persistent thread participant lists may include that agent.
pub(super) async fn discover_including_self(
    relay: &TrustedRelay,
    cancellation: &CancellationToken,
) -> Result<Vec<VerifiedPeer>, String> {
    discover_with_exclusion(relay, cancellation, None).await
}

async fn discover_with_exclusion(
    relay: &TrustedRelay,
    cancellation: &CancellationToken,
    excluded_pubkey: Option<&str>,
) -> Result<Vec<VerifiedPeer>, String> {
    if !relay.grants.is_managed_nemo() {
        return Err("verified peer discovery is unavailable outside the Nemo workspace".into());
    }
    relay.fresh_context(cancellation).await?;
    let channel = relay.bound_a2a_channel()?.to_owned();
    if channel != buzz_core::nemo::HOME_CHANNEL {
        return Err("verified peer discovery is outside the Nemo HOME channel".into());
    }
    let relay_pubkey = relay.relay_signer_pubkey(cancellation).await?;

    let authority_events = relay
        .query_signed_events(authority_filters(&relay_pubkey), cancellation)
        .await?;
    let direct_members = direct_members(&authority_events, &relay_pubkey)?;
    if direct_members.is_empty() {
        return Ok(Vec::new());
    }
    let policy_events = relay
        .query_signed_events(policy_filters(&direct_members), cancellation)
        .await?;
    let candidates = policy_candidates(&policy_events, &direct_members, excluded_pubkey);
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let candidate_limit = MAX_PEERS + usize::from(excluded_pubkey.is_none());
    if candidates.len() > candidate_limit {
        return Err("verified Nemo peer roster exceeds the bounded tool limit".into());
    }
    let profile_events = relay
        .query_signed_events(
            vec![serde_json::json!({
                "kinds": [0],
                "authors": candidates,
                "limit": candidates.len(),
            })],
            cancellation,
        )
        .await?;
    let owners = verified_profile_owners(&profile_events, &candidates, &direct_members);
    if owners.is_empty() {
        return Ok(Vec::new());
    }

    Ok(resolve_policies(&policy_events, &owners))
}

fn authority_filters(relay_pubkey: &str) -> Vec<serde_json::Value> {
    vec![serde_json::json!({
        "kinds": [buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST],
        "authors": [relay_pubkey],
        "limit": 1,
    })]
}

fn policy_filters(direct_members: &BTreeSet<String>) -> Vec<serde_json::Value> {
    vec![serde_json::json!({
        "kinds": [buzz_core::kind::KIND_MANAGED_AGENT],
        "authors": direct_members,
        "limit": MAX_PEERS + 1,
    })]
}

fn policy_candidates(
    events: &[Event],
    direct_members: &BTreeSet<String>,
    excluded_pubkey: Option<&str>,
) -> Vec<String> {
    let mut candidates = BTreeSet::new();
    for event in events {
        if event.kind.as_u16() as u32 != buzz_core::kind::KIND_MANAGED_AGENT
            || event.verify().is_err()
            || !direct_members.contains(&event.pubkey.to_hex())
        {
            continue;
        }
        let Some(pubkey) = single_tag_value(event, "d").and_then(canonical_pubkey) else {
            continue;
        };
        if excluded_pubkey != Some(pubkey.as_str()) {
            candidates.insert(pubkey);
        }
    }
    candidates.into_iter().collect()
}

fn direct_members(events: &[Event], relay_pubkey: &str) -> Result<BTreeSet<String>, String> {
    let membership = latest_matching(events, |event| {
        event.kind.as_u16() as u32 == buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST
            && event.pubkey.to_hex() == relay_pubkey
    })
    .ok_or_else(|| "relay did not return its current collaborator membership".to_owned())?;
    ensure_valid_event(membership)?;

    let mut members = BTreeSet::new();
    for tag in tags_named(membership, "member") {
        if let Some(pubkey) = tag.get(1).and_then(|value| canonical_pubkey(value)) {
            members.insert(pubkey);
        }
    }
    // Backward-compatible NIP-29-shaped membership snapshots.
    for tag in tags_named(membership, "p") {
        if let Some(pubkey) = tag.get(1).and_then(|value| canonical_pubkey(value)) {
            members.insert(pubkey);
        }
    }
    Ok(members)
}

fn verified_profile_owners(
    events: &[Event],
    candidates: &[String],
    direct_members: &BTreeSet<String>,
) -> Vec<(String, String)> {
    let candidate_set = candidates.iter().cloned().collect::<BTreeSet<_>>();
    let mut latest = HashMap::<String, &Event>::new();
    for event in events {
        let agent = event.pubkey.to_hex();
        if event.kind != Kind::Metadata || !candidate_set.contains(&agent) {
            continue;
        }
        if latest
            .get(&agent)
            .is_none_or(|previous| event_is_newer(event, previous))
        {
            latest.insert(agent, event);
        }
    }
    let mut owners = latest
        .into_iter()
        .filter_map(|(agent, profile)| {
            let owner = profile_owner(profile)?;
            direct_members.contains(&owner).then_some((agent, owner))
        })
        .collect::<Vec<_>>();
    owners.sort();
    owners
}

fn resolve_policies(events: &[Event], owners: &[(String, String)]) -> Vec<VerifiedPeer> {
    let expected = owners.iter().cloned().collect::<HashMap<_, _>>();
    let mut latest = HashMap::<String, &Event>::new();
    for event in events {
        if event.kind.as_u16() as u32 != buzz_core::kind::KIND_MANAGED_AGENT
            || event.verify().is_err()
        {
            continue;
        }
        let Some(agent) = single_tag_value(event, "d") else {
            continue;
        };
        let author = event.pubkey.to_hex();
        if expected.get(agent) != Some(&author) {
            continue;
        }
        if latest
            .get(agent)
            .is_none_or(|previous| event_is_newer(event, previous))
        {
            latest.insert(agent.to_owned(), event);
        }
    }

    let mut peers = latest
        .into_iter()
        .filter_map(|(pubkey, event)| {
            let content: ManagedAgentIdentity =
                serde_json::from_str(event.content.as_ref()).ok()?;
            let owner_pubkey = expected.get(&pubkey)?.clone();
            valid_peer_name(&content.name).then_some(VerifiedPeer {
                name: content.name,
                pubkey,
                owner_pubkey,
            })
        })
        .collect::<Vec<_>>();
    peers.sort_by(|left, right| {
        left.name
            .to_lowercase()
            .cmp(&right.name.to_lowercase())
            .then_with(|| left.pubkey.cmp(&right.pubkey))
    });
    peers
}

#[derive(Deserialize)]
struct ManagedAgentIdentity {
    name: String,
}

fn profile_owner(event: &Event) -> Option<String> {
    if event.kind != Kind::Metadata || event.verify().is_err() {
        return None;
    }
    let mut auth_tags = tags_named(event, "auth");
    let auth_tag = auth_tags.next()?;
    if auth_tags.next().is_some() {
        return None;
    }
    let json = serde_json::to_string(auth_tag).ok()?;
    buzz_sdk::nip_oa::parse_auth_tag(&json).ok()?;
    let owner = buzz_sdk::nip_oa::verify_auth_tag(&json, &event.pubkey).ok()?;
    let conditions = auth_tag.get(2)?;
    let applies = conditions.is_empty()
        || conditions.split('&').all(|clause| {
            if let Some(value) = clause.strip_prefix("kind=") {
                value.parse::<u16>() == Ok(event.kind.as_u16())
            } else if let Some(value) = clause.strip_prefix("created_at<") {
                value
                    .parse::<u64>()
                    .is_ok_and(|bound| event.created_at.as_secs() < bound)
            } else if let Some(value) = clause.strip_prefix("created_at>") {
                value
                    .parse::<u64>()
                    .is_ok_and(|bound| event.created_at.as_secs() > bound)
            } else {
                false
            }
        });
    applies.then(|| owner.to_hex())
}

fn valid_peer_name(name: &str) -> bool {
    !name.trim().is_empty()
        && name.chars().count() <= 128
        && name.chars().all(|character| {
            !character.is_control()
                && !matches!(
                    character,
                    '\u{061c}'
                        | '\u{200b}'
                        | '\u{200e}'..='\u{200f}'
                        | '\u{202a}'..='\u{202e}'
                        | '\u{2060}'..='\u{206f}'
                        | '\u{feff}'
                )
        })
}

fn canonical_pubkey(value: &str) -> Option<String> {
    let key = PublicKey::parse(value).ok()?;
    let canonical = key.to_hex();
    (canonical == value).then_some(canonical)
}

fn ensure_valid_event(event: &Event) -> Result<(), String> {
    event
        .verify()
        .map_err(|_| "relay authority snapshot signature is invalid".to_owned())
}

fn latest_matching(events: &[Event], predicate: impl Fn(&Event) -> bool) -> Option<&Event> {
    events
        .iter()
        .filter(|event| predicate(event))
        .reduce(|previous, candidate| {
            if event_is_newer(candidate, previous) {
                candidate
            } else {
                previous
            }
        })
}

fn event_is_newer(candidate: &Event, previous: &Event) -> bool {
    candidate.created_at > previous.created_at
        || (candidate.created_at == previous.created_at && candidate.id < previous.id)
}

fn single_tag_value<'a>(event: &'a Event, name: &'a str) -> Option<&'a str> {
    let mut tags = tags_named(event, name);
    let value = tags.next()?.get(1)?.as_str();
    tags.next().is_none().then_some(value)
}

fn tags_named<'a>(event: &'a Event, name: &'a str) -> impl Iterator<Item = &'a [String]> + 'a {
    event.tags.iter().filter_map(move |tag| {
        let slice = tag.as_slice();
        (slice.first().map(String::as_str) == Some(name)).then_some(slice)
    })
}

#[cfg(test)]
#[path = "peers_tests.rs"]
mod tests;
