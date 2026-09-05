use super::*;
use nostr::{EventBuilder, Tag, Timestamp};

fn signed_event(
    keys: &nostr::Keys,
    kind: u32,
    content: &str,
    tags: Vec<Tag>,
    created_at: u64,
) -> Event {
    EventBuilder::new(Kind::Custom(kind as u16), content)
        .tags(tags)
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(keys)
        .expect("signed fixture")
}

fn profile(owner: &nostr::Keys, agent: &nostr::Keys, created_at: u64) -> Event {
    let auth = buzz_sdk::nip_oa::compute_auth_tag(owner, &agent.public_key(), "kind=0")
        .expect("owner auth tag");
    let auth: Tag = serde_json::from_str(&auth).expect("auth tag JSON");
    EventBuilder::metadata(&nostr::Metadata::new())
        .tags([auth])
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(agent)
        .expect("agent profile")
}

fn policy(owner: &nostr::Keys, agent: &nostr::Keys, name: &str, created_at: u64) -> Event {
    signed_event(
        owner,
        buzz_core::kind::KIND_MANAGED_AGENT,
        &serde_json::json!({"name": name}).to_string(),
        vec![Tag::parse(["d", &agent.public_key().to_hex()]).unwrap()],
        created_at,
    )
}

#[test]
fn cross_owner_discovery_does_not_require_home_roster_rows() {
    let relay = nostr::Keys::generate();
    let self_agent = nostr::Keys::generate();
    let owner_a = nostr::Keys::generate();
    let owner_b = nostr::Keys::generate();
    let revoked_owner = nostr::Keys::generate();
    let agent_a = nostr::Keys::generate();
    let agent_b = nostr::Keys::generate();
    let revoked_agent = nostr::Keys::generate();
    let members = signed_event(
        &relay,
        buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST,
        "",
        vec![
            Tag::parse(["member", &owner_a.public_key().to_hex(), "member"]).unwrap(),
            Tag::parse(["member", &owner_b.public_key().to_hex(), "member"]).unwrap(),
        ],
        10,
    );
    let authority = vec![members];
    let direct = direct_members(&authority, &relay.public_key().to_hex()).expect("members");
    let policies = vec![
        policy(&owner_a, &agent_a, "Worker", 12),
        policy(&owner_b, &agent_b, "Worker", 12),
        policy(&revoked_owner, &revoked_agent, "Revoked", 12),
        policy(&owner_a, &self_agent, "Self", 12),
    ];
    let self_pubkey = self_agent.public_key().to_hex();
    let candidates = policy_candidates(&policies, &direct, Some(&self_pubkey));
    assert_eq!(candidates.len(), 2);
    assert!(!candidates.contains(&self_agent.public_key().to_hex()));
    assert!(!candidates.contains(&revoked_agent.public_key().to_hex()));
    let including_self = policy_candidates(&policies, &direct, None);
    assert_eq!(including_self.len(), 3);
    assert!(including_self.contains(&self_agent.public_key().to_hex()));
    let owners_including_self = verified_profile_owners(
        &[
            profile(&owner_a, &agent_a, 11),
            profile(&owner_b, &agent_b, 11),
            profile(&owner_a, &self_agent, 11),
        ],
        &including_self,
        &direct,
    );
    assert!(resolve_policies(&policies, &owners_including_self)
        .iter()
        .any(|peer| peer.pubkey == self_pubkey));
    let owners = verified_profile_owners(
        &[
            profile(&owner_a, &agent_a, 11),
            profile(&owner_b, &agent_b, 11),
            profile(&revoked_owner, &revoked_agent, 11),
        ],
        &candidates,
        &direct,
    );
    assert_eq!(owners.len(), 2, "revoked owner must not authorize a peer");

    let peers = resolve_policies(&policies, &owners);
    assert_eq!(peers.len(), 2);
    assert!(peers.iter().all(|peer| peer.name == "Worker"));
    assert!(peers.iter().all(|peer| !peer.owner_pubkey.is_empty()));
    let encoded = serde_json::to_string(&peers).unwrap();
    assert!(!encoded.contains("owner_pubkey"));
    assert_ne!(
        peers[0].pubkey, peers[1].pubkey,
        "duplicate names stay explicit"
    );
}

#[test]
fn forged_or_mismatched_directory_evidence_cannot_create_a_peer() {
    let relay = nostr::Keys::generate();
    let attacker = nostr::Keys::generate();
    let owner = nostr::Keys::generate();
    let agent = nostr::Keys::generate();
    let other_agent = nostr::Keys::generate();

    let members = signed_event(
        &relay,
        buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST,
        "",
        vec![Tag::parse(["member", &owner.public_key().to_hex(), "member"]).unwrap()],
        10,
    );
    let authority = vec![members];
    let direct = direct_members(&authority, &relay.public_key().to_hex()).unwrap();
    let valid_policy = policy(&owner, &agent, "Valid", 10);
    let forged_policy = policy(&attacker, &other_agent, "Forged", 20);
    let candidates = policy_candidates(&[valid_policy.clone(), forged_policy], &direct, None);
    assert_eq!(candidates, vec![agent.public_key().to_hex()]);

    let owners = verified_profile_owners(&[profile(&owner, &agent, 11)], &candidates, &direct);
    let wrong_owner_policy = policy(&attacker, &agent, "Forged", 30);
    let wrong_agent_policy = policy(&owner, &other_agent, "Other", 30);
    assert!(resolve_policies(&[wrong_owner_policy, wrong_agent_policy], &owners).is_empty());

    let invisible = policy(&owner, &agent, "Bad\u{202e}Name", 31);
    assert!(resolve_policies(&[invisible], &owners).is_empty());

    let ambiguous_coordinate = signed_event(
        &owner,
        buzz_core::kind::KIND_MANAGED_AGENT,
        &serde_json::json!({"name": "Ambiguous"}).to_string(),
        vec![
            Tag::parse(["d", &agent.public_key().to_hex()]).unwrap(),
            Tag::parse(["d", &other_agent.public_key().to_hex()]).unwrap(),
        ],
        32,
    );
    assert!(resolve_policies(&[ambiguous_coordinate], &owners).is_empty());
}

#[test]
fn authority_and_policy_queries_are_fixed_to_current_members() {
    let relay = "a".repeat(64);
    let filters = authority_filters(&relay);
    assert_eq!(filters.len(), 1);
    let encoded = serde_json::to_string(&filters).unwrap();
    assert!(encoded.contains(&relay));
    assert!(encoded.contains(&buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST.to_string()));

    let members = ["b".repeat(64), "c".repeat(64)]
        .into_iter()
        .collect::<BTreeSet<_>>();
    let policies = policy_filters(&members);
    let encoded = serde_json::to_string(&policies).unwrap();
    assert!(encoded.contains(&buzz_core::kind::KIND_MANAGED_AGENT.to_string()));
    assert!(encoded.contains(&"b".repeat(64)));
    assert!(encoded.contains(&"c".repeat(64)));
    assert!(encoded.contains(&(MAX_PEERS + 1).to_string()));
}
