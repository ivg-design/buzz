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
fn cross_owner_roster_requires_all_verified_authority_layers() {
    let relay = nostr::Keys::generate();
    let self_agent = nostr::Keys::generate();
    let owner_a = nostr::Keys::generate();
    let owner_b = nostr::Keys::generate();
    let revoked_owner = nostr::Keys::generate();
    let agent_a = nostr::Keys::generate();
    let agent_b = nostr::Keys::generate();
    let revoked_agent = nostr::Keys::generate();
    let cosmetic_member = nostr::Keys::generate();

    let roster = signed_event(
        &relay,
        buzz_core::kind::KIND_NIP29_GROUP_MEMBERS,
        "",
        vec![
            Tag::parse(["d", buzz_core::nemo::HOME_CHANNEL]).unwrap(),
            Tag::parse(["p", &self_agent.public_key().to_hex(), "", "bot"]).unwrap(),
            Tag::parse(["p", &agent_a.public_key().to_hex(), "", "bot"]).unwrap(),
            Tag::parse(["p", &agent_b.public_key().to_hex(), "", "bot"]).unwrap(),
            Tag::parse(["p", &revoked_agent.public_key().to_hex(), "", "bot"]).unwrap(),
            Tag::parse(["p", &cosmetic_member.public_key().to_hex(), "", "member"]).unwrap(),
        ],
        10,
    );
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
    let authority = vec![roster, members];
    let candidates = roster_candidates(
        &authority,
        &relay.public_key().to_hex(),
        buzz_core::nemo::HOME_CHANNEL,
        &self_agent.public_key().to_hex(),
    )
    .expect("roster candidates");
    assert_eq!(candidates.len(), 3);
    assert!(!candidates.contains(&self_agent.public_key().to_hex()));
    assert!(!candidates.contains(&cosmetic_member.public_key().to_hex()));

    let direct = direct_members(&authority, &relay.public_key().to_hex()).expect("members");
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

    let peers = resolve_policies(
        &[
            policy(&owner_a, &agent_a, "Worker", 12),
            policy(&owner_b, &agent_b, "Worker", 12),
            policy(&revoked_owner, &revoked_agent, "Revoked", 12),
        ],
        &owners,
    );
    assert_eq!(peers.len(), 2);
    assert!(peers.iter().all(|peer| peer.name == "Worker"));
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

    let valid_roster = signed_event(
        &relay,
        buzz_core::kind::KIND_NIP29_GROUP_MEMBERS,
        "",
        vec![
            Tag::parse(["d", buzz_core::nemo::HOME_CHANNEL]).unwrap(),
            Tag::parse(["p", &agent.public_key().to_hex(), "", "bot"]).unwrap(),
        ],
        10,
    );
    let forged_newer_roster = signed_event(
        &attacker,
        buzz_core::kind::KIND_NIP29_GROUP_MEMBERS,
        "",
        vec![
            Tag::parse(["d", buzz_core::nemo::HOME_CHANNEL]).unwrap(),
            Tag::parse(["p", &other_agent.public_key().to_hex(), "", "bot"]).unwrap(),
        ],
        20,
    );
    let members = signed_event(
        &relay,
        buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST,
        "",
        vec![Tag::parse(["member", &owner.public_key().to_hex(), "member"]).unwrap()],
        10,
    );
    let authority = vec![valid_roster, forged_newer_roster, members];
    let candidates = roster_candidates(
        &authority,
        &relay.public_key().to_hex(),
        buzz_core::nemo::HOME_CHANNEL,
        "",
    )
    .unwrap();
    assert_eq!(candidates, vec![agent.public_key().to_hex()]);

    let direct = direct_members(&authority, &relay.public_key().to_hex()).unwrap();
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
fn authority_queries_are_fixed_to_relay_and_nemo_home() {
    let relay = "a".repeat(64);
    let filters = authority_filters(&relay, buzz_core::nemo::HOME_CHANNEL);
    assert_eq!(filters.len(), 2);
    let encoded = serde_json::to_string(&filters).unwrap();
    assert!(encoded.contains(buzz_core::nemo::HOME_CHANNEL));
    assert!(encoded.contains(&relay));
    assert!(encoded.contains(&buzz_core::kind::KIND_NIP29_GROUP_MEMBERS.to_string()));
    assert!(encoded.contains(&buzz_core::kind::KIND_NIP43_MEMBERSHIP_LIST.to_string()));
}
