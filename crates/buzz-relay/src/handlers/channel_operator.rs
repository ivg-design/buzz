//! Resolve managed-agent channel commands to their currently authorized owner.
use crate::state::AppState;
use buzz_core::tenant::TenantContext;
use nostr::Event;
use uuid::Uuid;

/// Keep the event's agent signature and attribution; inherit only existing
/// channel owner/admin authority for administrative commands. Never change
/// self-removal semantics, or allow a former community owner to retain access.
pub(super) async fn actor(
    tenant: &TenantContext,
    state: &AppState,
    channel: Uuid,
    event: &Event,
) -> anyhow::Result<Vec<u8>> {
    let agent = event.pubkey.to_bytes().to_vec();
    if !matches!(event.kind.as_u16(), 9000 | 9001 | 9002) {
        return Ok(agent);
    }
    if event.kind.as_u16() == 9001
        && event.tags.iter().any(|t| {
            t.as_slice().first().is_some_and(|v| v == "p")
                && t.as_slice().get(1) == Some(&event.pubkey.to_hex())
        })
    {
        return Ok(agent);
    }
    let members = state
        .db
        .get_members_for_event_write(tenant.community(), channel)
        .await?;
    if members
        .iter()
        .any(|m| m.pubkey == agent && is_operator(&m.role))
    {
        return Ok(agent);
    }
    let Some((_, Some(owner))) = state
        .db
        .get_agent_channel_policy(tenant.community(), &agent)
        .await?
    else {
        return Ok(agent);
    };
    if state
        .db
        .get_relay_member(tenant.community(), &hex::encode(&owner))
        .await?
        .is_some()
        && members
            .iter()
            .any(|m| m.pubkey == owner && is_operator(&m.role))
    {
        return Ok(owner);
    }
    Ok(agent)
}

fn is_operator(role: &str) -> bool {
    matches!(role, "owner" | "admin")
}

#[cfg(test)]
mod tests {
    #[test]
    fn membership_does_not_imply_administration() {
        for (role, expected) in [
            ("owner", true),
            ("admin", true),
            ("member", false),
            ("bot", false),
            ("guest", false),
            ("", false),
        ] {
            assert_eq!(super::is_operator(role), expected);
        }
    }
}
