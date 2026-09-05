//! Channel administration through the existing signed SDK commands.
use super::{
    tools::{error_result, json_result},
    TrustedRelay,
};
use nostr::EventBuilder;
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Discover accessible channels, including archived channels for restoration.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelReadParams {
    /// Omit to list accessible channel metadata; supply to read its metadata and members.
    #[schemars(with = "Option<String>")]
    pub channel_id: Option<uuid::Uuid>,
    /// Continue an earlier page with both cursor fields.
    pub before_created_at: Option<u64>,
    /// Event ID returned in next_cursor.
    pub before_event_id: Option<String>,
    /// Maximum metadata records per page (1–100).
    #[serde(default = "default_limit")]
    pub limit: u16,
}
fn default_limit() -> u16 {
    50
}

/// One channel change, with relay-enforced current operator authority.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChannelApplyParams {
    /// Omit for the current channel, or when creating a channel.
    #[schemars(with = "Option<String>")]
    pub channel_id: Option<uuid::Uuid>,
    /// Requested administrative operation.
    pub action: ChannelAction,
}

/// Typed channel lifecycle, metadata and membership operations.
#[derive(Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ChannelAction {
    /// Create a stream or forum. Returned channel_id identifies the new channel.
    Create {
        name: String,
        channel_type: ChannelType,
        visibility: ChannelVisibility,
        description: Option<String>,
    },
    /// Update selected fields, preserving omitted fields.
    Update {
        name: Option<String>,
        description: Option<String>,
        visibility: Option<ChannelVisibility>,
    },
    /// Reversibly archive the channel. Existing history is retained.
    Archive,
    /// Restore an archived channel.
    Restore,
    /// Set the channel topic; an empty string clears it.
    Topic { text: String },
    /// Set the channel purpose; an empty string clears it.
    Purpose { text: String },
    /// Join an accessible channel as this agent.
    Join,
    /// Leave the channel as this agent.
    Leave,
    /// Add a member or update their role; omit role to preserve an existing role.
    AddMember {
        pubkey: String,
        role: Option<ChannelRole>,
    },
    /// Remove the specified member using existing relay ownership protections.
    RemoveMember { pubkey: String },
}
/// Supported user-created channel surfaces.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelType {
    Stream,
    Forum,
}
/// Channel visibility.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelVisibility {
    Open,
    Private,
}
impl ChannelVisibility {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Private => "private",
        }
    }
}
/// Existing channel membership roles; relay rules govern role changes.
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ChannelRole {
    Owner,
    Admin,
    Member,
    Guest,
    Bot,
}

pub(super) async fn read(
    relay: &Arc<TrustedRelay>,
    params: ChannelReadParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        if !(1..=100).contains(&params.limit) { return Err("limit must be between 1 and 100".into()); }
        if params.before_created_at.is_some() != params.before_event_id.is_some() { return Err("provide both cursor fields".into()); }
        let mut filter = serde_json::json!({"kinds":[39000], "limit": params.limit + 1});
        if let Some(channel) = params.channel_id { filter["#d"] = serde_json::json!([channel]); }
        if let Some(id) = params.before_event_id {
            nostr::EventId::parse(&id).map_err(|_| "invalid cursor event ID")?;
            filter["until"] = serde_json::json!(params.before_created_at);
            filter["before_id"] = serde_json::json!(id);
        }
        relay.fresh_context(&cancellation).await?;
        let mut events = relay.query_signed_events(vec![filter], &cancellation).await?;
        events.sort_by(|a,b| b.created_at.cmp(&a.created_at).then_with(|| a.id.cmp(&b.id)));
        let has_more = events.len() > usize::from(params.limit);
        events.truncate(usize::from(params.limit));
        let next_cursor = if has_more { events.last().map(|e| serde_json::json!({"before_created_at":e.created_at.as_secs(),"before_event_id":e.id.to_hex()})) } else { None };
        let members = if let Some(channel) = params.channel_id {
            relay.query_signed_events(vec![serde_json::json!({"kinds":[39002],"#d":[channel],"limit":1})], &cancellation).await?
        } else { vec![] };
        Ok::<_,String>(serde_json::json!({"channels":events,"members":members,"has_more":has_more,"next_cursor":next_cursor}))
    }.await;
    match result {
        Ok(value) => json_result(&value),
        Err(error) => error_result(error),
    }
}

pub(super) async fn apply(
    relay: &Arc<TrustedRelay>,
    params: ChannelApplyParams,
    cancellation: CancellationToken,
) -> CallToolResult {
    let result = async {
        let creating = matches!(params.action, ChannelAction::Create { .. });
        if creating && params.channel_id.is_some() { return Err("omit channel_id when creating a channel".into()); }
        let channel = if creating { uuid::Uuid::new_v4() } else { match params.channel_id { Some(id) => id, None => relay.bound_chat_channel()? } };
        let builder = build(channel, params.action)?;
        let published = relay.publish_channel_command(builder, &cancellation).await?;
        Ok::<_,String>(serde_json::json!({"channel_id":channel,"event_id":published.event_id,"accepted":published.accepted}))
    }.await;
    match result {
        Ok(value) => json_result(&value),
        Err(error) => error_result(error),
    }
}

fn build(channel: uuid::Uuid, action: ChannelAction) -> Result<EventBuilder, String> {
    use buzz_sdk as sdk;
    let result = match action {
        ChannelAction::Create {
            name,
            channel_type,
            visibility,
            description,
        } => sdk::build_create_channel(
            channel,
            &name,
            Some(match visibility {
                ChannelVisibility::Open => sdk::Visibility::Open,
                ChannelVisibility::Private => sdk::Visibility::Private,
            }),
            Some(match channel_type {
                ChannelType::Stream => sdk::ChannelKind::Stream,
                ChannelType::Forum => sdk::ChannelKind::Forum,
            }),
            description.as_deref(),
            None,
        ),
        ChannelAction::Update {
            name,
            description,
            visibility,
        } => sdk::build_update_channel(
            channel,
            name.as_deref(),
            description.as_deref(),
            visibility.as_ref().map(ChannelVisibility::as_str),
            None,
        ),
        ChannelAction::Archive => sdk::build_archive(channel),
        ChannelAction::Restore => sdk::build_unarchive(channel),
        ChannelAction::Topic { text } => sdk::build_set_topic(channel, &text),
        ChannelAction::Purpose { text } => sdk::build_set_purpose(channel, &text),
        ChannelAction::Join => sdk::build_join(channel),
        ChannelAction::Leave => sdk::build_leave(channel),
        ChannelAction::AddMember { pubkey, role } => sdk::build_add_member(
            channel,
            &pubkey,
            role.map(|role| match role {
                ChannelRole::Owner => sdk::MemberRole::Owner,
                ChannelRole::Admin => sdk::MemberRole::Admin,
                ChannelRole::Member => sdk::MemberRole::Member,
                ChannelRole::Guest => sdk::MemberRole::Guest,
                ChannelRole::Bot => sdk::MemberRole::Bot,
            }),
        ),
        ChannelAction::RemoveMember { pubkey } => sdk::build_remove_member(channel, &pubkey),
    };
    result.map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn archive_restore_and_forum_creation_use_existing_wire_commands() {
        let channel = uuid::Uuid::new_v4();
        let keys = nostr::Keys::generate();
        for (action, archived) in [
            (ChannelAction::Archive, "true"),
            (ChannelAction::Restore, "false"),
        ] {
            let event = build(channel, action)
                .unwrap()
                .sign_with_keys(&keys)
                .unwrap();
            assert_eq!(event.kind.as_u16(), 9002);
            assert!(event
                .tags
                .iter()
                .any(|t| t.as_slice() == ["archived", archived]));
            assert!(event
                .tags
                .iter()
                .any(|t| t.as_slice() == ["h", &channel.to_string()]));
        }
        let action = serde_json::from_value(serde_json::json!({"type":"create","name":"Design","channel_type":"forum","visibility":"open"})).unwrap();
        let event = build(channel, action)
            .unwrap()
            .sign_with_keys(&keys)
            .unwrap();
        assert_eq!(event.kind.as_u16(), 9007);
        assert!(event
            .tags
            .iter()
            .any(|t| t.as_slice() == ["channel_type", "forum"]));
        assert!(build(
            channel,
            ChannelAction::Update {
                name: None,
                description: None,
                visibility: None
            }
        )
        .is_err());
    }
}
