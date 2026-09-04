use nostr::PublicKey;

use buzz_core::job::JobEvent;
use buzz_core::kind::KIND_PROJECT;
use buzz_core::tenant::TenantContext;
use buzz_db::EventQuery;

use crate::state::AppState;

use super::gate::JobAuthError;

pub(super) async fn load_job_event_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    id: &str,
    field: &str,
) -> Result<JobEvent, JobAuthError> {
    let bytes = hex::decode(id)
        .map_err(|_| JobAuthError::Invalid(format!("{field} must be a hex event ID")))?;
    let mut query = EventQuery::for_community(tenant.community());
    query.ids = Some(vec![bytes]);
    query.limit = Some(2);
    query.max_limit = Some(2);
    let mut stored = lock
        .query_events(&query)
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading {field}: {error}")))?;
    if stored.len() != 1 {
        return Err(JobAuthError::Invalid(format!("{field} event not found")));
    }
    JobEvent::parse(&stored.pop().expect("checked length").event)
        .map_err(|error| JobAuthError::Invalid(format!("invalid {field} event: {error}")))
}

pub(super) async fn validate_project_binding(
    tenant: &TenantContext,
    state: &AppState,
    job: &JobEvent,
) -> Result<(), JobAuthError> {
    let common = job.common();
    let (kind, author, d_tag) = parse_project_address(&common.project.address)?;
    let mut query = EventQuery::for_community(tenant.community());
    query.kinds = Some(vec![kind as i32]);
    query.pubkey = Some(author.to_bytes().to_vec());
    query.d_tag = Some(d_tag.to_owned());
    query.global_only = true;
    query.limit = Some(2);
    query.max_limit = Some(2);
    let projects = state
        .db
        .query_events_for_event_write(&query)
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading project address: {error}")))?;
    let project = projects
        .first()
        .ok_or_else(|| JobAuthError::Restricted("project address not found".into()))?;
    if projects.len() != 1 {
        return Err(JobAuthError::Internal(
            "project address resolved to multiple live heads".into(),
        ));
    }
    let channels: Vec<&[String]> = project
        .event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            (parts.first().map(String::as_str) == Some("buzz-channel")).then_some(parts)
        })
        .collect();
    if channels.len() != 1 || channels[0] != ["buzz-channel", common.project.home_channel.as_str()]
    {
        return Err(JobAuthError::Restricted(
            "project address is not bound to project.home_channel".into(),
        ));
    }
    Ok(())
}

pub(super) async fn validate_project_binding_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    job: &JobEvent,
) -> Result<buzz_core::StoredEvent, JobAuthError> {
    let common = job.common();
    let (kind, author, d_tag) = parse_project_address(&common.project.address)?;
    let id = lock
        .lock_parameterized_head(tenant.community(), kind as i32, &author.to_bytes(), d_tag)
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking project head: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted("project address not found".into()))?;
    let mut query = EventQuery::for_community(tenant.community());
    query.ids = Some(vec![id]);
    query.limit = Some(2);
    query.max_limit = Some(2);
    let mut projects = lock
        .query_events(&query)
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading locked project head: {error}")))?;
    if projects.len() != 1 {
        return Err(JobAuthError::Internal(
            "locked project head did not resolve uniquely".into(),
        ));
    }
    let project = projects.pop().expect("checked length");
    let channels: Vec<&[String]> = project
        .event
        .tags
        .iter()
        .filter_map(|tag| {
            let parts = tag.as_slice();
            (parts.first().map(String::as_str) == Some("buzz-channel")).then_some(parts)
        })
        .collect();
    if channels.len() != 1 || channels[0] != ["buzz-channel", common.project.home_channel.as_str()]
    {
        return Err(JobAuthError::Restricted(
            "project address is not bound to project.home_channel".into(),
        ));
    }
    Ok(project)
}

pub(super) async fn resolve_repository_link_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    project: &buzz_core::StoredEvent,
    canonical_repository: &str,
) -> Result<(String, buzz_core::StoredEvent), JobAuthError> {
    let mut matches = Vec::new();
    for tag in project.event.tags.iter() {
        let parts = tag.as_slice();
        if parts.first().map(String::as_str) != Some("a") {
            continue;
        }
        let Some(coordinate) = parts.get(1) else {
            continue;
        };
        let mut coordinate_parts = coordinate.splitn(3, ':');
        if coordinate_parts.next() != Some("30617") {
            continue;
        }
        let Some(owner_text) = coordinate_parts.next() else {
            continue;
        };
        let Some(d_tag) = coordinate_parts.next().filter(|value| !value.is_empty()) else {
            continue;
        };
        let owner = PublicKey::parse(owner_text).map_err(|_| {
            JobAuthError::Internal("stored Project has an invalid repository coordinate".into())
        })?;
        let Some(id) = lock
            .lock_parameterized_head(tenant.community(), 30_617, &owner.to_bytes(), d_tag)
            .await
            .map_err(|error| {
                JobAuthError::Internal(format!("locking repository announcement: {error}"))
            })?
        else {
            continue;
        };
        let mut query = EventQuery::for_community(tenant.community());
        query.ids = Some(vec![id]);
        query.limit = Some(2);
        query.max_limit = Some(2);
        let mut announcements = lock.query_events(&query).await.map_err(|error| {
            JobAuthError::Internal(format!("loading repository announcement: {error}"))
        })?;
        if announcements.len() != 1 {
            return Err(JobAuthError::Internal(
                "repository coordinate did not resolve uniquely".into(),
            ));
        }
        let announcement = announcements.pop().expect("checked length");
        let url_matches = announcement.event.tags.iter().any(|tag| {
            let fields = tag.as_slice();
            match fields.first().map(String::as_str) {
                Some("web") => fields.get(1).map(String::as_str) == Some(canonical_repository),
                Some("clone") => fields
                    .iter()
                    .skip(1)
                    .any(|value| value == canonical_repository),
                _ => false,
            }
        });
        if url_matches {
            matches.push((coordinate.clone(), announcement));
        }
    }
    if matches.len() != 1 {
        return Err(JobAuthError::Restricted(
            "current Project must reference exactly one live repository announcement matching repository.canonical"
                .into(),
        ));
    }
    Ok(matches.pop().expect("checked length"))
}

pub(super) fn parse_project_address(address: &str) -> Result<(u32, PublicKey, &str), JobAuthError> {
    let mut parts = address.splitn(3, ':');
    let kind = parts
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(|| {
            JobAuthError::Invalid("project.address must be a NIP-33 coordinate".into())
        })?;
    let author_text = parts
        .next()
        .ok_or_else(|| JobAuthError::Invalid("project.address must name an author".into()))?;
    let d_tag = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| JobAuthError::Invalid("project.address must name a d tag".into()))?;
    if kind != KIND_PROJECT {
        return Err(JobAuthError::Invalid(
            "project.address kind must be 30621".into(),
        ));
    }
    let author = PublicKey::parse(author_text)
        .map_err(|_| JobAuthError::Invalid("project.address author must be a public key".into()))?;
    if author.to_hex() != author_text {
        return Err(JobAuthError::Invalid(
            "project.address author must use canonical lowercase hex".into(),
        ));
    }
    Ok((kind, author, d_tag))
}

pub(super) async fn validate_sponsor(
    tenant: &TenantContext,
    state: &AppState,
    job: &JobEvent,
) -> Result<(), JobAuthError> {
    let common = job.common();
    let actor = PublicKey::parse(&common.sender_pubkey)
        .map_err(|_| JobAuthError::Invalid("sender_pubkey must be a public key".into()))?;
    let sponsor = PublicKey::parse(&common.sponsor.pubkey)
        .map_err(|_| JobAuthError::Invalid("sponsor.pubkey must be a public key".into()))?;
    let record = state
        .db
        .get_agent_channel_policy(tenant.community(), &actor.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("loading job actor ownership: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted("job actor is not a registered user".into()))?;
    let effective_sponsor = record.1.unwrap_or_else(|| actor.to_bytes().to_vec());
    if effective_sponsor != sponsor.to_bytes() {
        return Err(JobAuthError::Restricted(
            "sponsor.pubkey does not match authoritative agent ownership".into(),
        ));
    }
    Ok(())
}

pub(super) async fn validate_sponsor_locked(
    tenant: &TenantContext,
    lock: &mut buzz_db::JobOperationLock,
    job: &JobEvent,
) -> Result<(), JobAuthError> {
    let common = job.common();
    let actor = PublicKey::parse(&common.sender_pubkey)
        .map_err(|_| JobAuthError::Invalid("sender_pubkey must be a public key".into()))?;
    let sponsor = PublicKey::parse(&common.sponsor.pubkey)
        .map_err(|_| JobAuthError::Invalid("sponsor.pubkey must be a public key".into()))?;
    let owner = lock
        .user_owner_for_share(tenant.community(), &actor.to_bytes())
        .await
        .map_err(|error| JobAuthError::Internal(format!("locking job actor ownership: {error}")))?
        .ok_or_else(|| JobAuthError::Restricted("job actor is not a registered user".into()))?;
    let effective_sponsor = owner.unwrap_or_else(|| actor.to_bytes().to_vec());
    if effective_sponsor != sponsor.to_bytes() {
        return Err(JobAuthError::Restricted(
            "sponsor.pubkey does not match authoritative agent ownership".into(),
        ));
    }
    Ok(())
}
