use base64::Engine;
use nostr::{EventBuilder, JsonUtil, Kind, Tag};
use tokio_util::sync::CancellationToken;

use super::TrustedRelay;

pub async fn fetch(
    relay: &TrustedRelay,
    source: &str,
    cancellation: &CancellationToken,
) -> Result<Option<Vec<u8>>, String> {
    let Some(target) = scoped_media_target(&relay.base_url, source)? else {
        return Ok(None);
    };
    relay.fresh_context(cancellation).await?;
    let authorization = sign_media_get(&relay.keys, &relay.relay_host)?;
    let request = relay.with_auth(
        relay
            .http
            .get(target)
            .header("Authorization", authorization),
    );
    let bytes = super::relay::send_bounded_cancellable(
        request,
        cancellation,
        "private media fetch",
        crate::view_image::MAX_SOURCE_BYTES,
    )
    .await?;
    Ok(Some(bytes))
}

fn scoped_media_target(relay_base: &str, source: &str) -> Result<Option<reqwest::Url>, String> {
    let target = match reqwest::Url::parse(source) {
        Ok(url) if matches!(url.scheme(), "http" | "https") => url,
        _ => return Ok(None),
    };
    let base = reqwest::Url::parse(relay_base)
        .map_err(|_| "trusted relay URL became invalid".to_owned())?;
    if !target.path().starts_with("/media/")
        || target.scheme() != base.scheme()
        || target.host_str() != base.host_str()
        || target.port_or_known_default() != base.port_or_known_default()
        || !target.username().is_empty()
        || target.password().is_some()
        || target.query().is_some()
        || target.fragment().is_some()
    {
        return Ok(None);
    }
    Ok(Some(target))
}

fn sign_media_get(keys: &nostr::Keys, authority: &str) -> Result<String, String> {
    let expiry = nostr::Timestamp::now().as_secs() + 600;
    let tags = vec![
        Tag::parse(["t", "get"]).map_err(|_| "media auth type binding failed".to_owned())?,
        Tag::parse(["expiration", &expiry.to_string()])
            .map_err(|_| "media auth expiry binding failed".to_owned())?,
        Tag::parse(["server", authority])
            .map_err(|_| "media auth server binding failed".to_owned())?,
    ];
    let event = EventBuilder::new(Kind::Custom(24242), "Get buzz-media")
        .tags(tags)
        .sign_with_keys(keys)
        .map_err(|_| "media auth signing failed".to_owned())?;
    Ok(format!(
        "Nostr {}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(event.as_json().as_bytes())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_token_is_only_a_bounded_get_capability() {
        let keys = nostr::Keys::generate();
        let header = sign_media_get(&keys, "relay.example").expect("token");
        let encoded = header.strip_prefix("Nostr ").expect("scheme");
        let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .expect("base64");
        let event: nostr::Event = serde_json::from_slice(&json).expect("event");
        assert_eq!(u32::from(event.kind.as_u16()), 24242);
        let tags: Vec<Vec<String>> = event
            .tags
            .iter()
            .map(|tag| tag.as_slice().to_vec())
            .collect();
        assert!(tags.iter().any(|tag| tag == &["t", "get"]));
        assert!(tags.iter().any(|tag| tag == &["server", "relay.example"]));
        assert!(!tags
            .iter()
            .any(|tag| tag.first().is_some_and(|name| name == "x")));
    }

    #[test]
    fn private_media_scope_requires_exact_tls_origin_and_clean_path() {
        let base = "https://relay.example";
        assert!(scoped_media_target(base, "https://relay.example/media/abc")
            .unwrap()
            .is_some());
        for rejected in [
            "http://relay.example:443/media/abc",
            "https://other.example/media/abc",
            "https://relay.example/not-media/abc",
            "https://relay.example/media/abc?token=caller",
            "https://user@relay.example/media/abc",
        ] {
            assert!(
                scoped_media_target(base, rejected).unwrap().is_none(),
                "accepted {rejected}"
            );
        }
    }
}
