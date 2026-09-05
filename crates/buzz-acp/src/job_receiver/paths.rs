use std::path::{Component, Path};

use buzz_core::job::JobRequest;

/// Reject host-local and symlink-escaped request paths before prompt admission.
pub fn request_paths_are_contained(repository_root: &Path, request: &JobRequest) -> bool {
    let Ok(root) = repository_root.canonicalize() else {
        return false;
    };
    request.common.repository.paths.iter().all(|value| {
        let path = Path::new(value);
        if path.is_absolute()
            || value.contains('\\')
            || path.components().any(|component| match component {
                Component::Normal(name) => name
                    .to_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(".git")),
                _ => true,
            })
        {
            return false;
        }
        let mut candidate = root.clone();
        for component in path.components() {
            candidate.push(component.as_os_str());
            match std::fs::symlink_metadata(&candidate) {
                Ok(_) => match candidate.canonicalize() {
                    Ok(resolved) if resolved.starts_with(&root) => {}
                    _ => return false,
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return false,
            }
        }
        true
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use buzz_core::job::{JobCommon, JobProject, JobRepository, JobSponsor, JOB_SCHEMA_VERSION};
    use nostr::Keys;
    use uuid::Uuid;

    fn request(path: &str) -> JobRequest {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        JobRequest {
            common: JobCommon {
                schema_version: JOB_SCHEMA_VERSION.into(),
                operation_id: Uuid::new_v4().to_string(),
                idempotency_key: "path-test".into(),
                coordinator_epoch: 1,
                project: JobProject {
                    address: format!("30621:{}:nemo", sender.public_key().to_hex()),
                    home_channel: Uuid::new_v4().to_string(),
                },
                conversation: None,
                repository: JobRepository {
                    canonical: "https://github.com/mysteropodes/nemo".into(),
                    github_issue: None,
                    github_pr: None,
                    github_run: None,
                    base_sha: "a".repeat(40),
                    branch: "codex/path".into(),
                    worktree_id: "path".into(),
                    paths: vec![path.into()],
                    contracts: vec![],
                },
                sender_pubkey: sender.public_key().to_hex(),
                recipient_pubkey: recipient.public_key().to_hex(),
                sponsor: JobSponsor {
                    pubkey: sender.public_key().to_hex(),
                    github_login: "owner".into(),
                },
                expires_at: "2030-01-01T00:00:00Z".into(),
            },
            capability: "rust".into(),
            title: None,
            origin: None,
            summary: "path test".into(),
            acceptance: vec!["contained".into()],
            supersedes_event_id: None,
        }
    }

    #[test]
    fn rejects_git_and_symlink_escape() {
        let root = std::env::temp_dir().join(format!("buzz-paths-{}", Uuid::new_v4()));
        let outside = std::env::temp_dir().join(format!("buzz-outside-{}", Uuid::new_v4()));
        std::fs::create_dir_all(root.join("safe")).expect("root");
        std::fs::create_dir_all(&outside).expect("outside");
        assert!(request_paths_are_contained(&root, &request("safe/new.rs")));
        assert!(!request_paths_are_contained(&root, &request(".git/config")));
        assert!(!request_paths_are_contained(
            &root,
            &request("safe/.GiT/config")
        ));
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&outside, root.join("escape")).expect("symlink");
            assert!(!request_paths_are_contained(&root, &request("escape/file")));
        }
        std::fs::remove_dir_all(root).ok();
        std::fs::remove_dir_all(outside).ok();
    }
}
