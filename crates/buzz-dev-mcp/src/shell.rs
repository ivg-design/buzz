use crate::shim::Shim;
use rmcp::model::{CallToolResult, Content};
use rmcp::ErrorData;
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

const DEFAULT_TIMEOUT_MS: u64 = 120_000;
const MAX_TIMEOUT_MS: u64 = 1_200_000;
const MAX_COMMAND_BYTES: usize = 1_000_000;
const CAPTURE_CAP: usize = 10 * 1024 * 1024;
const MAX_BYTES: usize = 50 * 1024;
const MAX_LINES: usize = 2000;
const TAIL_BYTES: usize = 8 * 1024;
const ARTIFACT_RING_SIZE: usize = 8;
const READ_CHUNK: usize = 16 * 1024;

pub struct SharedState {
    pub cwd: PathBuf,
    pub(crate) protected_paths: crate::paths::ProtectedPathPolicy,
    pub shim: Shim,
    pub session_dir: TempDir,
    /// Session-private home presented to every model-controlled shell.  The
    /// operator's real HOME may contain GitHub CLI state and global Git
    /// credential-helper configuration, so it is never inherited here.
    pub isolated_home: PathBuf,
    pub isolated_git_config: PathBuf,
    operator_rustup_home: Option<PathBuf>,
    #[cfg(target_os = "macos")]
    sandbox_profile: PathBuf,
    pub bootstrap_instructions: String,
    /// The shell resolved at construction: `Ok((path, display_name))` when a shell
    /// is available, `Err(msg)` when none was found. Stored once so both the
    /// bootstrap hint and every `run()` call read the SAME resolution — no drift.
    pub resolved_shell: Result<(PathBuf, String), String>,
    pub artifacts: Mutex<VecDeque<PathBuf>>,
    next_call_id: Mutex<u64>,
}

impl SharedState {
    #[cfg(test)]
    pub fn new(cwd: PathBuf, shim: Shim) -> std::io::Result<Self> {
        Self::new_with_protected_paths(cwd, shim, crate::paths::ProtectedPathPolicy::default())
    }

    pub(crate) fn new_with_protected_paths(
        cwd: PathBuf,
        shim: Shim,
        protected_paths: crate::paths::ProtectedPathPolicy,
    ) -> std::io::Result<Self> {
        Self::new_inner(
            cwd,
            shim,
            std::env::var_os("HOME").map(PathBuf::from),
            protected_paths,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        cwd: PathBuf,
        shim: Shim,
        protected_paths: crate::paths::ProtectedPathPolicy,
    ) -> std::io::Result<Self> {
        Self::new_with_protected_paths(cwd, shim, protected_paths)
    }

    #[cfg(test)]
    fn new_with_operator_home(
        cwd: PathBuf,
        shim: Shim,
        operator_home: Option<PathBuf>,
    ) -> std::io::Result<Self> {
        Self::new_inner(
            cwd,
            shim,
            operator_home,
            crate::paths::ProtectedPathPolicy::default(),
        )
    }

    fn new_inner(
        cwd: PathBuf,
        shim: Shim,
        operator_home: Option<PathBuf>,
        protected_paths: crate::paths::ProtectedPathPolicy,
    ) -> std::io::Result<Self> {
        let cwd = cwd.canonicalize()?;
        let operator_rustup_home = operator_home.as_deref().and_then(|home| {
            home.join(".rustup")
                .canonicalize()
                .ok()
                .filter(|path| path.is_dir())
        });
        let session_dir = tempfile::Builder::new()
            .prefix("buzz-dev-mcp-session-")
            .tempdir()?;
        let isolated_home = session_dir.path().join("home");
        std::fs::create_dir(&isolated_home)?;
        set_owner_only_directory(&isolated_home)?;
        let isolated_git_config = session_dir.path().join("gitconfig");
        std::fs::write(&isolated_git_config, b"")?;
        set_owner_only_file(&isolated_git_config)?;
        // Resolve the shell ONCE using the same PATH the spawn will use.
        // Both the bootstrap dialect hint and every run() call read this result,
        // so they can never disagree. A failed resolution is stored as Err and
        // surfaces as an actionable error on the first tool call.
        let resolved_shell = resolve_bash(&shim.path_env);
        let shell_hint = match &resolved_shell {
            Ok((_, name)) => name.as_str(),
            Err(_) => "bash",
        };
        let bootstrap_instructions = build_bootstrap(&cwd, shell_hint);
        #[cfg(target_os = "macos")]
        let sandbox_profile_contents = macos_sandbox_profile(
            &cwd,
            session_dir.path(),
            operator_home.as_deref(),
            &protected_paths,
            shim.read_roots(),
        )?;
        #[cfg(target_os = "macos")]
        let sandbox_profile = {
            let path = session_dir.path().join("sandbox.sb");
            std::fs::write(&path, sandbox_profile_contents)?;
            set_owner_only_file(&path)?;
            path
        };
        Ok(Self {
            cwd,
            protected_paths,
            shim,
            session_dir,
            isolated_home,
            isolated_git_config,
            operator_rustup_home,
            #[cfg(target_os = "macos")]
            sandbox_profile,
            bootstrap_instructions,
            resolved_shell,
            artifacts: Mutex::new(VecDeque::with_capacity(ARTIFACT_RING_SIZE)),
            next_call_id: Mutex::new(0),
        })
    }

    fn next_id(&self) -> u64 {
        let mut g = match self.next_call_id.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        *g += 1;
        *g
    }
}

fn build_bootstrap(cwd: &Path, shell_hint: &str) -> String {
    let stack = detect_stack(cwd);
    let buzz_hint =
        if std::env::var("BUZZ_RELAY_URL").is_ok() && std::env::var("BUZZ_PRIVATE_KEY").is_ok() {
            "\nBuzz relay configured. Run `buzz --help` to see available commands.\n"
        } else {
            ""
        };
    format!(
        "Working directory: {}\n\
         Detected stack: {}\n\
         Shell: {shell_hint} (set BUZZ_SHELL to override) — write command strings in that shell's syntax.\n\
         Pass `workdir` per call rather than `cd`.\n\
         {buzz_hint}",
        cwd.display(),
        stack,
    )
}

fn detect_stack(cwd: &Path) -> String {
    let markers = [
        ("Cargo.toml", "rust (cargo)"),
        ("package.json", "node"),
        ("go.mod", "go"),
        ("pyproject.toml", "python (pyproject)"),
        ("requirements.txt", "python"),
        ("Gemfile", "ruby"),
        ("pom.xml", "java (maven)"),
        ("build.gradle", "java (gradle)"),
        ("build.gradle.kts", "kotlin (gradle)"),
    ];
    let mut found: Vec<&str> = markers
        .iter()
        .filter(|(f, _)| cwd.join(f).exists())
        .map(|(_, name)| *name)
        .collect();
    if found.is_empty() {
        "unknown".into()
    } else {
        found.sort();
        found.join(", ")
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ShellParams {
    pub command: String,
    /// Checkout-relative directory. Absolute, tilde and parent paths fail.
    #[serde(default)]
    pub workdir: Option<String>,
    /// Defaults to 120000 ms (2 min) if omitted; capped at 1,200,000 ms (20 min).
    /// For long-running commands (git push with hooks, cargo build, test suites), use 300000+.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn effective_timeout_ms(requested: Option<u64>) -> u64 {
    requested.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS)
}

pub async fn run(
    state: &SharedState,
    p: ShellParams,
    ct: CancellationToken,
) -> Result<CallToolResult, ErrorData> {
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (state, p, ct);
        return Ok(CallToolResult::error(vec![Content::text(
            "generic shell is unavailable: equivalent OS-level filesystem isolation is not implemented on this platform",
        )]));
    }
    #[cfg(target_os = "macos")]
    let (state, p, ct) = (state, p, ct);

    if p.command.len() > MAX_COMMAND_BYTES {
        return Err(ErrorData::invalid_params(
            format!("command exceeds {MAX_COMMAND_BYTES} byte limit"),
            None,
        ));
    }
    let timeout_ms = effective_timeout_ms(p.timeout_ms);
    let workdir = crate::paths::resolve_workdir(state, p.workdir.as_deref())
        .map_err(|message| ErrorData::invalid_params(message, None))?;
    let bash = match &state.resolved_shell {
        Ok((path, _)) => path.clone(),
        Err(msg) => return Ok(CallToolResult::error(vec![Content::text(msg.clone())])),
    };
    let shell_arg = shell_flag(&bash);
    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut command = Command::new("/usr/bin/sandbox-exec");
        command
            .arg("-f")
            .arg(&state.sandbox_profile)
            .arg(&bash)
            .arg(shell_arg)
            .arg(&p.command);
        command
    };
    #[cfg(not(target_os = "macos"))]
    let mut cmd = {
        let mut command = Command::new(&bash);
        command.arg(shell_arg).arg(&p.command);
        command
    };
    cmd.current_dir(&workdir);
    cmd.env_clear();
    cmd.env("PATH", &state.shim.path_env);
    // The generic model shell never receives harness signing, bearer, grant,
    // or session-scope inputs, even when this library runs in-process inside
    // buzz-acp (whose own process legitimately retains those values).
    crate::trusted::scrub_async_command_environment(&mut cmd);
    let isolated_config_home = state.isolated_home.join(".config");
    let isolated_gh_config = isolated_config_home.join("gh");
    let isolated_cargo_home = state.isolated_home.join(".cargo");
    let isolated_tmp = state.session_dir.path().join("tmp");
    if let Err(error) = std::fs::create_dir_all(&isolated_tmp)
        .and_then(|()| std::fs::create_dir_all(&isolated_cargo_home))
    {
        return Ok(CallToolResult::error(vec![Content::text(format!(
            "failed to prepare isolated temporary directory: {error}"
        ))]));
    }
    cmd.env("HOME", &state.isolated_home)
        .env("USERPROFILE", &state.isolated_home)
        .env("XDG_CONFIG_HOME", &isolated_config_home)
        .env("GH_CONFIG_DIR", &isolated_gh_config)
        .env("CARGO_HOME", &isolated_cargo_home)
        .env("TMPDIR", &isolated_tmp)
        .env("TMP", &isolated_tmp)
        .env("TEMP", &isolated_tmp)
        .env("GIT_CONFIG_GLOBAL", &state.isolated_git_config)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "")
        .env("SSH_ASKPASS", "")
        .env("GCM_INTERACTIVE", "Never");
    if let Some(rustup_home) = &state.operator_rustup_home {
        cmd.env("RUSTUP_HOME", rustup_home);
    }
    // Explicitly retain only build/runtime hints known not to carry auth.
    // `env_clear` is the security boundary: a future GITHUB_PAT-like alias is
    // denied without relying on an ever-growing secret-name blocklist.
    for name in [
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TERM",
        "NO_COLOR",
        "SDKROOT",
        "DEVELOPER_DIR",
    ] {
        if let Some(value) = std::env::var_os(name) {
            cmd.env(name, value);
        }
    }
    for (k, v) in &state.shim.git_env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    cmd.kill_on_drop(true);
    set_process_group(&mut cmd);
    crate::configure_no_window_async(&mut cmd);

    let started = Instant::now();
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return Ok(CallToolResult::error(vec![Content::text(format!(
                "failed to spawn shell: {e}"
            ))]));
        }
    };

    let pid = child.id();

    // KillGroup ties the spawned bash and all its descendants to a single kill
    // primitive (Unix process group / Windows Job Object). Built from the live
    // child so the Windows job can take the process handle, which only exists
    // after spawn. Held for the whole run; its Drop is the last-resort reaper.
    let mut kill_group = KillGroup::new(&child, pid);

    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();

    let mut stdout_handle = tokio::spawn(async move {
        match stdout_pipe {
            Some(p) => read_capped(p).await,
            None => CapturedStream::default(),
        }
    });
    let mut stderr_handle = tokio::spawn(async move {
        match stderr_pipe {
            Some(p) => read_capped(p).await,
            None => CapturedStream::default(),
        }
    });

    let timeout_dur = Duration::from_millis(timeout_ms);
    let mut notes: Vec<String> = Vec::new();
    let (status, timed_out) = tokio::select! {
        biased;
        _ = ct.cancelled() => {
            // Kill process group, reap child, abort reader tasks.
            kill_group.kill_immediate();
            // Bounded reap so we don't leak zombies. If reap times out,
            // KillGroup drop will kill again as a last resort.
            match tokio::time::timeout(Duration::from_secs(1), child.wait()).await {
                Ok(Ok(_)) => { kill_group.disarm(); } // reaped; disarm guard
                Ok(Err(e)) => {
                    tracing::debug!("cancel: child wait error: {e}");
                    // Leave kill_group armed for drop-kill.
                }
                Err(_) => {
                    tracing::debug!("cancel: child reap timed out; guard will kill on drop");
                }
            }
            stdout_handle.abort();
            stderr_handle.abort();
            return Ok(CallToolResult::error(vec![Content::text("cancelled")]));
        }
        r = tokio::time::timeout(timeout_dur, child.wait()) => match r {
        Ok(Ok(s)) => (Some(s), false),
        Ok(Err(err)) => {
            notes.push(format!("child wait failed: {err}"));
            (None, false)
        }
        Err(_) => {
            // Kill process group — this closes the pipes, causing reads to EOF.
            kill_group.kill_graceful().await;
            // Reap the child so it doesn't become a zombie.
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() >= deadline => {
                        if let Err(e) = child.start_kill() {
                            notes.push(format!("force-kill failed: {e}"));
                        }
                        if let Err(e) = child.wait().await {
                            notes.push(format!("post-kill wait: {e}"));
                        }
                        break;
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Err(err) => {
                        notes.push(format!("try_wait failed: {err}"));
                        break;
                    }
                }
            }
            (None, true)
        }
        }
    };

    if !timed_out {
        kill_group.kill_graceful().await;
    }

    let stdout_cap = match tokio::time::timeout(Duration::from_secs(5), &mut stdout_handle).await {
        Ok(Ok(cap)) => cap,
        _ => {
            stdout_handle.abort();
            notes.push("stdout reader did not complete".into());
            CapturedStream::default()
        }
    };
    let stderr_cap = match tokio::time::timeout(Duration::from_secs(5), &mut stderr_handle).await {
        Ok(Ok(cap)) => cap,
        _ => {
            stderr_handle.abort();
            notes.push("stderr reader did not complete".into());
            CapturedStream::default()
        }
    };

    let duration_ms = started.elapsed().as_millis() as u64;
    let exit_code = status
        .as_ref()
        .and_then(|s| s.code())
        .unwrap_or(if timed_out { 124 } else { -1 });

    let id = state.next_id();
    let (stdout_text, stdout_truncated, stdout_artifact) =
        finalize_stream(state, id, "stdout", stdout_cap, &mut notes);
    let (stderr_text, stderr_truncated, stderr_artifact) =
        finalize_stream(state, id, "stderr", stderr_cap, &mut notes);

    let body = serde_json::json!({
        "exit_code": exit_code,
        "stdout": stdout_text,
        "stderr": stderr_text,
        "timed_out": timed_out,
        "duration_ms": duration_ms,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "stdout_artifact": stdout_artifact,
        "stderr_artifact": stderr_artifact,
        "notes": notes,
    });
    let text = serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into());
    kill_group.disarm();
    Ok(CallToolResult::success(vec![Content::text(text)]))
}

#[cfg(target_os = "macos")]
fn macos_sandbox_profile(
    cwd: &Path,
    session_dir: &Path,
    operator_home: Option<&Path>,
    protected_paths: &crate::paths::ProtectedPathPolicy,
    shim_read_roots: &[PathBuf],
) -> std::io::Result<String> {
    fn literal(path: &Path) -> String {
        path.to_string_lossy()
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
    }

    let session_dir = session_dir.canonicalize()?;
    let operator_home = operator_home
        .ok_or_else(|| std::io::Error::other("operator HOME is required for shell isolation"))?
        .canonicalize()
        .map_err(|error| {
            std::io::Error::other(format!(
                "operator HOME must name an existing canonical directory: {error}"
            ))
        })?;
    if !operator_home.is_dir() {
        return Err(std::io::Error::other(
            "operator HOME must name an existing canonical directory",
        ));
    }
    let git_directories = git_storage_directories(cwd);
    let mut profile = String::from(
        "(version 1)\n\
         (import \"system.sb\")\n\
         (deny default)\n\
         (allow process-fork)\n\
         (allow signal (target same-sandbox))\n\
         (allow network-outbound)\n\
         (deny process-exec (literal \"/usr/bin/security\"))\n\
         (deny process-exec (literal \"/usr/bin/osascript\"))\n\
         (deny process-exec (literal \"/usr/bin/open\"))\n\
         (deny process-exec (literal \"/bin/launchctl\"))\n\
         (deny process-exec (literal \"/usr/libexec/git-core/git-credential-osxkeychain\"))\n\
         (deny appleevent-send)\n\
         (deny mach-priv-task-port)\n\
         (deny process-info*)\n\
         (allow process-info* (target self))\n\
         (deny mach-lookup (global-name \"com.apple.securityd\") (global-name \"com.apple.secd\"))\n",
    );
    let read_only_tool_roots = [
        operator_home.join(".cargo/bin"),
        operator_home.join(".cargo/registry"),
        operator_home.join(".cargo/git"),
        operator_home.join(".rustup"),
        operator_home.join(".nvm/versions"),
        operator_home.join(".local/bin"),
    ];
    let system_executable_roots = [
        PathBuf::from("/bin"),
        PathBuf::from("/sbin"),
        PathBuf::from("/usr/bin"),
        PathBuf::from("/usr/sbin"),
        PathBuf::from("/usr/libexec"),
    ];
    let homebrew_read_roots = [
        PathBuf::from("/opt/homebrew/bin"),
        PathBuf::from("/opt/homebrew/sbin"),
        PathBuf::from("/opt/homebrew/Cellar"),
        PathBuf::from("/opt/homebrew/opt"),
        PathBuf::from("/opt/homebrew/lib"),
        PathBuf::from("/usr/local/bin"),
        PathBuf::from("/usr/local/lib"),
    ];
    let developer_tool_roots = [
        PathBuf::from("/Library/Developer/CommandLineTools"),
        PathBuf::from("/Applications/Xcode.app/Contents/Developer"),
    ];
    let mut readable_roots = vec![cwd.to_path_buf(), session_dir.clone()];
    readable_roots.extend(git_directories.iter().cloned());
    readable_roots.extend(
        read_only_tool_roots
            .iter()
            .filter(|path| path.exists())
            .cloned(),
    );
    readable_roots.extend(shim_read_roots.iter().cloned());
    readable_roots.extend(
        system_executable_roots
            .iter()
            .chain(homebrew_read_roots.iter())
            .chain(developer_tool_roots.iter())
            .filter(|path| path.exists())
            .cloned(),
    );
    let mut writable_roots = vec![cwd.to_path_buf(), session_dir.clone()];
    writable_roots.extend(git_directories.iter().cloned());

    // Seatbelt's default is deny. Add only the exact checkout, session and
    // toolchain roots needed by a developer shell. In particular, nothing
    // grants ambient access to /Users/Shared, /Volumes, another home, or an
    // operator-selected absolute path.
    for root in &readable_roots {
        profile.push_str(&format!(
            "(allow file-read* file-test-existence (literal \"{}\") (subpath \"{}\"))\n",
            literal(root),
            literal(root)
        ));
    }
    for root in &writable_roots {
        profile.push_str(&format!(
            "(allow file-write* (literal \"{}\") (subpath \"{}\"))\n",
            literal(root),
            literal(root)
        ));
    }
    let mut executable_roots = vec![cwd.to_path_buf(), session_dir.clone()];
    executable_roots.extend(
        system_executable_roots
            .iter()
            .chain(read_only_tool_roots.iter())
            .chain(homebrew_read_roots.iter())
            .chain(developer_tool_roots.iter())
            .chain(shim_read_roots.iter())
            .filter(|path| path.exists())
            .cloned(),
    );
    for root in &executable_roots {
        profile.push_str(&format!(
            "(allow process-exec (literal \"{}\") (subpath \"{}\"))\n",
            literal(root),
            literal(root)
        ));
    }
    for selector in [
        Path::new("/var/select/developer_dir"),
        Path::new("/private/var/select/developer_dir"),
    ] {
        profile.push_str(&format!(
            "(allow file-read* file-test-existence (literal \"{}\"))\n",
            literal(selector)
        ));
    }

    // A linked worktree's Git storage normally lives outside `cwd` (and often
    // under the operator home). Permit the storage needed by `git add`, then
    // remove every configuration, pointer, ref and hook authority surface.
    for git_dir in git_directories {
        for name in [
            "config",
            "config.worktree",
            "HEAD",
            "commondir",
            "gitdir",
            "packed-refs",
            "shallow",
            "info/grafts",
            "objects/info/alternates",
        ] {
            profile.push_str(&format!(
                "(deny file-write* (literal \"{}\"))\n",
                literal(&git_dir.join(name))
            ));
        }
        for name in ["refs", "hooks"] {
            profile.push_str(&format!(
                "(deny file-write* (subpath \"{}\"))\n",
                literal(&git_dir.join(name))
            ));
        }
    }
    let dot_git = cwd.join(".git");
    profile.push_str(&format!(
        "(deny file-write* (literal \"{}\"))\n",
        literal(&dot_git)
    ));
    for protected in protected_paths.paths() {
        profile.push_str(&format!(
            "(deny file-read* file-write* file-test-existence (literal \"{}\") (subpath \"{}\"))\n",
            literal(protected),
            literal(protected)
        ));
    }
    Ok(profile)
}

#[cfg(target_os = "macos")]
fn git_storage_directories(cwd: &Path) -> Vec<PathBuf> {
    let dot_git = cwd.join(".git");
    let git_dir = if dot_git.is_dir() {
        dot_git.canonicalize().ok()
    } else {
        std::fs::read_to_string(&dot_git)
            .ok()
            .and_then(|value| {
                value
                    .strip_prefix("gitdir:")
                    .map(str::trim)
                    .map(PathBuf::from)
            })
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    cwd.join(path)
                }
            })
            .and_then(|path| path.canonicalize().ok())
    };
    let Some(git_dir) = git_dir else {
        return Vec::new();
    };
    let common_dir = std::fs::read_to_string(git_dir.join("commondir"))
        .ok()
        .map(|value| PathBuf::from(value.trim()))
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                git_dir.join(path)
            }
        })
        .and_then(|path| path.canonicalize().ok());
    let mut directories = vec![git_dir];
    if let Some(common_dir) = common_dir {
        if !directories.contains(&common_dir) {
            directories.push(common_dir);
        }
    }
    directories
}

#[cfg(unix)]
fn set_owner_only_directory(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_owner_only_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_owner_only_file(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_owner_only_file(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// The flag used to pass a command string to the shell.
///
/// bash/zsh/sh: `-c`
/// cmd.exe:     `/C`
/// powershell/pwsh: `-Command`
///
/// The resolver supports `BUZZ_SHELL=cmd`/`pwsh` (explicit operator overrides
/// resolve without the System32 exclusion, so these shells work). The dispatch
/// here ensures each shell receives the correct flag regardless of which one
/// was resolved.
fn shell_flag(shell: &Path) -> &'static str {
    match shell
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("cmd") => "/C",
        Some("powershell" | "pwsh") => "-Command",
        _ => "-c",
    }
}

/// Extract a short display name from a resolved shell path (e.g. `pwsh.exe` → `"pwsh"`).
fn shell_name_from_path(p: &Path) -> String {
    p.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "bash".to_string())
}

/// Resolve the shell to spawn. On Unix, bash on PATH is correct and was never
/// broken, so the resolver only needs to honor BUZZ_SHELL. The probe logic and
/// System32 exclusion are Windows-only.
///
/// Returns `(resolved_path, display_name)`. The display name is derived from the
/// resolved path so the caller can use it for diagnostics without a second lookup.
#[cfg(not(windows))]
fn resolve_bash(_path_env: &str) -> Result<(PathBuf, String), String> {
    // Honor BUZZ_SHELL on Unix so power users can opt into zsh or another shell.
    if let Some(raw) = std::env::var_os("BUZZ_SHELL") {
        let p = PathBuf::from(&raw);
        // Absolute / rooted path: must exist as a file.
        if p.components().count() > 1 || p.has_root() {
            if p.is_file() {
                let name = shell_name_from_path(&p);
                return Ok((p, name));
            }
            // Non-existent path: fall through to bash.
        } else {
            // Bare command name: scan the process PATH directly.
            let path_var = std::env::var_os("PATH").unwrap_or_default();
            for dir in std::env::split_paths(&path_var) {
                let candidate = dir.join(&p);
                if candidate.is_file() {
                    let name = shell_name_from_path(&candidate);
                    return Ok((candidate, name));
                }
            }
            // Not found: fall through to bash.
        }
    }
    Ok((PathBuf::from("bash"), "bash".to_string()))
}

/// Windows bash resolution. Probe order (first hit wins):
///   1. `BUZZ_SHELL` env override — explicit operator choice, any shell (cmd,
///      PowerShell, bash, etc.). Bare command names are resolved through PATH
///      WITHOUT the System32 exclusion — the operator explicitly chose this shell,
///      and cmd.exe/powershell.exe live in System32 legitimately.
///   2. `GIT_BASH` env override — legacy escape hatch (kept for back-compat).
///   3. `bash.exe` on PATH, excluding System32 so we never resolve WSL's launcher.
///   4. `git.exe` on PATH → its sibling `..\\bin\\bash.exe`. Git for Windows's
///      recommended "Git from the command line" option adds `Git\\cmd` to PATH,
///      not `Git\\bin`, so this is the normal post-install route.
///   5. Standard `ProgramFiles`, `ProgramFiles(x86)`, and `LocalAppData` paths
///      when the child inherited their parent environment.
///   6. Git for Windows's machine then user registry `InstallPath`.
///
/// Returns `(resolved_path, display_name)`. The display name is derived from the
/// resolved path, guaranteeing the dialect hint and the spawned shell agree.
///
/// No bash found -> actionable error pointing at the prerequisite.
#[cfg(windows)]
fn resolve_bash(path_env: &str) -> Result<(PathBuf, String), String> {
    if let Some(raw) = std::env::var_os("BUZZ_SHELL") {
        let p = PathBuf::from(&raw);
        if p.components().count() > 1 || p.has_root() {
            if p.is_file() {
                let name = shell_name_from_path(&p);
                return Ok((p, name));
            }
        } else if let Some(found) = scan_path_for_command(&p, path_env, None) {
            let name = shell_name_from_path(&found);
            return Ok((found, name));
        }
    }

    if let Some(p) = std::env::var_os("GIT_BASH").map(PathBuf::from) {
        if p.is_file() {
            let name = shell_name_from_path(&p);
            return Ok((p, name));
        }
    }

    let system_root = std::env::var_os("SystemRoot").map(PathBuf::from);
    if let Some(p) = scan_path_for_bash(path_env, system_root.as_deref()) {
        return Ok((p, "bash".to_string()));
    }

    if let Some(git) = scan_path_for_command(Path::new("git.exe"), path_env, None) {
        if let Some(bash) = bash_from_git(&git) {
            return Ok((bash, "bash".to_string()));
        }
    }

    if let Some(bash) = git_bash_from_standard_paths() {
        return Ok((bash, "bash".to_string()));
    }

    if let Some(bash) = git_bash_from_registry() {
        return Ok((bash, "bash".to_string()));
    }

    Err(
        "Git for Windows (Git Bash) is required but was not found. Checked \\
         BUZZ_SHELL, GIT_BASH, bash.exe and git.exe on PATH, the standard Git install locations, \\
         and HKLM/HKCU\\\\SOFTWARE\\\\GitForWindows. Git's \"Cmd\" PATH option adds \\
         Git\\\\cmd\\\\git.exe but not Git\\\\bin\\\\bash.exe; Buzz normally derives Git Bash from that git.exe. \\
         Install it from https://git-scm.com/download/win and select \"Git from the command line \\
         and also from 3rd-party software\", then relaunch Buzz. You can also set \\
         BUZZ_SHELL to a shell executable."
            .into(),
    )
}

/// Git for Windows puts `git.exe` in `<install>\\cmd`; the MSYS bash binary is
/// its stable sibling at `<install>\\bin\\bash.exe`.
#[cfg(windows)]
fn bash_from_git(git: &Path) -> Option<PathBuf> {
    let install_root = git.parent()?.parent()?;
    let bash = install_root.join("bin").join("bash.exe");
    bash.is_file().then_some(bash)
}

/// Probe machine and per-user Git for Windows registry keys after the standard
/// install-location fallback has been exhausted.
#[cfg(windows)]
#[allow(unsafe_code)]
fn git_bash_from_registry() -> Option<PathBuf> {
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Foundation::{ERROR_MORE_DATA, ERROR_SUCCESS};
    use windows_sys::Win32::System::Registry::{
        RegCloseKey, RegOpenKeyExW, RegQueryValueExW, HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE,
        KEY_READ,
    };

    const KEY: &str = "SOFTWARE\\GitForWindows";
    const VALUE: &str = "InstallPath";
    let key: Vec<u16> = KEY.encode_utf16().chain(Some(0)).collect();
    let value: Vec<u16> = VALUE.encode_utf16().chain(Some(0)).collect();

    // SAFETY: Inputs are null-terminated UTF-16 for the duration of each call,
    // and every successfully opened handle is closed before trying the next hive.
    unsafe {
        for hive in [HKEY_LOCAL_MACHINE, HKEY_CURRENT_USER] {
            let mut handle = std::ptr::null_mut();
            if RegOpenKeyExW(hive, key.as_ptr(), 0, KEY_READ, &mut handle) != ERROR_SUCCESS {
                continue;
            }

            let mut byte_len = 0;
            let status = RegQueryValueExW(
                handle,
                value.as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut byte_len,
            );
            if (status != ERROR_SUCCESS && status != ERROR_MORE_DATA) || byte_len == 0 {
                RegCloseKey(handle);
                continue;
            }

            let mut data = vec![0u16; (byte_len as usize).div_ceil(2)];
            let status = RegQueryValueExW(
                handle,
                value.as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                data.as_mut_ptr().cast(),
                &mut byte_len,
            );
            RegCloseKey(handle);
            if status != ERROR_SUCCESS {
                continue;
            }

            while data.last() == Some(&0) {
                data.pop();
            }
            let bash = PathBuf::from(OsString::from_wide(&data))
                .join("bin")
                .join("bash.exe");
            if bash.is_file() {
                return Some(bash);
            }
        }
    }

    None
}

#[cfg(windows)]
fn git_bash_from_standard_paths() -> Option<PathBuf> {
    git_bash_from_standard_path_bases([
        std::env::var_os("ProgramFiles").map(PathBuf::from),
        std::env::var_os("ProgramFiles(x86)").map(PathBuf::from),
        std::env::var_os("LocalAppData").map(PathBuf::from),
    ])
}

#[cfg(windows)]
fn git_bash_from_standard_path_bases(
    [program_files, program_files_x86, local_app_data]: [Option<PathBuf>; 3],
) -> Option<PathBuf> {
    [
        program_files.map(|base| base.join("Git")),
        program_files_x86.map(|base| base.join("Git")),
        local_app_data.map(|base| base.join("Programs").join("Git")),
    ]
    .into_iter()
    .flatten()
    .map(|install_root| install_root.join("bin").join("bash.exe"))
    .find(|bash| bash.is_file())
}

/// True if `path` is inside the Windows app-execution-alias directory
/// (`%LOCALAPPDATA%\Microsoft\WindowsApps`). Paths in that directory are WSL
/// stub launchers, not real executables — running them spawns `wsl.exe` /
/// `wslhost.exe` / `conhost.exe` trees rather than the intended shell.
///
/// The check is purely path-structural (component-wise, case-insensitive) so it
/// compiles and is testable on any host.  It matches the path component named
/// `Microsoft` immediately followed by `WindowsApps`, so a sibling directory
/// named `MicrosoftWindowsApps` does not match.
#[cfg(any(windows, test))]
fn is_windows_apps_alias(path: &Path) -> bool {
    let mut components = path.components().peekable();
    while components.peek().is_some() {
        let mut it = components.clone();
        if it.next().is_some_and(|c| {
            c.as_os_str()
                .to_string_lossy()
                .eq_ignore_ascii_case("Microsoft")
        }) && it.next().is_some_and(|c| {
            c.as_os_str()
                .to_string_lossy()
                .eq_ignore_ascii_case("WindowsApps")
        }) {
            return true;
        }
        components.next();
    }
    false
}

/// True if `dir` is `root` or lives under it, comparing path components
/// case-INsensitively. Windows paths are case-insensitive, but `Path::starts_with`
/// compares components case-sensitively on every platform — so a PATH entry spelled
/// `C:\WINDOWS\System32` would slip past a `%SystemRoot%`=`C:\Windows` prefix test
/// and let WSL's `System32\bash.exe` be resolved, reintroducing the `0x8007072c`
/// spawn failure. Component-wise comparison (not a lowercased substring match) avoids
/// a false hit on a sibling like `C:\Windows2`.
#[cfg(windows)]
fn is_under_dir(dir: &Path, root: &Path) -> bool {
    let mut dir_components = dir.components();
    for root_component in root.components() {
        match dir_components.next() {
            Some(d)
                if d.as_os_str()
                    .eq_ignore_ascii_case(root_component.as_os_str()) => {}
            _ => return false,
        }
    }
    true
}

/// Scan the child's PATH for `bash.exe`, skipping the Windows system directory
/// (`system_root`, normally `%SystemRoot%`) and the Windows app-execution-alias
/// directory (`%LOCALAPPDATA%\Microsoft\WindowsApps`) so we never resolve WSL's
/// `System32\bash.exe` or the `WindowsApps\bash.exe` stub launcher.
///
/// Skipping happens during iteration so scanning continues to the next PATH entry
/// when an alias is encountered — alias-first/real-bash-second selects the real one.
/// PATH is parsed with `std::env::split_paths` (never a hand-split on `;`) so it
/// matches exactly what the spawned child would see.
#[cfg(windows)]
fn scan_path_for_bash(path_env: &str, system_root: Option<&Path>) -> Option<PathBuf> {
    for dir in std::env::split_paths(path_env) {
        if let Some(root) = system_root {
            if is_under_dir(&dir, root) {
                continue;
            }
        }
        let candidate = dir.join("bash.exe");
        if candidate.is_file() && !is_windows_apps_alias(&candidate) {
            return Some(candidate);
        }
    }
    None
}

/// Scan `path_env` for `name` (or `name.exe` on Windows if `name` has no
/// extension), skipping any directory under `system_root` to avoid resolving
/// WSL helpers. Returns the first absolute path found.
#[cfg(windows)]
fn scan_path_for_command(
    name: &Path,
    path_env: &str,
    system_root: Option<&Path>,
) -> Option<PathBuf> {
    let needs_exe = name.extension().is_none();
    for dir in std::env::split_paths(path_env) {
        if let Some(root) = system_root {
            if is_under_dir(&dir, root) {
                continue;
            }
        }
        // Try as-is first.
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        // On Windows, also try with .exe suffix when the name has no extension.
        if needs_exe {
            let mut with_exe = dir.join(name);
            with_exe.set_extension("exe");
            if with_exe.is_file() {
                return Some(with_exe);
            }
        }
    }
    None
}

#[cfg(unix)]
fn set_process_group(cmd: &mut Command) {
    cmd.process_group(0);
}

#[cfg(not(unix))]
fn set_process_group(_cmd: &mut Command) {}

/// Kill primitive covering the spawned bash AND every descendant it forks,
/// mirroring the same guarantee across platforms.
///
/// - Unix: the child's process group (set via [`set_process_group`]); kills go
///   to the whole group via `killpg`.
/// - Windows: a Job Object the child is assigned to at construction. A bare
///   `TerminateProcess` on bash leaves MSYS-forked grandchildren (e.g. `sleep`)
///   running — they hold the stdout/stderr pipes open, so the reap blocks until
///   they self-exit. Terminating the job kills the entire tree atomically.
///
/// Held for the whole `run`; `Drop` is the last-resort reaper if an explicit
/// kill was skipped or failed.
#[cfg(unix)]
struct KillGroup(Option<i32>);

#[cfg(unix)]
impl KillGroup {
    fn new(_child: &tokio::process::Child, pid: Option<u32>) -> Self {
        Self(pid.map(|p| p as i32))
    }

    /// Immediate SIGKILL of the process group. Sync; safe to call from Drop.
    /// No grace period — used when the parent task is being torn down.
    fn kill_immediate(&self) {
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;
        if let Some(pid) = self.0 {
            let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }

    /// Graceful SIGTERM → 200ms async sleep → SIGKILL. Async; never blocks the runtime.
    async fn kill_graceful(&self) {
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;
        if let Some(pid) = self.0 {
            let pgid = Pid::from_raw(pid);
            let _ = killpg(pgid, Signal::SIGTERM);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = killpg(pgid, Signal::SIGKILL);
        }
    }

    /// Disarm the Drop-time kill once the child has been reaped explicitly.
    fn disarm(&mut self) {
        self.0 = None;
    }
}

#[cfg(unix)]
impl Drop for KillGroup {
    fn drop(&mut self) {
        self.kill_immediate();
    }
}

#[cfg(windows)]
struct KillGroup {
    job: windows_sys::Win32::Foundation::HANDLE,
}

// SAFETY: `job` is a raw Win32 HANDLE (`*mut c_void`), which is neither `Send`
// nor `Sync` by default. The shell tool's async future holds a `KillGroup`
// across an `.await`, so it must be `Send` to be spawned. A job-object handle
// is a kernel object reference, not thread-affine: `TerminateJobObject` and
// `CloseHandle` are thread-safe, and Rust's `&self`/`&mut self` borrows still
// serialize access to the field. Moving or sharing it across threads is sound.
#[cfg(windows)]
#[allow(unsafe_code)]
unsafe impl Send for KillGroup {}
#[cfg(windows)]
#[allow(unsafe_code)]
unsafe impl Sync for KillGroup {}

#[cfg(windows)]
#[allow(unsafe_code)]
impl KillGroup {
    fn new(child: &tokio::process::Child, _pid: Option<u32>) -> Self {
        use std::mem::{size_of, zeroed};
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
            SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        };

        // SAFETY: each call is a documented Win32 FFI call with arguments that
        // satisfy its contract — a null SECURITY_ATTRIBUTES/name for an
        // anonymous job, a zeroed #[repr(C)] info struct sized by size_of, and
        // the live process handle from `child` (valid while it is running).
        // A null job HANDLE on failure makes every later call a harmless no-op.
        let job = unsafe {
            let job: HANDLE = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if !job.is_null() {
                let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = zeroed();
                // KILL_ON_JOB_CLOSE: when the LAST handle to the job closes,
                // Windows kills every process still in it. This is both the
                // explicit-kill mechanism and the Drop-time safety net — and the
                // reason the job HANDLE must outlive the child (see Drop).
                info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    std::ptr::addr_of!(info).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                );
                if let Some(handle) = child.raw_handle() {
                    AssignProcessToJobObject(job, handle as HANDLE);
                }
            }
            job
        };
        Self { job }
    }

    fn kill_immediate(&self) {
        self.terminate();
    }

    async fn kill_graceful(&self) {
        // A Job Object has no SIGTERM analogue; termination is atomic, so the
        // graceful path is the same single terminate as the immediate path.
        self.terminate();
    }

    fn terminate(&self) {
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        if !self.job.is_null() {
            // SAFETY: `self.job` is a valid job HANDLE for this struct's
            // lifetime; exit code 137 mirrors the SIGKILL (128+9) we report on
            // Unix.
            unsafe {
                TerminateJobObject(self.job, 137);
            }
        }
    }

    /// No-op on Windows: the job is terminated explicitly, and closing the
    /// handle on Drop with no live processes left is harmless. Kept for a
    /// uniform call shape with the Unix guard.
    fn disarm(&mut self) {}
}

#[cfg(windows)]
#[allow(unsafe_code)]
impl Drop for KillGroup {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::CloseHandle;
        if !self.job.is_null() {
            // Closing the last job handle triggers KILL_ON_JOB_CLOSE, killing any
            // process still in the job — the last-resort reaper. The handle is
            // held until here precisely so this fires no earlier than run end.
            // SAFETY: `self.job` is a valid HANDLE created in `new` and closed
            // exactly once here.
            unsafe {
                CloseHandle(self.job);
            }
        }
    }
}

// Fallback for targets that are neither unix nor windows: no process-tree kill
// primitive is wired up, so timeouts rely on the cross-platform start_kill in
// `run`. Keeps the crate compiling everywhere.
#[cfg(not(any(unix, windows)))]
struct KillGroup;

#[cfg(not(any(unix, windows)))]
impl KillGroup {
    fn new(_child: &tokio::process::Child, _pid: Option<u32>) -> Self {
        Self
    }
    fn kill_immediate(&self) {}
    async fn kill_graceful(&self) {}
    fn disarm(&mut self) {}
}

#[derive(Default)]
struct CapturedStream {
    bytes: Vec<u8>,
    /// Total bytes the process produced (may exceed bytes.len() if capped).
    total_bytes: usize,
    capped: bool,
}

async fn read_capped<R: AsyncRead + Unpin>(mut r: R) -> CapturedStream {
    let mut out = CapturedStream::default();
    let mut chunk = vec![0u8; READ_CHUNK];
    loop {
        match r.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                out.total_bytes = out.total_bytes.saturating_add(n);
                if !out.capped {
                    let remaining = CAPTURE_CAP.saturating_sub(out.bytes.len());
                    if remaining == 0 {
                        out.capped = true;
                    } else {
                        let take = n.min(remaining);
                        out.bytes.extend_from_slice(&chunk[..take]);
                        if out.bytes.len() >= CAPTURE_CAP {
                            out.capped = true;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    out
}

fn finalize_stream(
    state: &SharedState,
    call_id: u64,
    label: &str,
    cap: CapturedStream,
    notes: &mut Vec<String>,
) -> (String, bool, Option<String>) {
    let CapturedStream {
        bytes: buf,
        total_bytes,
        capped,
    } = cap;
    let captured_len = buf.len();
    let line_count = buf.iter().filter(|b| **b == b'\n').count();
    let needs_truncate = capped || captured_len > MAX_BYTES || line_count > MAX_LINES;

    if !needs_truncate {
        return (lossy(buf), false, None);
    }

    let artifact_path = crate::shim::artifact_dir(state.session_dir.path())
        .join(format!("{call_id:06}.{label}.txt"));
    let artifact_str = match std::fs::write(&artifact_path, &buf) {
        Ok(()) => {
            rotate_artifacts(state, artifact_path.clone());
            Some(artifact_path.to_string_lossy().into_owned())
        }
        Err(e) => {
            notes.push(format!(
                "{label}: artifact write failed ({}): {e}",
                artifact_path.display()
            ));
            None
        }
    };

    let tail_start = captured_len.saturating_sub(TAIL_BYTES);
    let tail_aligned = align_to_char_boundary(&buf, tail_start);
    let tail = lossy(buf[tail_aligned..].to_vec());

    let cap_note = if capped {
        format!(
            " (capture capped at {} bytes; further output discarded)",
            CAPTURE_CAP
        )
    } else {
        String::new()
    };
    let artifact_suffix = match &artifact_str {
        Some(p) => format!("; captured output (first 10MB) at {p}"),
        None => "; artifact unavailable".into(),
    };
    let notice = format!(
        "[truncated: showing last {} bytes; {} bytes captured / {} lines / {} bytes total{cap_note}{artifact_suffix}]\n",
        tail.len(),
        captured_len,
        line_count,
        total_bytes,
    );
    let mut out = String::with_capacity(notice.len() + tail.len());
    out.push_str(&notice);
    out.push_str(&tail);
    (out, true, artifact_str)
}

fn align_to_char_boundary(buf: &[u8], start: usize) -> usize {
    let mut i = start.min(buf.len());
    while i < buf.len() && (buf[i] & 0xC0) == 0x80 {
        i += 1;
    }
    i
}

fn lossy(buf: Vec<u8>) -> String {
    String::from_utf8(buf).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn rotate_artifacts(state: &SharedState, new_path: PathBuf) {
    let mut ring = match state.artifacts.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    ring.push_back(new_path);
    while ring.len() > ARTIFACT_RING_SIZE {
        if let Some(old) = ring.pop_front() {
            let _ = std::fs::remove_file(old);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shim::Shim;
    use serde_json::Value;
    use tempfile::tempdir;

    fn make_state(cwd: &std::path::Path) -> SharedState {
        let shim = Shim::install().expect("shim install");
        SharedState::new(cwd.to_path_buf(), shim).expect("state new")
    }

    /// Pull the JSON body out of a CallToolResult so tests can assert on fields.
    fn body(r: rmcp::model::CallToolResult) -> Value {
        let text = match r.content.first().and_then(|c| c.as_text()) {
            Some(t) => t.text.clone(),
            None => panic!("no text content"),
        };
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("json ({error}): {text}"))
    }

    #[test]
    fn timeout_bounds_preserve_default_and_cap_requests_at_twenty_minutes() {
        assert_eq!(effective_timeout_ms(None), 120_000);
        assert_eq!(effective_timeout_ms(Some(120_000)), 120_000);
        assert_eq!(effective_timeout_ms(Some(1_200_000)), 1_200_000);
        assert_eq!(effective_timeout_ms(Some(1_200_001)), 1_200_000);
        assert_eq!(effective_timeout_ms(Some(u64::MAX)), 1_200_000);
    }

    #[cfg(not(target_os = "macos"))]
    #[tokio::test(flavor = "current_thread")]
    async fn generic_shell_fails_closed_without_os_isolation() {
        let dir = tempdir().expect("tempdir");
        let state = make_state(dir.path());
        let result = run(
            &state,
            ShellParams {
                command: "echo must-not-run".into(),
                workdir: None,
                timeout_ms: None,
            },
            CancellationToken::new(),
        )
        .await
        .expect("tool result");
        let text = result
            .content
            .first()
            .and_then(|content| content.as_text())
            .map(|text| text.text.as_str())
            .unwrap_or_default();
        assert!(text.contains("unavailable"));
        assert!(!text.contains("must-not-run"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn basic_echo() {
        let dir = tempdir().expect("tempdir");
        let state = make_state(dir.path());
        let r = run(
            &state,
            ShellParams {
                command: "echo hello".into(),
                workdir: None,
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect("ok");
        let v = body(r);
        assert_eq!(
            v["exit_code"],
            0,
            "profile={} body={v}",
            state.sandbox_profile.display()
        );
        assert_eq!(v["stdout"], "hello\n");
        assert_eq!(v["timed_out"], false);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn model_shell_cannot_reach_operator_home_or_global_git_helper() {
        use std::os::unix::fs::PermissionsExt as _;

        let universe = tempdir().expect("universe");
        let cwd = universe.path().join("checkout");
        let operator_home = universe.path().join("operator-home");
        std::fs::create_dir_all(&cwd).expect("checkout");
        std::fs::create_dir_all(&operator_home).expect("operator home");
        let sentinel = format!("credential-{}", uuid::Uuid::new_v4());
        let helper = operator_home.join("credential-helper");
        std::fs::write(
            &helper,
            format!("#!/bin/sh\nprintf 'username=buzz\\npassword={sentinel}\\n'\n"),
        )
        .expect("helper");
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700))
            .expect("helper permissions");
        std::fs::write(
            operator_home.join(".gitconfig"),
            format!("[credential]\n\thelper = {}\n", helper.display()),
        )
        .expect("global config");
        std::fs::write(operator_home.join("sentinel"), &sentinel).expect("sentinel file");

        let shim = Shim::install().expect("shim install");
        let state = SharedState::new_with_operator_home(cwd, shim, Some(operator_home.clone()))
            .expect("state new");
        let command = format!(
            "printf 'protocol=https\\nhost=github.com\\n\\n' | /usr/bin/git credential fill 2>/dev/null || true; \
             /bin/cat '{}' 2>/dev/null || true; \
             '{}' get 2>/dev/null || true; \
             printf poison > '{}' 2>/dev/null || true; \
             printf 'HOME=%s\\n' \"$HOME\"",
            operator_home.join("sentinel").display(),
            helper.display(),
            operator_home.join(".gitconfig").display(),
        );
        let result = run(
            &state,
            ShellParams {
                command,
                workdir: None,
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect("shell result");
        let result = body(result);
        let stdout = result["stdout"].as_str().expect("stdout");
        let stderr = result["stderr"].as_str().expect("stderr");
        assert!(!stdout.contains(&sentinel));
        assert!(!stderr.contains(&sentinel));
        assert!(!stdout.contains(operator_home.to_string_lossy().as_ref()));
        assert!(stdout.contains(state.isolated_home.to_string_lossy().as_ref()));
        assert_eq!(
            std::fs::read_to_string(operator_home.join(".gitconfig")).expect("gitconfig"),
            format!("[credential]\n\thelper = {}\n", helper.display())
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_shell_construction_requires_canonical_operator_home() {
        let checkout = tempdir().expect("checkout");
        let shim = Shim::install().expect("shim");
        let error = SharedState::new_with_operator_home(checkout.path().to_path_buf(), shim, None)
            .err()
            .expect("missing HOME must fail closed");
        assert!(error.to_string().contains("operator HOME is required"));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn model_shell_env_is_allowlisted_and_protected_writes_fail() {
        let checkout = tempdir().expect("checkout");
        let protected = checkout.path().join(".buzz/ledger");
        std::fs::create_dir_all(&protected).expect("protected root");
        let sentinel = protected.join("authority.json");
        std::fs::write(&sentinel, "operator").expect("sentinel");
        let shim = Shim::install().expect("shim");
        let state = SharedState::new_for_test(
            checkout.path().to_path_buf(),
            shim,
            crate::paths::ProtectedPathPolicy::from_paths([protected]),
        )
        .expect("state");
        // The test runner environment commonly lacks this name; setting it
        // verifies env_clear rather than merely observing absence.
        std::env::set_var("GITHUB_PAT_FUTURE_ALIAS", "must-not-cross");
        let result = run(
            &state,
            ShellParams {
                command: "printf '%s' \"${GITHUB_PAT_FUTURE_ALIAS-unset}\"; printf poison > .buzz/ledger/authority.json 2>/dev/null || true".into(),
                workdir: None,
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect("shell result");
        std::env::remove_var("GITHUB_PAT_FUTURE_ALIAS");
        let value = body(result);
        assert_eq!(value["stdout"], "unset");
        assert_eq!(
            std::fs::read_to_string(&sentinel).expect("sentinel"),
            "operator"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn deny_by_default_blocks_shared_volumes_and_external_protected_paths() {
        let checkout = tempdir().expect("checkout");
        let shared = tempfile::Builder::new()
            .prefix("buzz-shell-shared-")
            .tempdir_in("/Users/Shared")
            .expect("shared fixture");
        let ordinary = shared.path().join("ordinary-sentinel");
        let protected_root = shared.path().join("external-authority");
        let protected = protected_root.join("grant.json");
        let shared_write = shared.path().join("model-write");
        std::fs::create_dir(&protected_root).expect("protected fixture");
        std::fs::write(&ordinary, "shared-secret").expect("ordinary sentinel");
        std::fs::write(&protected, "authority-secret").expect("authority sentinel");

        let shim = Shim::install().expect("shim");
        let state = SharedState::new_for_test(
            checkout.path().to_path_buf(),
            shim,
            crate::paths::ProtectedPathPolicy::from_paths([protected_root.clone()]),
        )
        .expect("state");
        let command = format!(
            "/bin/cat '{}' >/dev/null 2>&1; printf 'shared_read=%s\\n' $?; \
             printf poison > '{}' 2>/dev/null; printf 'shared_write=%s\\n' $?; \
             /bin/ls /Volumes >/dev/null 2>&1; printf 'volumes_read=%s\\n' $?; \
             /bin/cat '{}' >/dev/null 2>&1; printf 'protected_read=%s\\n' $?; \
             printf poison > '{}' 2>/dev/null; printf 'protected_write=%s\\n' $?",
            ordinary.display(),
            shared_write.display(),
            protected.display(),
            protected.display(),
        );
        let result = run(
            &state,
            ShellParams {
                command,
                workdir: None,
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect("shell result");
        let result = body(result);
        assert_eq!(result["exit_code"], 0, "{result}");
        let stdout = result["stdout"].as_str().expect("stdout");
        for operation in [
            "shared_read",
            "shared_write",
            "volumes_read",
            "protected_read",
            "protected_write",
        ] {
            assert!(
                !stdout.contains(&format!("{operation}=0")),
                "{operation} escaped the production Seatbelt profile: {result}"
            );
        }
        assert!(!shared_write.exists());
        assert_eq!(
            std::fs::read_to_string(&protected).expect("protected sentinel"),
            "authority-secret"
        );
        let profile = std::fs::read_to_string(&state.sandbox_profile).expect("profile");
        assert!(profile.contains("(deny default)"));
        let protected_literal = protected_root.to_string_lossy();
        assert!(profile.contains(&format!(
            "(deny file-read* file-write* file-test-existence (literal \"{protected_literal}\")"
        )));
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn model_shell_can_edit_and_stage_ordinary_checkout_files() {
        let checkout = tempdir().expect("checkout");
        let init = std::process::Command::new("/usr/bin/git")
            .args(["init", "-q"])
            .current_dir(checkout.path())
            .status()
            .expect("git init");
        assert!(init.success());
        std::fs::write(checkout.path().join("ordinary.txt"), "before\n").expect("file");
        let state = make_state(checkout.path());
        let result = run(
            &state,
            ShellParams {
                command:
                    "printf 'after\\n' > ordinary.txt && git add ordinary.txt && git status --short"
                        .into(),
                workdir: None,
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect("shell result");
        let value = body(result);
        assert_eq!(value["exit_code"], 0, "{value}");
        assert!(
            value["stdout"]
                .as_str()
                .unwrap_or_default()
                .contains("A  ordinary.txt"),
            "{value}"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn timeout_fires() {
        let dir = tempdir().expect("tempdir");
        let state = make_state(dir.path());
        let r = run(
            &state,
            ShellParams {
                // Short sleep, not 999: the kill path must actually terminate
                // the process tree on timeout. If a regression leaves the child
                // (or an MSYS grandchild) orphaned, the test stalls until this
                // brief sleep self-exits — ~5s, not ~16min — so the failure
                // stays visible instead of hiding behind a 999s sleep.
                command: "sleep 5".into(),
                workdir: None,
                timeout_ms: Some(150),
            },
            CancellationToken::new(),
        )
        .await
        .expect("ok");
        let v = body(r);
        assert_eq!(v["timed_out"], true);
        assert_eq!(v["exit_code"], 124);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test(flavor = "current_thread")]
    async fn workdir_is_honored() {
        let dir = tempdir().expect("tempdir");
        let sub = dir.path().join("sub");
        std::fs::create_dir(&sub).expect("mkdir sub");
        let state = make_state(dir.path());
        let r = run(
            &state,
            ShellParams {
                command: "pwd".into(),
                workdir: Some("sub".to_string()),
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect("ok");
        let v = body(r);
        let stdout = v["stdout"].as_str().unwrap_or("");
        // Compare canonicalized paths (macOS /tmp -> /private/tmp, etc.).
        let sub_canon = std::fs::canonicalize(&sub).expect("canon");
        assert!(
            stdout
                .trim()
                .ends_with(sub_canon.to_string_lossy().as_ref())
                || stdout.contains(sub.file_name().unwrap().to_str().unwrap()),
            "stdout: {stdout}"
        );
        let error = run(
            &state,
            ShellParams {
                command: "pwd".into(),
                workdir: Some(sub_canon.display().to_string()),
                timeout_ms: Some(5_000),
            },
            CancellationToken::new(),
        )
        .await
        .expect_err("absolute workdir must be rejected");
        assert!(format!("{error:?}").contains("must be relative"));
    }

    // --- is_windows_apps_alias predicate tests (cross-host) ---

    #[test]
    fn test_windows_apps_alias_detected_typical_path() {
        // Typical WSL alias: %LOCALAPPDATA%\Microsoft\WindowsApps\bash.exe
        // Forward-slash form parses on both Windows and non-Windows hosts.
        assert!(
            is_windows_apps_alias(Path::new(
                "C:/Users/alice/AppData/Local/Microsoft/WindowsApps/bash.exe"
            )),
            "standard WindowsApps path must be detected as an alias"
        );
    }

    #[test]
    fn test_windows_apps_alias_detected_case_insensitive() {
        assert!(
            is_windows_apps_alias(Path::new(
                "C:/Users/alice/AppData/Local/MICROSOFT/WINDOWSAPPS/bash.exe"
            )),
            "WindowsApps detection must be case-insensitive"
        );
    }

    #[test]
    fn test_windows_apps_alias_rejected_real_git_bash() {
        assert!(
            !is_windows_apps_alias(Path::new("C:/Program Files/Git/bin/bash.exe")),
            "real Git Bash must not be detected as a WindowsApps alias"
        );
    }

    #[test]
    fn test_windows_apps_alias_rejected_system32_bash() {
        assert!(
            !is_windows_apps_alias(Path::new("C:/Windows/System32/bash.exe")),
            "System32 bash must not be detected as a WindowsApps alias"
        );
    }

    #[test]
    fn test_windows_apps_alias_rejected_partial_component_match() {
        // A directory named "Microsoft" without a "WindowsApps" sibling must not match.
        assert!(
            !is_windows_apps_alias(Path::new("C:/Microsoft/SomeOtherDir/bash.exe")),
            "path with Microsoft but not WindowsApps must not be detected"
        );
    }

    #[test]
    fn test_windows_apps_alias_rejected_unix_bash() {
        assert!(
            !is_windows_apps_alias(Path::new("/usr/bin/bash")),
            "Unix bash must not be detected as a WindowsApps alias"
        );
    }
}

#[cfg(all(test, windows))]
mod windows_resolver_tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;
    use tempfile::tempdir;

    // Process-global env mutation guard: tests that mutate BUZZ_SHELL,
    // SystemRoot, or GIT_BASH must hold this lock for the duration of the
    // test so parallel test threads cannot race on these env vars.
    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn touch(path: &Path) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, b"").expect("touch");
    }

    #[test]
    fn buzz_shell_override_wins_over_everything() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // BUZZ_SHELL pointing at a real file must be returned without probing
        // the standard Git-for-Windows locations or PATH.
        let dir = tempdir().expect("tempdir");
        let fake_bash = dir.path().join("my-bash.exe");
        touch(&fake_bash);
        // Temporarily set BUZZ_SHELL; clean up after the test.
        env::set_var("BUZZ_SHELL", &fake_bash);
        let result = resolve_bash("");
        env::remove_var("BUZZ_SHELL");
        let (resolved, _name) = result.expect("BUZZ_SHELL override should resolve");
        assert_eq!(resolved, fake_bash);
    }

    #[test]
    fn buzz_shell_override_skipped_when_path_absent() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // If BUZZ_SHELL points at a non-existent path the resolver must fall
        // through rather than returning a dead path.
        env::set_var("BUZZ_SHELL", r"C:\does\not\exist\bash.exe");
        // We cannot easily assert the fallback here without a full Git install,
        // but we can assert the override itself is not returned.
        let result = resolve_bash("");
        env::remove_var("BUZZ_SHELL");
        if let Ok((resolved, _)) = result {
            assert_ne!(
                resolved.to_str().unwrap_or(""),
                r"C:\does\not\exist\bash.exe",
                "non-existent BUZZ_SHELL must not be returned"
            );
        }
        // An Err is also acceptable (no Git installed on test host).
    }

    /// Explicit BUZZ_SHELL bare name resolves through PATH and uses NO System32
    /// exclusion — cmd/pwsh live in System32 legitimately.
    #[test]
    fn buzz_shell_explicit_bare_name_resolves_from_system32() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // Simulate cmd.exe living in a dir that would be excluded by the WSL guard.
        // The explicit BUZZ_SHELL branch must NOT skip System32.
        let sys32 = tempdir().expect("sys32");
        let fake_cmd = sys32.path().join("cmd.exe");
        touch(&fake_cmd);

        // Build a path_env with only sys32 (the WSL exclusion would skip this dir
        // for bash.exe, but must NOT skip it for an explicit BUZZ_SHELL).
        let path_env = env::join_paths([sys32.path().to_path_buf()]).expect("join");
        env::set_var("BUZZ_SHELL", "cmd");
        // Override SystemRoot so the exclusion would trigger on sys32 if applied.
        let old_sysroot = env::var_os("SystemRoot");
        env::set_var("SystemRoot", sys32.path());

        let result = resolve_bash(path_env.to_str().expect("utf8"));

        env::remove_var("BUZZ_SHELL");
        match old_sysroot {
            Some(v) => env::set_var("SystemRoot", v),
            None => env::remove_var("SystemRoot"),
        }

        let (resolved, name) =
            result.expect("explicit BUZZ_SHELL=cmd should resolve even from System32-like dir");
        assert_eq!(resolved, fake_cmd);
        assert_eq!(name, "cmd");
    }

    /// Implicit bash.exe scan still skips System32 (WSL guard intact).
    #[test]
    fn implicit_bash_scan_still_skips_system32() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // Same setup: bash.exe is only in a dir that is under SystemRoot.
        // Without an explicit BUZZ_SHELL, the fallback scan must skip it.
        let sys32 = tempdir().expect("sys32");
        touch(&sys32.path().join("bash.exe"));

        let path_env = env::join_paths([sys32.path().to_path_buf()]).expect("join");
        // No BUZZ_SHELL — trigger the implicit bash fallback scan.
        env::remove_var("BUZZ_SHELL");
        env::remove_var("GIT_BASH");
        // Point SystemRoot at sys32's parent so sys32 is "under SystemRoot".
        let parent = sys32.path().parent().unwrap().to_path_buf();
        let old_sysroot = env::var_os("SystemRoot");
        env::set_var("SystemRoot", &parent);

        let result = resolve_bash(path_env.to_str().expect("utf8"));

        match old_sysroot {
            Some(v) => env::set_var("SystemRoot", v),
            None => env::remove_var("SystemRoot"),
        }

        // Should be Err (no Git installed on test host, and the only bash.exe was
        // under SystemRoot so it was skipped). Ok is also acceptable if git bash
        // happens to be installed at the fixed Program Files path — we just assert
        // the System32 bash was NOT returned.
        if let Ok((resolved, _)) = result {
            assert!(
                !resolved.starts_with(sys32.path()),
                "implicit bash scan must not return the System32 bash: {resolved:?}"
            );
        }
    }

    /// The bootstrap hint (resolved_shell field) and the spawn path are the
    /// same object — both come from SharedState.resolved_shell.
    /// Verify that constructing SharedState with BUZZ_SHELL set produces a
    /// resolved_shell whose display name appears in bootstrap_instructions.
    #[test]
    fn shared_state_bootstrap_hint_matches_resolved_shell() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let dir = tempdir().expect("tempdir");
        let fake_pwsh = dir.path().join("pwsh.exe");
        touch(&fake_pwsh);
        env::set_var("BUZZ_SHELL", &fake_pwsh);

        let shim = crate::shim::Shim::install().expect("shim");
        let state = SharedState::new(dir.path().to_path_buf(), shim).expect("state");

        env::remove_var("BUZZ_SHELL");

        let (_, name) = state.resolved_shell.as_ref().expect("resolved ok");
        assert_eq!(name, "pwsh");
        assert!(
            state.bootstrap_instructions.contains("pwsh"),
            "bootstrap must mention the resolved shell name"
        );
    }

    /// F3: BUZZ_SHELL bare command name (e.g. "pwsh") resolved through PATH.
    /// When pwsh.exe is on PATH, resolve_bash must return it and report "pwsh".
    #[test]
    fn buzz_shell_bare_name_resolved_through_path_when_present() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let dir = tempdir().expect("tempdir");
        let fake_pwsh = dir.path().join("pwsh.exe");
        touch(&fake_pwsh);

        let path_env = env::join_paths([dir.path().to_path_buf()]).expect("join");
        env::set_var("BUZZ_SHELL", "pwsh");
        let result = resolve_bash(path_env.to_str().expect("utf8"));
        env::remove_var("BUZZ_SHELL");

        let (resolved, name) = result.expect("bare BUZZ_SHELL=pwsh should resolve from PATH");
        assert_eq!(resolved, fake_pwsh, "should resolve to pwsh.exe on PATH");
        assert_eq!(name, "pwsh", "display name must match resolved shell");
    }

    /// F3: BUZZ_SHELL bare command name absent from PATH → fall through, do not
    /// report pwsh as the active shell.
    #[test]
    fn buzz_shell_bare_name_absent_from_path_falls_through() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        // Set BUZZ_SHELL to a command that won't be on any real PATH.
        env::set_var("BUZZ_SHELL", "buzz-shell-does-not-exist-xyz");
        let result = resolve_bash("");
        env::remove_var("BUZZ_SHELL");
        if let Ok((resolved, name)) = result {
            assert_ne!(
                resolved.file_name().and_then(|n| n.to_str()).unwrap_or(""),
                "buzz-shell-does-not-exist-xyz.exe",
                "absent BUZZ_SHELL must not be returned as the resolved path"
            );
            assert_ne!(
                name, "buzz-shell-does-not-exist-xyz",
                "absent BUZZ_SHELL must not be reported as the shell name"
            );
        }
        // Err is also acceptable (no Git on test host).
    }

    #[test]
    fn git_cmd_on_path_resolves_sibling_git_bash() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let dir = tempdir().expect("tempdir");
        let git = dir.path().join("Git").join("cmd").join("git.exe");
        let bash = dir.path().join("Git").join("bin").join("bash.exe");
        touch(&git);
        touch(&bash);

        let path_env = env::join_paths([git.parent().expect("cmd dir")]).expect("join");
        let old_buzz_shell = env::var_os("BUZZ_SHELL");
        let old_git_bash = env::var_os("GIT_BASH");
        env::remove_var("BUZZ_SHELL");
        env::remove_var("GIT_BASH");

        let result = resolve_bash(path_env.to_str().expect("utf8"));

        match old_buzz_shell {
            Some(value) => env::set_var("BUZZ_SHELL", value),
            None => env::remove_var("BUZZ_SHELL"),
        }
        match old_git_bash {
            Some(value) => env::set_var("GIT_BASH", value),
            None => env::remove_var("GIT_BASH"),
        }

        assert_eq!(
            result
                .expect("Git cmd PATH entry should resolve its sibling bash")
                .0,
            bash
        );
    }

    #[test]
    fn program_files_x86_git_bash_is_found() {
        let dir = tempdir().expect("tempdir");
        let program_files_x86 = dir.path().join("Program Files (x86)");
        let bash = program_files_x86.join("Git").join("bin").join("bash.exe");
        touch(&bash);

        assert_eq!(
            git_bash_from_standard_path_bases([None, Some(program_files_x86), None]),
            Some(bash)
        );
    }

    #[test]
    fn path_scan_skips_system32_and_returns_absolute() {
        // A bash.exe under %SystemRoot% (where WSL's launcher lives) must be
        // skipped; a bash.exe elsewhere on PATH is returned as an absolute path.
        let sys_root = tempdir().expect("sysroot");
        let real = tempdir().expect("real");
        touch(&sys_root.path().join("System32").join("bash.exe"));
        let real_bash = real.path().join("bash.exe");
        touch(&real_bash);

        let path_env =
            env::join_paths([sys_root.path().join("System32"), real.path().to_path_buf()])
                .expect("join");

        let found = scan_path_for_bash(path_env.to_str().expect("utf8"), Some(sys_root.path()))
            .expect("bash found outside System32");
        assert!(found.is_absolute());
        assert!(!found.starts_with(sys_root.path()));
        assert_eq!(found, real_bash);
    }

    #[test]
    fn path_scan_returns_none_when_only_system32_has_bash() {
        // If the ONLY bash.exe on PATH is under System32, the scan finds nothing.
        let sys_root = tempdir().expect("sysroot");
        touch(&sys_root.path().join("System32").join("bash.exe"));
        let path_env = env::join_paths([sys_root.path().join("System32")]).expect("join");

        let found = scan_path_for_bash(path_env.to_str().expect("utf8"), Some(sys_root.path()));
        assert!(found.is_none());
    }

    #[test]
    fn path_scan_skips_system32_when_path_case_differs_from_root() {
        // Windows paths are case-insensitive; a PATH entry spelled differently from
        // %SystemRoot% (e.g. `...\WINDOWS\System32` vs root `...\Windows`) must STILL
        // be excluded, or WSL's bash.exe leaks through. Build the System32 dir under a
        // genuinely upper-cased sibling component so the exclusion can only pass via a
        // case-insensitive compare, not a literal `starts_with`.
        let base = tempdir().expect("base");
        let root = base.path().join("Windows");
        let upper = base.path().join("WINDOWS");
        let sys32 = upper.join("System32");
        touch(&sys32.join("bash.exe"));

        let path_env = env::join_paths([sys32]).expect("join");
        let found = scan_path_for_bash(path_env.to_str().expect("utf8"), Some(&root));
        assert!(
            found.is_none(),
            "case-divergent System32 must still be excluded"
        );
    }

    /// PATH-only discovery — a bash.exe custom-installed on PATH (not under
    /// the standard Program Files locations) must be found by the runtime
    /// resolver. This verifies the PATH fallback in resolve_bash: a custom
    /// install that lives outside Program Files is still usable as a shell.
    #[test]
    fn path_only_bash_is_found_by_scan() {
        // scan_path_for_bash is the runtime resolver's PATH fallback helper.
        // Verify it returns the bash.
        let real = tempdir().expect("real");
        let real_bash = real.path().join("bash.exe");
        touch(&real_bash);

        let path_env = env::join_paths([real.path().to_path_buf()]).expect("join");
        let sys_root = tempdir().expect("sysroot"); // empty — no System32 here

        let found = scan_path_for_bash(path_env.to_str().expect("utf8"), Some(sys_root.path()))
            .expect("bash on PATH must be found");
        assert_eq!(found, real_bash);
    }

    /// WSL alias rejection — when WindowsApps\bash.exe is first on PATH and a
    /// legitimate Git Bash follows, the scanner must skip the alias and return
    /// the real one (alias-first / real-bash-second ordering).
    #[test]
    fn windows_apps_alias_first_real_bash_second_returns_real() {
        let base = tempdir().expect("base");

        // Simulate %LOCALAPPDATA%\Microsoft\WindowsApps structure.
        let microsoft = base.path().join("Microsoft");
        let windows_apps = microsoft.join("WindowsApps");
        std::fs::create_dir_all(&windows_apps).expect("mkdir WindowsApps");
        let alias_bash = windows_apps.join("bash.exe");
        touch(&alias_bash);

        // Legitimate Git Bash in a separate directory.
        let git_bin = base.path().join("git").join("bin");
        std::fs::create_dir_all(&git_bin).expect("mkdir git/bin");
        let real_bash = git_bin.join("bash.exe");
        touch(&real_bash);

        let path_env = env::join_paths([windows_apps.clone(), git_bin.clone()]).expect("join");
        let sys_root = tempdir().expect("sysroot"); // empty

        let found = scan_path_for_bash(path_env.to_str().expect("utf8"), Some(sys_root.path()))
            .expect("real bash must be found after skipping alias");
        assert_eq!(
            found, real_bash,
            "must skip WindowsApps alias and return real bash"
        );
    }

    /// WSL alias rejection — when only WindowsApps\bash.exe is on PATH (no real
    /// Git Bash installed), the scanner must return None rather than the alias.
    #[test]
    fn windows_apps_alias_only_returns_none() {
        let base = tempdir().expect("base");

        let microsoft = base.path().join("Microsoft");
        let windows_apps = microsoft.join("WindowsApps");
        std::fs::create_dir_all(&windows_apps).expect("mkdir WindowsApps");
        let alias_bash = windows_apps.join("bash.exe");
        touch(&alias_bash);

        let path_env = env::join_paths([windows_apps.clone()]).expect("join");
        let sys_root = tempdir().expect("sysroot"); // empty

        let found = scan_path_for_bash(path_env.to_str().expect("utf8"), Some(sys_root.path()));
        assert!(
            found.is_none(),
            "alias-only PATH must return None, not the WSL launcher"
        );
    }
}
