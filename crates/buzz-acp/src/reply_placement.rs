//! Routing policy for agent replies posted back to Buzz.

/// Where an agent should place an ordinary reply to a top-level message.
///
/// Existing thread events always stay in their canonical thread. The policy
/// applies equally to stream channels and direct-message conversations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ReplyPlacement {
    /// Open a thread rooted at the triggering top-level event.
    #[default]
    Thread,
    /// Post beside the triggering event in the current conversation timeline.
    Timeline,
}

impl std::fmt::Display for ReplyPlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Thread => f.write_str("thread"),
            Self::Timeline => f.write_str("timeline"),
        }
    }
}

/// Keep a reply to an existing thread flat at its canonical root.
pub(crate) fn append_thread_instruction(prompt: &mut String, event_id: &str) {
    prompt.push_str(&format!(
        "\nIMPORTANT: For ordinary replies in this turn, use `--reply-to {event_id}` \
         on `buzz messages send` so the conversation stays threaded. \
         If the human explicitly asks for a channel-root, top-level, \
         or broadcast post, send that message without `--reply-to`. \
         If the requested destination is ambiguous, ask before sending."
    ));
}

/// Open a thread for a top-level event under the default placement policy.
pub(crate) fn append_new_thread_instruction(prompt: &mut String, event_id: &str) {
    prompt.push_str(&format!(
        "\nIMPORTANT: This is a new top-level message. For ordinary replies in \
         this turn, use `--reply-to {event_id}` on `buzz messages send` — the \
         triggering message is the thread root. Do NOT reply into any other \
         (older) thread. If the human explicitly asks for a channel-root, \
         top-level, or broadcast post, send that message without `--reply-to`."
    ));
}

/// Keep a top-level event's response in the current conversation timeline.
pub(crate) fn append_timeline_instruction(prompt: &mut String) {
    prompt.push_str(
        "\nIMPORTANT: This is a top-level message. For ordinary replies \
         in this turn, use `buzz messages send` without `--reply-to` so the \
         response appears directly in the current timeline. If the human \
         explicitly asks to start or use a thread, honor that request.",
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliArgs;
    use clap::{CommandFactory, Parser};

    const PRIVATE_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn defaults_to_thread() {
        let args = CliArgs::parse_from(["buzz-acp", "--private-key", PRIVATE_KEY]);
        assert_eq!(args.reply_placement, ReplyPlacement::Thread);
    }

    #[test]
    fn timeline_flag_parses() {
        let args = CliArgs::parse_from([
            "buzz-acp",
            "--private-key",
            PRIVATE_KEY,
            "--reply-placement",
            "timeline",
        ]);
        assert_eq!(args.reply_placement, ReplyPlacement::Timeline);
    }

    #[test]
    fn cli_arg_is_bound_to_documented_env_var() {
        let command = CliArgs::command();
        let argument = command
            .get_arguments()
            .find(|argument| argument.get_id() == "reply_placement")
            .expect("reply-placement argument");
        assert_eq!(
            argument.get_env().and_then(|value| value.to_str()),
            Some("BUZZ_ACP_REPLY_PLACEMENT")
        );
    }
}
