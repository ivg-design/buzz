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
        "\nIMPORTANT: For ordinary replies in this turn, use `buzz_chat_send`. \
         Trusted reply destination: thread root {event_id}. This is the default; for an explicitly requested different destination, use buzz_chat_thread_create \
         when a new task needs its own thread, and buzz_chat_send with that returned \
         root_event_id when explicitly addressing it. Keep this task and its peer \
         questions and answers together; no human relay is needed."
    ));
}

/// Open a thread for a top-level event under the default placement policy.
pub(crate) fn append_new_thread_instruction(prompt: &mut String, event_id: &str) {
    prompt.push_str(&format!(
        "\nIMPORTANT: This is a new top-level message. For ordinary replies in \
         this turn, use `buzz_chat_send`. Trusted reply destination: new thread root {event_id}. \
         This is the default. Use an explicit destination only when continuing work in another selected thread."
    ));
}

/// Keep a top-level event's response in the current conversation timeline.
pub(crate) fn append_timeline_instruction(prompt: &mut String) {
    prompt.push_str(
        "\nIMPORTANT: This is a top-level message. For ordinary replies \
         in this turn, use `buzz_chat_send`. Its trusted session scope places \
         the response directly in the current timeline. Trusted reply \
         destination: current timeline. For a work assignment or requested new discussion, first create a task thread \
         with buzz_chat_thread_create. Continue its progress, questions and results \
         there using buzz_chat_send with the returned root_event_id. You can ask \
         any enrolled teammate with buzz_peer_ask and receive the reply directly.",
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
