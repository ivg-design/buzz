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
         Its trusted session scope fixes the reply destination. Trusted reply \
         destination: thread root {event_id}. Do not supply or reconstruct it. \
         If the human asks for a different destination, explain that this turn \
         is scope-bound and ask the owner or operator to perform that action."
    ));
}

/// Open a thread for a top-level event under the default placement policy.
pub(crate) fn append_new_thread_instruction(prompt: &mut String, event_id: &str) {
    prompt.push_str(&format!(
        "\nIMPORTANT: This is a new top-level message. For ordinary replies in \
         this turn, use `buzz_chat_send`. Its trusted session scope fixes the \
         destination. Trusted reply destination: new thread root {event_id}. \
         Do not supply or reconstruct it, and do not reply into an older thread."
    ));
}

/// Keep a top-level event's response in the current conversation timeline.
pub(crate) fn append_timeline_instruction(prompt: &mut String) {
    prompt.push_str(
        "\nIMPORTANT: This is a top-level message. For ordinary replies \
         in this turn, use `buzz_chat_send`. Its trusted session scope places \
         the response directly in the current timeline. Trusted reply \
         destination: current timeline. Do not supply or \
         reconstruct a destination. If the human asks for a thread, explain \
         that this turn is scope-bound and ask the owner or operator to start it.",
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
