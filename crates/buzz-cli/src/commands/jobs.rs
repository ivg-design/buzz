//! Structured JSON CLI for the signed Buzz agent-job protocol.

mod dispatch;
mod projection;
mod publish;
mod query;
#[cfg(test)]
mod tests;

pub use dispatch::dispatch;
pub(crate) use query::capabilities;

const JOB_QUERY_BOUND: u32 = 10_000;
const CLI_RESULT_SCHEMA_VERSION: &str = "buzz.cli-result.v1";
const CAPABILITY_DISCOVERY: &str = "buzz.capabilities.discover";

pub(super) struct ParsedInput<T> {
    pub(super) body: T,
    pub(super) raw: String,
}
