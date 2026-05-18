use std::sync::Arc;

fn main() -> gestalt::Result<()> {
    gestalt::runtime::run_agent_provider(Arc::new(
        gestalt_agent_hermes::HermesAgentProvider::default(),
    ))
}
