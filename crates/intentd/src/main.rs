use std::sync::Arc;

use clap::Parser;

/// intentd — the Intent backend daemon (bootstrap skeleton).
#[derive(Debug, Parser)]
#[command(name = "intentd", version, about, long_about = None)]
struct Cli {}

fn main() {
    Cli::parse();

    // Composition root: the binary is the only place that wires concrete
    // implementations together (§3.2 rule 5). Stub only — nothing is started.
    let services = Arc::new(intent_services::Services);
    let _acp = intent_acp::AcpClient::new(services.clone());

    println!("intentd {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Usage: intentd [OPTIONS]");
    println!();
    println!("The Intent backend daemon is not yet implemented.");
    println!("This is a bootstrap skeleton; run with --help or --version.");
}
