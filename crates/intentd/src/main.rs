use clap::Parser;

/// intentd — the Intent backend daemon (bootstrap skeleton).
#[derive(Debug, Parser)]
#[command(name = "intentd", version, about, long_about = None)]
struct Cli {}

fn main() {
    Cli::parse();
    println!("intentd {}", env!("CARGO_PKG_VERSION"));
    println!();
    println!("Usage: intentd [OPTIONS]");
    println!();
    println!("The Intent backend daemon is not yet implemented.");
    println!("This is a bootstrap skeleton; run with --help or --version.");
}
