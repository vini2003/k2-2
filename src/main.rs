mod client;
mod config;
mod protocol;
mod server;
mod tui;

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use config::AppConfig;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "k2", about = "Blazing fast Minecraft world/schematic mover")]
struct Cli {
    /// Path to YAML config
    #[arg(short, long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run in host mode and serve downloads
    Server {
        /// Server name from config
        #[arg(short, long)]
        name: String,
    },
    /// Run interactive client
    Client {
        /// Server name from config
        #[arg(short, long)]
        server: Option<String>,
        /// Override remote address (host:port)
        #[arg(long)]
        addr: Option<String>,
        /// Override token file path
        #[arg(long)]
        token: Option<PathBuf>,
        /// Directory to write downloads
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Alias for client; reads config and drops into UI
    Ui {
        /// Server name from config
        #[arg(short, long)]
        server: Option<String>,
        /// Override remote address (host:port)
        #[arg(long)]
        addr: Option<String>,
        /// Override token file path
        #[arg(long)]
        token: Option<PathBuf>,
        /// Directory to write downloads
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = AppConfig::load(cli.config.as_deref())?;

    match cli.command {
        Commands::Server { name } => {
            let host = cfg
                .servers
                .get(&name)
                .cloned()
                .ok_or_else(|| anyhow!("server `{name}` missing from config"))?;
            server::run_server(&name, host).await?;
        }
        Commands::Client { server, addr, token, output } => {
            run_client(&cfg, server, addr, token, output).await?;
        }
        Commands::Ui { server, addr, token, output } => {
            run_client(&cfg, server, addr, token, output).await?;
        }
    }

    Ok(())
}

async fn run_client(
    cfg: &AppConfig,
    server: Option<String>,
    addr: Option<String>,
    token: Option<PathBuf>,
    output: Option<PathBuf>,
) -> Result<()> {
    let target_name = server
        .or_else(|| cfg.client.as_ref().and_then(|c| c.default_server.clone()))
        .or_else(|| {
            if cfg.servers.len() == 1 {
                cfg.servers.keys().next().cloned()
            } else {
                None
            }
        });
    let Some(name) = target_name else {
        return Err(anyhow!("no server specified; pass --server or set client.default_server; config has {} servers", cfg.servers.len()));
    };

    let host = cfg
        .servers
        .get(&name)
        .cloned()
        .ok_or_else(|| anyhow!("server `{name}` missing from config"))?;

    let addr = addr.unwrap_or_else(|| host.listen.clone());
    let token = token
        .or_else(|| cfg.client.as_ref().and_then(|c| c.token_file.clone()))
        .unwrap_or(host.token_file.clone());
    let output_dir = output
        .or_else(|| cfg.client.as_ref().and_then(|c| c.output_dir.clone()))
        .unwrap_or_else(|| PathBuf::from("downloads"));

    tui::run_tui(name, addr, token, output_dir, cfg.servers.clone()).await?;
    Ok(())
}
