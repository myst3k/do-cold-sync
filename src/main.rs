// Only suppress casts — inherent to S3 APIs mixing u64/i64/f64
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
)]

mod s3;
mod sync;
mod tui;

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(name = "do-cold-sync", version, about = "Sync S3-compatible object storage buckets")]
struct Cli {
    // ── DigitalOcean mode ──
    /// `DigitalOcean` API key (enables DO mode: auto-discovers buckets)
    #[arg(long, env = "DO_API_KEY")]
    do_api_key: Option<String>,

    // ── Shared credentials (DO Spaces mode: same key for source + dest) ──
    /// Spaces access key (used for both source and dest in DO mode)
    #[arg(long, env = "DO_SPACES_KEY")]
    key: Option<String>,

    /// Spaces secret key
    #[arg(long, env = "DO_SPACES_SECRET")]
    secret: Option<String>,

    // ── Source config ──
    /// Source S3 endpoint (e.g., <https://nyc3.digitaloceanspaces.com>)
    #[arg(long, env = "SOURCE_ENDPOINT")]
    source_endpoint: Option<String>,

    /// Source bucket name
    #[arg(long)]
    source: Option<String>,

    /// Source region
    #[arg(long)]
    source_region: Option<String>,

    /// Source access key (overrides --key for source)
    #[arg(long, env = "SOURCE_KEY")]
    source_key: Option<String>,

    /// Source secret key (overrides --secret for source)
    #[arg(long, env = "SOURCE_SECRET")]
    source_secret: Option<String>,

    // ── Dest config ──
    /// Destination S3 endpoint (e.g., <https://nyc3.digitaloceanspaces.com>)
    #[arg(long, env = "DEST_ENDPOINT")]
    dest_endpoint: Option<String>,

    /// Destination bucket name
    #[arg(long)]
    dest: Option<String>,

    /// Destination region
    #[arg(long)]
    dest_region: Option<String>,

    /// Destination access key (overrides --key for dest)
    #[arg(long, env = "DEST_KEY")]
    dest_key: Option<String>,

    /// Destination secret key (overrides --secret for dest)
    #[arg(long, env = "DEST_SECRET")]
    dest_secret: Option<String>,

    // ── Options ──
    /// Concurrent transfers
    #[arg(long, default_value = "16")]
    concurrency: usize,

    /// Disable TUI, run in headless CLI mode
    #[arg(long)]
    no_tui: bool,

    /// Verify only (don't copy, just compare)
    #[arg(long)]
    verify_only: bool,

    /// Checkpoint directory
    #[arg(long, default_value = "~/.do-cold-sync")]
    data_dir: String,

    /// Source prefix filter
    #[arg(long)]
    prefix: Option<String>,

    /// Export inventory after sync (csv or json)
    #[arg(long, value_parser = ["csv", "json"])]
    export: Option<String>,

    /// Export file path (defaults to ./sync-inventory.{csv,json})
    #[arg(long)]
    export_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let data_dir = cli.data_dir.replace('~', &std::env::var("HOME").unwrap_or_default());
    std::fs::create_dir_all(&data_dir)?;

    // Determine if we can run headless
    let has_source = cli.source.is_some()
        && (cli.source_endpoint.is_some() || cli.source_region.is_some());
    let has_dest = cli.dest.is_some()
        && (cli.dest_endpoint.is_some() || cli.dest_region.is_some());
    let has_creds = (cli.key.is_some() && cli.secret.is_some())
        || (cli.source_key.is_some() && cli.source_secret.is_some());

    let headless = cli.no_tui || (has_source && has_dest && has_creds);

    if headless {
        let src_key = cli.source_key.or(cli.key.clone()).unwrap_or_default();
        let src_secret = cli.source_secret.or(cli.secret.clone()).unwrap_or_default();
        let dst_key = cli.dest_key.or(cli.key).unwrap_or_default();
        let dst_secret = cli.dest_secret.or(cli.secret).unwrap_or_default();

        let src_region = cli.source_region.unwrap_or_else(|| "nyc3".to_string());
        let dst_region = cli.dest_region.unwrap_or_else(|| src_region.clone());

        let config = sync::SyncConfig {
            source: sync::EndpointConfig {
                endpoint: cli.source_endpoint.unwrap_or_else(|| {
                    format!("https://{src_region}.digitaloceanspaces.com")
                }),
                bucket: cli.source.unwrap_or_default(),
                access_key: src_key,
                secret_key: src_secret,
                region: src_region,
            },
            dest: sync::EndpointConfig {
                endpoint: cli.dest_endpoint.unwrap_or_else(|| {
                    format!("https://{dst_region}.digitaloceanspaces.com")
                }),
                bucket: cli.dest.unwrap_or_default(),
                access_key: dst_key,
                secret_key: dst_secret,
                region: dst_region,
            },
            concurrency: cli.concurrency,
            verify_only: cli.verify_only,
            prefix: cli.prefix,
            data_dir,
            export_format: cli.export.clone(),
            export_path: cli.export_path.clone(),
        };

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("do_cold_sync=info".parse()?)
            )
            .init();

        sync::run_headless(config).await?;
    } else {
        let tui_config = tui::TuiConfig {
            do_api_key: cli.do_api_key,
            shared_key: cli.key,
            shared_secret: cli.secret,
            concurrency: cli.concurrency,
            data_dir,
        };
        tui::run(tui_config).await?;
    }

    Ok(())
}
