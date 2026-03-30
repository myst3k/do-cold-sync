<h1 align="center">do-cold-sync</h1>

<p align="center">
  <strong>Migrate DigitalOcean Spaces to Cold Storage</strong><br>
  Single binary. Streaming transfers. Checkpoint resume. Built-in verification.
</p>

<p align="center">
  <a href="#install">Install</a> &bull;
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="#features">Features</a> &bull;
  <a href="#demo">Demo</a> &bull;
  <a href="#cli-reference">CLI Reference</a> &bull;
  <a href="#license">License</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License">
  <img src="https://img.shields.io/badge/rust-1.75%2B-orange" alt="Rust">
  <img src="https://img.shields.io/badge/platform-linux%20%7C%20macos-green" alt="Platform">
</p>

---

## Demo

<!-- Replace with actual recording -->
<p align="center">
  <img src="docs/assets/demo.gif" alt="DO Cold Sync TUI Demo" width="700">
</p>

> *Interactive TUI: paste your DO API key, select source and destination, sync with live progress.*

## What It Does

Copies objects from a DigitalOcean Spaces bucket to a Spaces Cold Storage bucket. Streams data directly without buffering, verifies every object, and automatically re-transfers anything missing or mismatched.


## Install

### Download Binary

```bash
curl -sL https://github.com/myst3k/do-cold-sync/releases/latest/download/do-cold-sync-linux-amd64 \
  -o /usr/local/bin/do-cold-sync && chmod +x /usr/local/bin/do-cold-sync
```

### Build from Source

```bash
git clone https://github.com/myst3k/do-cold-sync.git
cd do-cold-sync
cargo build --release
# Binary at ./target/release/do-cold-sync
```

Requires Rust 1.75+ and a C++ compiler (for RocksDB).

## Quick Start

### Interactive TUI (Recommended)

```bash
do-cold-sync
```

1. Paste your DigitalOcean API key (Personal Access Token)
2. The tool creates a temporary Spaces key, discovers your buckets, and detects storage tiers
3. Select your **source** (standard) bucket
4. Select your **destination** (cold storage) bucket
5. Adjust concurrency with +/- if needed
6. Press Enter to start syncing
7. Watch live progress, then review the completion report

The temporary Spaces key is automatically cleaned up on exit.

### Headless CLI

```bash
# Using shared credentials (same key for source + dest)
do-cold-sync \
  --key $DO_SPACES_KEY \
  --secret $DO_SPACES_SECRET \
  --source my-standard-space \
  --source-region nyc3 \
  --dest my-cold-space \
  --dest-region nyc3

# With environment variables
export DO_SPACES_KEY=your_key
export DO_SPACES_SECRET=your_secret
do-cold-sync --source my-space --source-region nyc3 --dest my-cold-space --dest-region nyc3
```

### Cross-Provider Sync

Works with any S3-compatible storage. Useful for testing or migrating between providers.

```bash
do-cold-sync \
  --source-endpoint https://nyc3.digitaloceanspaces.com \
  --source-region nyc3 \
  --source my-space \
  --source-key $SRC_KEY --source-secret $SRC_SECRET \
  --dest-endpoint https://s3.us-east-1.wasabisys.com \
  --dest-region us-east-1 \
  --dest my-wasabi-bucket \
  --dest-key $DST_KEY --dest-secret $DST_SECRET
```

## Features

### Streaming Transfers

Objects stream directly from source to destination. Data flows through without buffering the entire object in memory. A 5 GiB object uses ~256 KiB of RAM, not 5 GiB.

### Checkpoint Resume

Every transferred object is recorded in a local RocksDB checkpoint. If the process crashes, gets interrupted, or you simply stop it — it picks up exactly where it left off. No re-transferring completed objects.

```bash
# First run: transfers 5000 of 10000 objects, then crashes
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3

# Second run: skips the 5000 already done, transfers remaining 5000
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3
```

Checkpoint data stored in `~/.do-cold-sync/` by default. Override with `--data-dir`.

### Skip Existing

Before transferring each object, the tool checks if it already exists in the destination with the correct size. Matching objects are skipped. Safe to run repeatedly — fully idempotent.

### Built-in Verification with Auto-Repair

After syncing, every object is verified by checking it exists in the destination with the correct size. Verification runs concurrently for speed. If any objects are missing or have size mismatches, they are **automatically re-transferred** — no manual intervention needed.

### Retry with Backoff

Failed transfers retry up to 3 times with increasing delays (1s, 2s, 3s). Transient network errors, rate limits, and timeouts are handled automatically.

### Circuit Breaker

If 5+ consecutive requests fail with authentication or permission errors, the sync stops immediately with a clear error message instead of burning through hundreds of retries.

### Inventory Export

Export a full manifest of every object and its sync status:

```bash
# CSV export
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 --export csv

# JSON export
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 --export json

# Custom path
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 \
  --export csv --export-path /tmp/inventory.csv
```

**CSV format:**
```csv
# do-cold-sync inventory
# source: my-space
# dest: my-cold-space
# exported: 2026-03-29T20:01:37+00:00
# total: 200
# synced: 95, skipped: 5, failed: 0, verified: 100, missing: 0
key,size,status,timestamp
photos/img001.jpg,1048576,synced,2026-03-29T20:01:30+00:00
photos/img002.jpg,2097152,skipped,2026-03-29T20:01:30+00:00
```

**JSON format:**
```json
{
  "source_bucket": "my-space",
  "dest_bucket": "my-cold-space",
  "summary": {
    "total": 200,
    "synced": 95,
    "skipped": 5,
    "failed": 0,
    "bytes_synced": 1073741824
  },
  "objects": [...]
}
```

In TUI mode, the inventory is automatically saved to `~/.do-cold-sync/sync-inventory.csv`. Press `e` on the completion screen to see the path.

### Concurrent Transfers

Control parallelism with `--concurrency`. The TUI auto-scales based on your system's CPU count. Adjust with +/- on the confirm screen.

```bash
# Conservative (good for rate-limited endpoints)
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 --concurrency 4

# Aggressive (maximize throughput)
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 --concurrency 64
```

### Verify Only

Check if destination matches source without transferring anything:

```bash
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 --verify-only
```

### Prefix Filter

Only sync objects under a specific prefix:

```bash
do-cold-sync --source my-space --dest my-cold-space --source-region nyc3 --prefix "backups/2024/"
```

## Prerequisites

Before using this tool, you need:

1. **A DigitalOcean API key** — Create a Custom Scoped Personal Access Token in the [DO Console](https://cloud.digitalocean.com/account/api/tokens) with these scopes:

   | Scope | Permission | Why |
   |-------|------------|-----|
   | `spaces` | read, update | List buckets, read/write objects |
   | `spaces_key` | create, read, delete | Create and clean up temporary access keys |

2. **A Cold Storage bucket** — Create one in the [DO Console](https://cloud.digitalocean.com/spaces) (cold storage buckets cannot be created via API)
3. **A Droplet or machine** with network access to both buckets

## How It Works

```
                    do-cold-sync
                         |
         +---------------+---------------+
         |                               |
   DigitalOcean API              S3-Compatible API
   (Bearer token)                (SigV4 signing)
         |                               |
   Create temp                    GET objects from
   Spaces key                     standard Spaces
         |                               |
   List buckets                   Stream directly to
   Detect tiers                   Cold Storage (PUT)
         |                               |
   Clean up key                   Verify + auto-repair
   on exit                        every object
```

**Detailed flow:**

1. Create a temporary fullaccess Spaces key via the DO API
2. List all buckets, HEAD each to detect standard vs cold storage
3. List all objects in the source bucket
4. For each object:
   - Check local checkpoint — skip if already done
   - HEAD on destination — skip if exists with matching size
   - GET from source (streaming)
   - PUT to destination (streaming, no memory buffering)
   - Record in checkpoint
   - Retry up to 3x on failure
5. Verify all objects exist in destination with correct sizes (concurrent)
6. Auto-repair any missing or mismatched objects
7. Export inventory
8. Clean up temporary Spaces key

## CLI Reference

```
Usage: do-cold-sync [OPTIONS]

Options:
      --do-api-key <DO_API_KEY>          DigitalOcean API key [env: DO_API_KEY]
      --key <KEY>                        Spaces access key (source + dest) [env: DO_SPACES_KEY]
      --secret <SECRET>                  Spaces secret key [env: DO_SPACES_SECRET]
      --source-endpoint <ENDPOINT>       Source S3 endpoint [env: SOURCE_ENDPOINT]
      --source <SOURCE>                  Source bucket name
      --source-region <REGION>           Source region
      --source-key <KEY>                 Source access key [env: SOURCE_KEY]
      --source-secret <SECRET>           Source secret key [env: SOURCE_SECRET]
      --dest-endpoint <ENDPOINT>         Destination S3 endpoint [env: DEST_ENDPOINT]
      --dest <DEST>                      Destination bucket name
      --dest-region <REGION>             Destination region
      --dest-key <KEY>                   Destination access key [env: DEST_KEY]
      --dest-secret <SECRET>             Destination secret key [env: DEST_SECRET]
      --concurrency <N>                  Concurrent transfers [default: 16]
      --no-tui                           Disable TUI, run headless
      --verify-only                      Verify only (don't transfer)
      --data-dir <DIR>                   Checkpoint directory [default: ~/.do-cold-sync]
      --prefix <PREFIX>                  Source prefix filter
      --export <FORMAT>                  Export inventory (csv or json)
      --export-path <PATH>              Export file path
  -h, --help                             Print help
  -V, --version                          Print version
```

## Performance

| Metric | Value |
|--------|-------|
| Memory per transfer | ~256 KiB (streaming, no buffering) |
| Concurrent transfers | Configurable, auto-scaled by CPU count |
| Checkpoint | RocksDB — survives crashes, instant local lookups |
| Verification | Concurrent HEAD checks + auto-repair |
| Resume | Instant — skips completed objects via local checkpoint |

## Deploying on DigitalOcean

This tool is designed to run on a DO Droplet for maximum throughput (same network as Spaces).

```bash
# On a fresh Droplet
curl -sL https://github.com/myst3k/do-cold-sync/releases/latest/download/do-cold-sync-linux-amd64 \
  -o /usr/local/bin/do-cold-sync && chmod +x /usr/local/bin/do-cold-sync

# Run the TUI
do-cold-sync

# Or headless with env vars
export DO_SPACES_KEY=your_key
export DO_SPACES_SECRET=your_secret
do-cold-sync \
  --source my-standard-space \
  --source-region nyc3 \
  --dest my-cold-space \
  --dest-region nyc3 \
  --concurrency 32 \
  --export csv
```

## Contributing

Contributions are welcome. Please open an issue first to discuss what you'd like to change.

```bash
# Run tests
cargo test

# Lint
cargo clippy -- -W clippy::pedantic

# Format
cargo fmt
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
