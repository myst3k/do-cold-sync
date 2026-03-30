mod checkpoint;
pub mod inventory;

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::s3;
use checkpoint::Checkpoint;

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
}

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub source: EndpointConfig,
    pub dest: EndpointConfig,
    pub concurrency: usize,
    pub verify_only: bool,
    pub prefix: Option<String>,
    pub data_dir: String,
    pub export_format: Option<String>,
    pub export_path: Option<String>,
}

#[derive(Debug, Default)]
pub struct SyncProgress {
    pub total_source_objects: AtomicI64,
    pub objects_synced: AtomicI64,
    pub objects_skipped: AtomicI64,
    pub objects_failed: AtomicI64,
    pub bytes_synced: AtomicU64,
    pub objects_verified: AtomicI64,
    pub objects_missing: AtomicI64,
    pub objects_mismatch: AtomicI64,
    pub active_transfers: AtomicI64,
    pub phase: std::sync::Mutex<String>,
    pub current_key: std::sync::Mutex<String>,
    pub done: AtomicBool,
}

impl SyncProgress {
    pub fn set_phase(&self, phase: &str) {
        *self.phase.lock().unwrap() = phase.to_string();
    }

    pub fn set_current_key(&self, key: &str) {
        *self.current_key.lock().unwrap() = key.to_string();
    }
}

pub async fn run_headless(config: SyncConfig) -> Result<()> {
    let progress = Arc::new(SyncProgress::default());
    run_sync(config, progress).await
}

pub async fn run_sync(config: SyncConfig, progress: Arc<SyncProgress>) -> Result<()> {
    let source = s3::S3Client::new(
        config.source.endpoint.clone(),
        config.source.region.clone(),
        config.source.access_key.clone(),
        config.source.secret_key.clone(),
        "do-cold-sync".to_string(),
    )?;
    let dest = s3::S3Client::new(
        config.dest.endpoint.clone(),
        config.dest.region.clone(),
        config.dest.access_key.clone(),
        config.dest.secret_key.clone(),
        "do-cold-sync".to_string(),
    )?;

    let checkpoint_path = format!("{}/{}_{}", config.data_dir, config.source.bucket, config.dest.bucket);
    let checkpoint = Checkpoint::open(&checkpoint_path)?;
    let inv = Arc::new(inventory::Inventory::default());

    // Ensure dest bucket exists
    progress.set_phase("checking destination");
    let head = dest.head_bucket(&config.dest.bucket).await;
    if head.is_err() {
        info!(bucket = %config.dest.bucket, "creating destination bucket");
        dest.create_bucket(&config.dest.bucket).await
            .context("failed to create destination bucket")?;
    }

    // Phase 1: List source
    progress.set_phase("listing source");
    info!(bucket = %config.source.bucket, "listing source objects");

    let source_objects = list_all(&source, &config.source.bucket, config.prefix.as_deref()).await?;
    let total = source_objects.len();
    progress.total_source_objects.store(total as i64, Ordering::Relaxed);
    info!(total, "source listing complete");

    if config.verify_only {
        progress.set_phase("verifying");
                let vp = VerifyParams {
            source: &source,
            source_bucket: &config.source.bucket,
            dest: &dest,
            dest_bucket: &config.dest.bucket,
            concurrency: config.concurrency,
        };
        verify(&vp, &source_objects, &progress, &inv).await?;
    } else {
        progress.set_phase("syncing");
        sync_objects(&SyncParams {
            source: &source,
            source_bucket: &config.source.bucket,
            dest: &dest,
            dest_bucket: &config.dest.bucket,
            objects: &source_objects,
            checkpoint: &checkpoint,
            concurrency: config.concurrency,
            progress: &progress,
            inv: &inv,
        }).await?;

        progress.set_phase("verifying");
                let vp = VerifyParams {
            source: &source,
            source_bucket: &config.source.bucket,
            dest: &dest,
            dest_bucket: &config.dest.bucket,
            concurrency: config.concurrency,
        };
        verify(&vp, &source_objects, &progress, &inv).await?;
    }

    let synced = progress.objects_synced.load(Ordering::Relaxed);
    let skipped = progress.objects_skipped.load(Ordering::Relaxed);
    let failed = progress.objects_failed.load(Ordering::Relaxed);
    let missing = progress.objects_missing.load(Ordering::Relaxed);
    let bytes = progress.bytes_synced.load(Ordering::Relaxed);

    progress.set_phase("done");
    progress.done.store(true, Ordering::Relaxed);

    info!(
        total, synced, skipped, failed, missing,
        bytes = humansize::format_size(bytes, humansize::BINARY),
        "sync complete"
    );

    // missing objects that were successfully repaired aren't really missing anymore
    let unresolved_missing = missing.saturating_sub(synced);
    if failed > 0 || unresolved_missing > 0 {
        warn!("{failed} failed, {unresolved_missing} still missing in destination");
    }

    // Export inventory
    if let Some(ref format) = config.export_format {
        let default_path = format!("./sync-inventory.{format}");
        let path = config.export_path.as_deref().unwrap_or(&default_path);

        match format.as_str() {
            "csv" => inv.export_csv(path, &config.source.bucket, &config.dest.bucket)?,
            "json" => inv.export_json(path, &config.source.bucket, &config.dest.bucket)?,
            _ => {}
        }
        info!(path = %path, format = %format, "inventory exported");
    }

    Ok(())
}

async fn list_all(
    client: &s3::S3Client,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<Vec<crate::s3::S3Object>> {
    let mut all = Vec::new();
    let mut token: Option<String> = None;
    let mut page = 0u64;

    loop {
        let resp = client
            .list_objects_v2(bucket, prefix, token.as_deref(), Some(1000), None)
            .await?;

        page += 1;
        all.extend(resp.objects);

        if page.is_multiple_of(10) || !resp.is_truncated {
            let total_bytes: u64 = all.iter().map(|o| o.size).sum();
            info!(
                objects = all.len(),
                bytes = %humansize::format_size(total_bytes, humansize::BINARY),
                pages = page,
                "listing progress"
            );
        }

        if resp.is_truncated {
            token = resp.next_continuation_token;
        } else {
            break;
        }
    }
    Ok(all)
}

const CIRCUIT_BREAKER_THRESHOLD: i64 = 5;

struct SyncParams<'a> {
    source: &'a s3::S3Client,
    source_bucket: &'a str,
    dest: &'a s3::S3Client,
    dest_bucket: &'a str,
    objects: &'a [crate::s3::S3Object],
    checkpoint: &'a Checkpoint,
    concurrency: usize,
    progress: &'a Arc<SyncProgress>,
    inv: &'a Arc<inventory::Inventory>,
}

async fn sync_objects(params: &SyncParams<'_>) -> Result<()> {
    let sem = Arc::new(Semaphore::new(params.concurrency));
    let consecutive_auth_errors = Arc::new(AtomicI64::new(0));
    let mut handles = Vec::new();

    for obj in params.objects {
        if consecutive_auth_errors.load(Ordering::Relaxed) >= CIRCUIT_BREAKER_THRESHOLD {
            let failed_so_far = params.progress.objects_failed.load(Ordering::Relaxed);
            error!(
                threshold = CIRCUIT_BREAKER_THRESHOLD,
                failed = failed_so_far,
                "too many consecutive auth errors — stopping. check your credentials and bucket permissions."
            );
            break;
        }

        if params.checkpoint.is_done(&obj.key)? {
            params.progress.objects_skipped.fetch_add(1, Ordering::Relaxed);
            params.inv.record(&obj.key, obj.size, "skipped");
            continue;
        }

        let permit = sem.clone().acquire_owned().await?;
        let task = SyncTask {
            src: params.source.clone(),
            source_bucket: params.source_bucket.to_string(),
            dest_client: params.dest.clone(),
            dest_bucket: params.dest_bucket.to_string(),
            key: obj.key.clone(),
            size: obj.size,
            checkpoint: params.checkpoint.clone(),
            progress: params.progress.clone(),
            inv: params.inv.clone(),
            auth_errors: consecutive_auth_errors.clone(),
        };

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            task.progress.active_transfers.fetch_add(1, Ordering::Relaxed);
            sync_single_object(&task).await;
            task.progress.active_transfers.fetch_sub(1, Ordering::Relaxed);
        }));
    }

    for h in handles {
        h.await.ok();
    }
    Ok(())
}

struct SyncTask {
    src: s3::S3Client,
    source_bucket: String,
    dest_client: s3::S3Client,
    dest_bucket: String,
    key: String,
    size: u64,
    checkpoint: Checkpoint,
    progress: Arc<SyncProgress>,
    inv: Arc<inventory::Inventory>,
    auth_errors: Arc<AtomicI64>,
}

async fn sync_single_object(task: &SyncTask) {
    let SyncTask { src, source_bucket: sb, dest_client, dest_bucket: db, key, size, checkpoint: cp, progress: prog, inv, auth_errors } = task;
    let size = *size;
    prog.set_current_key(key);
    let human_size = humansize::format_size(size, humansize::BINARY);

    if let Ok(h) = dest_client.head_object(db, key).await {
        if h.exists && h.content_length == size {
            cp.mark_done(key, size).ok();
            prog.objects_skipped.fetch_add(1, Ordering::Relaxed);
            inv.record(key, size, "skipped");
            let done = prog.objects_skipped.load(Ordering::Relaxed)
                + prog.objects_synced.load(Ordering::Relaxed);
            let total = prog.total_source_objects.load(Ordering::Relaxed);
            info!(key = %key, size = %human_size, "[{done}/{total}] skipped (exists)");
            return;
        }
    }

    let start = std::time::Instant::now();
    let mut last_err = None;
    for attempt in 1..=3 {
        match transfer(src, sb, dest_client, db, key, size).await {
            Ok(()) => {
                let elapsed = start.elapsed();
                let speed = if elapsed.as_secs_f64() > 0.0 {
                    humansize::format_size(
                        (size as f64 / elapsed.as_secs_f64()) as u64,
                        humansize::BINARY,
                    )
                } else {
                    "∞".to_string()
                };
                auth_errors.store(0, Ordering::Relaxed);
                cp.mark_done(key, size).ok();
                prog.objects_synced.fetch_add(1, Ordering::Relaxed);
                prog.bytes_synced.fetch_add(size, Ordering::Relaxed);
                prog.set_current_key(key);
                inv.record(key, size, "synced");
                let done = prog.objects_skipped.load(Ordering::Relaxed)
                    + prog.objects_synced.load(Ordering::Relaxed);
                let total = prog.total_source_objects.load(Ordering::Relaxed);
                let total_bytes = humansize::format_size(
                    prog.bytes_synced.load(Ordering::Relaxed),
                    humansize::BINARY,
                );
                info!(
                    key = %key,
                    size = %human_size,
                    elapsed = ?elapsed,
                    speed = %format!("{speed}/s"),
                    "[{done}/{total}] synced ({total_bytes} total)"
                );
                return;
            }
            Err(e) => {
                if attempt < 3 {
                    warn!(key = %key, attempt, error = %e, "retrying");
                    tokio::time::sleep(std::time::Duration::from_secs(attempt as u64)).await;
                }
                last_err = Some(e);
            }
        }
    }
    if let Some(ref e) = last_err {
        let err_str = e.to_string();
        if err_str.contains("403") || err_str.contains("401")
            || err_str.contains("301") || err_str.contains("AccessDenied")
            || err_str.contains("Forbidden") || err_str.contains("Moved")
        {
            auth_errors.fetch_add(1, Ordering::Relaxed);
        }
        error!(key = %key, error = %err_str, "failed after 3 attempts");
    }
    prog.objects_failed.fetch_add(1, Ordering::Relaxed);
    inv.record(key, size, "failed");
}

async fn transfer(
    source: &s3::S3Client,
    source_bucket: &str,
    dest: &s3::S3Client,
    dest_bucket: &str,
    key: &str,
    size: u64,
) -> Result<()> {
    let resp = source.get_object(source_bucket, key, None).await?;

    // Stream directly from source to dest
    let stream = resp.body.bytes_stream();
    let body = reqwest::Body::wrap_stream(stream);

    dest.put_object(dest_bucket, key, body, size).await?;
    Ok(())
}

struct VerifyParams<'a> {
    source: &'a s3::S3Client,
    source_bucket: &'a str,
    dest: &'a s3::S3Client,
    dest_bucket: &'a str,
    concurrency: usize,
}

async fn verify(
    params: &VerifyParams<'_>,
    source_objects: &[crate::s3::S3Object],
    progress: &Arc<SyncProgress>,
    inv: &Arc<inventory::Inventory>,
) -> Result<()> {
    let total = source_objects.len();
    info!(total, "starting verification");

    let sem = Arc::new(Semaphore::new(params.concurrency));
    let needs_repair: Arc<std::sync::Mutex<Vec<crate::s3::S3Object>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for obj in source_objects {
        let permit = sem.clone().acquire_owned().await?;
        let dest_client = params.dest.clone();
        let db = params.dest_bucket.to_string();
        let key = obj.key.clone();
        let size = obj.size;
        let prog = progress.clone();
        let inv = inv.clone();
        let repair_list = needs_repair.clone();
        let obj_clone = obj.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;

            let result = dest_client.head_object(&db, &key).await;
            match result {
                Ok(head) if head.exists && head.content_length == size => {
                    prog.objects_verified.fetch_add(1, Ordering::Relaxed);
                    inv.record(&key, size, "verified");
                }
                Ok(head) if head.exists => {
                    warn!(
                        key = %key,
                        source_size = size,
                        dest_size = head.content_length,
                        "SIZE MISMATCH"
                    );
                    prog.objects_mismatch.fetch_add(1, Ordering::Relaxed);
                    inv.record(&key, size, "mismatch");
                    repair_list.lock().unwrap().push(obj_clone);
                }
                _ => {
                    warn!(key = %key, "MISSING in destination");
                    prog.objects_missing.fetch_add(1, Ordering::Relaxed);
                    inv.record(&key, size, "missing");
                    repair_list.lock().unwrap().push(obj_clone);
                }
            }

            let done = prog.objects_verified.load(Ordering::Relaxed)
                + prog.objects_missing.load(Ordering::Relaxed)
                + prog.objects_mismatch.load(Ordering::Relaxed);
            if done % 100 == 0 || done as usize == total {
                let verified = prog.objects_verified.load(Ordering::Relaxed);
                let missing = prog.objects_missing.load(Ordering::Relaxed);
                let mismatch = prog.objects_mismatch.load(Ordering::Relaxed);
                info!("[{done}/{total}] verified={verified} missing={missing} mismatch={mismatch}");
            }
        }));
    }

    for h in handles {
        h.await.ok();
    }

    let verified = progress.objects_verified.load(Ordering::Relaxed);
    let missing = progress.objects_missing.load(Ordering::Relaxed);
    let mismatch = progress.objects_mismatch.load(Ordering::Relaxed);
    info!("verification complete: {verified} ok, {missing} missing, {mismatch} mismatch");

    // Auto-repair missing/mismatched objects
    let to_repair = needs_repair.lock().unwrap().clone();
    if !to_repair.is_empty() {
        repair_objects(params, &to_repair, progress, inv).await?;
    }

    Ok(())
}

async fn repair_objects(
    params: &VerifyParams<'_>,
    objects: &[crate::s3::S3Object],
    progress: &Arc<SyncProgress>,
    inv: &Arc<inventory::Inventory>,
) -> Result<()> {
    info!(count = objects.len(), "repairing missing/mismatched objects");
    progress.set_phase("repairing");

    let sem = Arc::new(Semaphore::new(params.concurrency));
    let mut handles = Vec::new();

    for obj in objects {
        let permit = sem.clone().acquire_owned().await?;
        let src = params.source.clone();
        let dest_client = params.dest.clone();
        let sb = params.source_bucket.to_string();
        let db = params.dest_bucket.to_string();
        let key = obj.key.clone();
        let size = obj.size;
        let prog = progress.clone();
        let inv = inv.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            match transfer(&src, &sb, &dest_client, &db, &key, size).await {
                Ok(()) => {
                    prog.objects_synced.fetch_add(1, Ordering::Relaxed);
                    prog.bytes_synced.fetch_add(size, Ordering::Relaxed);
                    inv.record(&key, size, "repaired");
                    info!(key = %key, "repaired");
                }
                Err(e) => {
                    error!(key = %key, error = %e, "repair failed");
                    prog.objects_failed.fetch_add(1, Ordering::Relaxed);
                    inv.record(&key, size, "repair_failed");
                }
            }
        }));
    }

    for h in handles {
        h.await.ok();
    }

    info!(repaired = objects.len(), "repair pass complete");
    Ok(())
}
