use std::io::Write;

use anyhow::{Context, Result};
use chrono::Utc;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct InventoryEntry {
    pub key: String,
    pub size: u64,
    pub status: String,
    pub timestamp: String,
}

#[derive(Debug, Default)]
pub struct Inventory {
    entries: std::sync::Mutex<Vec<InventoryEntry>>,
}

impl Inventory {
    pub fn record(&self, key: &str, size: u64, status: &str) {
        self.entries.lock().unwrap().push(InventoryEntry {
            key: key.to_string(),
            size,
            status: status.to_string(),
            timestamp: Utc::now().to_rfc3339(),
        });
    }

    pub fn export_csv(&self, path: &str, source_bucket: &str, dest_bucket: &str) -> Result<()> {
        let entries = self.entries.lock().unwrap();
        let mut f = std::fs::File::create(path).context("failed to create export file")?;

        writeln!(f, "# do-cold-sync inventory")?;
        writeln!(f, "# source: {source_bucket}")?;
        writeln!(f, "# dest: {dest_bucket}")?;
        writeln!(f, "# exported: {}", Utc::now().to_rfc3339())?;
        writeln!(f, "# total: {}", entries.len())?;

        let synced = entries.iter().filter(|e| e.status == "synced").count();
        let skipped = entries.iter().filter(|e| e.status == "skipped").count();
        let failed = entries.iter().filter(|e| e.status == "failed").count();
        let verified = entries.iter().filter(|e| e.status == "verified").count();
        let missing = entries.iter().filter(|e| e.status == "missing").count();

        writeln!(f, "# synced: {synced}, skipped: {skipped}, failed: {failed}, verified: {verified}, missing: {missing}")?;
        writeln!(f, "key,size,status,timestamp")?;

        for entry in entries.iter() {
            let escaped_key = if entry.key.contains(',') || entry.key.contains('"') {
                format!("\"{}\"", entry.key.replace('"', "\"\""))
            } else {
                entry.key.clone()
            };
            writeln!(f, "{},{},{},{}", escaped_key, entry.size, entry.status, entry.timestamp)?;
        }

        Ok(())
    }

    pub fn export_json(&self, path: &str, source_bucket: &str, dest_bucket: &str) -> Result<()> {
        let entries = self.entries.lock().unwrap();

        let synced = entries.iter().filter(|e| e.status == "synced").count();
        let skipped = entries.iter().filter(|e| e.status == "skipped").count();
        let failed = entries.iter().filter(|e| e.status == "failed").count();

        let total_bytes: u64 = entries.iter()
            .filter(|e| e.status == "synced")
            .map(|e| e.size)
            .sum();

        let output = serde_json::json!({
            "source_bucket": source_bucket,
            "dest_bucket": dest_bucket,
            "exported_at": Utc::now().to_rfc3339(),
            "summary": {
                "total": entries.len(),
                "synced": synced,
                "skipped": skipped,
                "failed": failed,
                "bytes_synced": total_bytes,
            },
            "objects": *entries,
        });

        let f = std::fs::File::create(path).context("failed to create export file")?;
        serde_json::to_writer_pretty(f, &output)?;
        Ok(())
    }
}
