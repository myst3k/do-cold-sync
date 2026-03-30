use anyhow::{Context, Result};
use rocksdb::{DB, Options};

#[derive(Clone)]
pub struct Checkpoint {
    db: std::sync::Arc<DB>,
}

impl Checkpoint {
    pub fn open(path: &str) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path).context("failed to open checkpoint db")?;
        Ok(Self { db: std::sync::Arc::new(db) })
    }

    pub fn is_done(&self, key: &str) -> Result<bool> {
        Ok(self.db.get(key.as_bytes())?.is_some())
    }

    pub fn mark_done(&self, key: &str, size: u64) -> Result<()> {
        self.db.put(key.as_bytes(), size.to_le_bytes())?;
        Ok(())
    }
}
