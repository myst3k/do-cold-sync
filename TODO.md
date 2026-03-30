# do-cold-sync TODO

## High Priority
- [ ] Test with large objects (100+ MiB) to verify multipart upload works
- [ ] Wire multipart upload into sync engine (currently streams single-part only)
- [ ] Test with buckets containing 10K+ objects
- [ ] Performance benchmark vs rclone and s5cmd
- [ ] Redesign TUI with polished layout (current logo needs work)

## Features
- [ ] DO API bucket listing — currently works but needs to scan all regions
- [ ] Auto-detect bucket region from ListBuckets response (already returns BucketRegion)
- [ ] Progress bar during sync in TUI
- [ ] ETA calculation improvement (currently rough estimate)
- [ ] Export inventory from TUI (e key on done screen — needs actual export implementation)
- [ ] Rate limiting awareness for cold storage (450 write/sec, 250 read/sec)

## Infrastructure
- [ ] GitHub Actions CI/CD pipeline
- [ ] Cross-compile for linux-amd64 (DO Droplet target)
- [ ] DO Marketplace app packaging
- [ ] curl/wget install script for releases
- [ ] Static binary build (musl target)

## Known Issues
- [ ] Unused `uuid`, `sha2`, `hex` dependencies — verify if needed or remove
- [ ] API key masking could panic on very short keys (5-8 chars)
- [ ] HOME env var missing causes cryptic path errors
- [ ] Cargo clippy pedantic not yet clean
