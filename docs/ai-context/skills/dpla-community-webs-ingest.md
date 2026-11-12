---
name: dpla-community-webs-ingest
description: Run Community Webs ingest. Use when the user says harvest community-webs, run community-webs ingest, export community webs, or process community webs DB.
---

# DPLA Community Webs Ingest

Community Webs has its own scripts, run from `ingest_pipeline_python/` (no `i3.conf` edit per delivery):

```bash
python3 special_cases/community-webs/copy_cw.py <path-to-.db-or-.sqlite3>   # copy the delivery to S3
python3 special_cases/community-webs/launch_cw.py                           # full run on EC2, then asks before S3 sync + partner email
python3 special_cases/community-webs/launch_cw.py --send [--force]          # sync + email later; --force if the safety check blocked it
```

See the Community Webs section of `ingest_pipeline_python/README.md`.
