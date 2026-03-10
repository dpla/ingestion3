---
name: send-email
description: Send an ingest summary email to a hub's configured contacts from i3.conf using the most recent (or specified) mapping output. Use when user asks send/resend a hub ingest summary email.
---

# send-email
```bash
source .env && bash scripts/communication/send-ingest-email.sh $HUB
```

Pass `--yes` to skip the confirmation prompt. Pass a mapping dir path as a second argument to use a specific mapping output instead of the latest.
