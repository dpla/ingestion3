---
name: dpla-hub-info
description: Show key i3.conf config for a hub (provider, harvest.type, harvest.endpoint, schedule, email, setlist). Use when user asks for hub config, harvest type/endpoint, who gets emails, schedule months, or OAI setlist details.
---

# dpla-hub-info

Run the hub-info script to show i3.conf config for a hub:

```bash
source .env
./scripts/status/hub-info.sh <hub>
```

## Related

- Schedule for a month: `./scripts/communication/schedule.sh 2` (February)
- Schedule for a hub: `./scripts/communication/schedule.sh <hub>`
