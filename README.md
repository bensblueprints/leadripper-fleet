# LeadRipper Fleet

Control server for a fleet of LeadRipper desktop worker nodes.

## Endpoints

### Worker (requires `x-license-key` + `x-machine-id` headers)
- `POST /api/fleet/heartbeat` — register / check in
- `POST /api/fleet/pull-job` — claim next queued job
- `POST /api/fleet/job-result` — return leads, mark job done

### Admin (requires `x-admin-token` header)
- `GET /api/admin/stats`
- `GET /api/admin/nodes`
- `GET /api/admin/jobs`
- `POST /api/admin/dispatch`
- `POST /api/admin/jobs/:id/cancel`
- `POST /api/admin/nodes/:id/{pause|resume|kill}`
- `PATCH /api/admin/nodes/:id` — update label / cpu_cap / ram_cap

### Dashboard
- `GET /` — HTML dashboard (token entered in UI)

## Environment

- `ADMIN_TOKEN` — required, admin dashboard auth
- `LICENSE_HASHES` — comma-separated SHA-256 hashes of valid license keys; empty = open mode (dev only)
- `PORT` — default 3000
- `DATA_DIR` — default `/data`, where SQLite lives

## Deploy

Coolify → GitHub → auto-deploy. Attach a volume to `/data`.
