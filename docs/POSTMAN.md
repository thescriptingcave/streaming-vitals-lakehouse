# Testing the APIs with Postman

Status: 2026-09-16. Step-by-step guide for using the ready-to-import
`postman/healthcare-vitals-lakehouse.postman_collection.json` against the
running local stack. No separate "test project" needed — the stack ships its
own everyday HTTP endpoints (Lambda API, Trino, Superset) plus the Floci AWS
surface.

## 0. What you need

- The stack running: `make up` (+ the Flink job if you also want live vitals
  tables — see GETTING_STARTED §4). Postman can test the L3 API and Superset
  even with Flink stopped.
- Postman (desktop app works best; web app also fine).
- `make seed` run at least once (populates DynamoDB fixtures the L3 API
  reads). Re-run it whenever you `make reset`.

## 1. Import the collection

1. Open Postman → **Import** (top-left) → **Upload Files**.
2. Select `postman/healthcare-vitals-lakehouse.postman_collection.json`.
3. You'll see a collection named **Healthcare Realtime Vitals Lakehouse** with
   four folders. Everything is pre-configured for the local stack.

Recommended: open the collection → **Variable** tab and eyeball the values —
they should match:

| Variable | Value | What it is |
|---|---|---|
| `patients_api` | `http://patients.execute-api.localhost.floci.io:4566` | Floci API GW data plane |
| `trino_base` | `http://127.0.0.1:8083` | Trino coordinator (override port) |
| `trino_user` | `trino` | Trino user, no password |
| `superset_base` | `http://127.0.0.1:18088` | Superset (override port) |
| `floci_endpoint` | `http://127.0.0.1:4566` | raw emulated AWS |
| `patient_id` | `P0001` | the demo cohort id used by requests |
| `aws_access_key/secret` | `test` / `test` | dummy creds Floci accepts |

> DNS note: `patients.execute-api.localhost.floci.io` resolves to 127.0.0.1 on
> macOS/Docker Desktop automatically (Floci's documented hostname scheme). If it
> ever fails, confirm Floci is up: `make status`.

## 2. Seed the data (one-time per clean state)

Open a terminal in the repo:

```bash
make seed     # idempotent; upserts latest_vitals + alerts for P0001–P0003
```

Now the "happy path" latest-vitals requests will return real readings instead
of the business 404.

## 3. The four folders

### 3.1 Patients API (L3) — API Gateway → Lambda → DynamoDB

Click the folder, then each request's **Send**:

| Request | Expected |
|---|---|
| `GET /patients/{id}/alerts` | **200** `{"patient_id":"P0001","alerts":[…]}` |
| `GET /patients/{id}/latest-vitals` | **200** with the seeded reading after `make seed` |
| `POST /patients/{id}/latest-vitals` | **404** "Not Found" — only GET routes exist (deliberate negative test) |
| `GET /patients/{id}` | **404** `{"message":"Not Found"}` unless that route is added |

What you're exercising: request → API Gateway routing → Lambda handler
(`lambdas/L3`) → DynamoDB → JSON response. On Floci all of this is real
request traffic, so failures here are genuine handler bugs, not emulator noise.

### 3.2 Trino REST — SQL over HTTP

1. `POST /v1/statement` → body is the SQL, header `X-Trino-User: trino`.
   This is the exact protocol the `trino` CLI uses.
2. The response columns + a `nextUri` pointing at the next page.
3. Copy that `nextUri` into the collection variable `{{next_uri}}` and run
   `GET {nextUri}` until you see a `data` array containing the answer.

Try a few statements (edit the request body):
```sql
SELECT count(*) FROM iceberg.healthcare.vitals;                 -- event rows
SELECT patient_id, count(*) FROM iceberg.healthcare.vitals GROUP BY 1;
SELECT * FROM iceberg.healthcare.vitals_1m ORDER BY window_start LIMIT 5;
```

### 3.3 Superset API

1. `POST /api/v1/security/login` (body `admin`/`admin`).
   Its **Tests** script automatically stores the returned `access_token` in
   `{{superset_token}}`.
2. `GET /api/v1/dashboard/1` → Vitals Live dashboard metadata. Requires the
   token (sent as `Authorization: Bearer {{superset_token}}`).

### 3.4 AWS via SigV4 (advanced — the Floci "implementation" demo)

1. Open `POST Kinesis.ListStreams`.
2. In the request's **Authorization** tab, choose the **AWS Signature** preset;
   the collection supplies `test`/`test`, region `us-east-1`, service
   `kinesis`.
3. **Send** → Floci returns real `{"StreamNames":["vitals",…]}` JSON. This is
   the wire-level call boto3 makes under the hood — proof that Postman can
   talk to the emulated AWS surface directly.

## 4. Workflow tips

- **Collection run**: run a whole folder in sequence with the *Runner* — great
  for a quick external smoke of the L3 API.
- **Bulk test data**: duplicate `GET /alerts` and edit `{id}` in the URL or via
  the `{{patient_id}}` variable to test P0002/P0003/P0004.
- **Save responses** from the "happy path" calls — useful evidence for showing
  the API works without a browser.
- **Make assertions permanent**: Postman **Tests** tab → add e.g.
  `pm.response.to.have.status(200);` so the collection behaves like a test
  suite in the Runner.

## 5. Troubleshooting

| Symptom | Fix |
|---|---|
| `patients.execute-api...` doesn't reach | Floci down → `make up`; hostname scheme needs the `FLOCI_HOSTNAME` default |
| `latest-vitals` returns the business 404 | run `make seed` (also after `make reset`) |
| Trino 503/connection refused | wrong port — override uses `8083`; base compose uses `8082` (update `trino_base`) |
| Superset 401 on dashboard | run the login request first so the token is stored |
| Kinesis SigV4 403 | keep creds `test`/`test` and region `us-east-1` (emulator doesn't validate, but it does parse the signature shape) |
| Collection variables changed accidentally | re-import is safe; or reset them from the table in §1 |

## 6. What this gives a learner

- Real HTTP endpoints to practice API testing on (no separate service to build).
- Evidence the L3 Lambda → DynamoDB path works over the wire.
- A look at Trino's REST statement protocol and Superset's REST API.
- The SigV4 request that reveals what an AWS SDK call actually *is*.