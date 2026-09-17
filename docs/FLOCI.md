# Floci — a beginner's guide (no AWS experience required)

Status: 2026-09-16. Written for someone who is **new to Floci, new to AWS
emulators**, and wants to actually *understand* how emulation works before
using it. The companion [FOCI_VERIFICATION.md](./FOCI_VERIFICATION.md) records
which services Floci supports; this page teaches you what that means.

---

## Part 0 — the 30-second version

This project simulates Amazon's AWS cloud **entirely on your laptop**, for
free, with no AWS account and no credit card. The tool that does this is a
Docker container called **Floci**.

The trick: your code talks to AWS the exact same way it would talk to the real
cloud — but instead of reaching the internet, you point it at
`http://localhost:4566`. Floci answers those requests and *pretends to be
AWS*. One URL change later, the same code can run against real AWS.

Think of Floci as a **flight simulator**: you practise telemetry, controls,
and calls from your seat (your code), and the simulator behaves like a real
aircraft just enough to teach you, while running in your garage (your laptop).

---

## Part 1 — background, in plain language

Before Floci makes sense, four ideas need to be unpacked. None require prior
AWS experience.

### 1.1 What "AWS" is in this project (a 1-paragraph version)

AWS is a collection of **web services** you call over HTTP. This project uses a
handful of them:

| Service | Plain-English job | Role in this project |
|---|---|---|
| **Kinesis** | a queue of streaming messages ("records") | the producer drops a vitals reading into it every second |
| **S3** | an internet hard drive | the lake: Iceberg tables + raw data live here |
| **DynamoDB** | a fast key→value database | tiny read-models (`latest_vitals`, `alerts`) |
| **Lambda** | runs a small function when something happens | `L1` alert on new alarms, `L3` patients API, etc. |
| **SNS** | publish short messages to subscribers | sends "HR critical!" notifications |
| **API Gateway** | gives your Lambdas an HTTP URL | the patients REST API |
| **EventBridge** | a scheduler ("run X every day at 02:00") | triggers daily maintenance |
| **Firehose** | buffers stream records then saves them to S3 | raw archiving path |
| **IAM** | who is *allowed* to call what (permissions) | recorded but not enforced by Floci |

You don't need to master each one today. The glossary at the end has one-line
meanings you can keep nearby.

### 1.2 "Endpoint" (the one word you'll hear constantly)

Every AWS call is just an HTTP request to a URL. That URL is called the
**endpoint**. Real AWS endpoints look like `https://kinesis.us-east-1.amazonaws.com`.
Floci's endpoint is `http://localhost:4566` for **every** service.

That is the *entire* trick of this project — nothing else changes. Compare:

```bash
# "real" AWS you might see in tutorials (imagine an account + credit card):
aws kinesis list-streams --region us-east-1

# Floci: identical command, one extra flag pointing at the emulator:
aws --endpoint-url http://localhost:4566 kinesis list-streams
```

Your code never writes `kinesis.amazonaws.com`. It writes
`endpoint_url = "http://localhost:4566"` once, and every service route answers
there.

### 1.3 SDK vs CLI (two ways to make the calls)

- **CLI** (`aws ...`) — the command-line tool you type by hand. Great for
  exploring.
- **SDK / boto3** — Python libraries your *code* uses to make the same calls.
  In this repo, `lambdas/common/aws.py` is a thin helper that creates boto3
  clients pointed at `AWS_ENDPOINT_URL` (set in `.env` to `:4566`).

### 1.4 Account, region, credentials (three "metadata" ideas)

Real AWS requires an *account* (billing identity, a 12-digit number) and
*credentials* (secret keys). Every resource lives in a *region* (a data-center
location like `us-east-1`).

**Floci fakes all three so you never sign up:** account = `000000000000`,
region = `us-east-1`, credentials = literally `test` / `test`. The emulator
*checks* that you passed some credentials, but never validates them. In your
head: "any non-empty string works".

---

## Part 2 — make your first emulator calls (hands-on, ~2 minutes)

If the compose stack is up (`make up`), Floci is already running. Open a
terminal and try:

```bash
# 1. Ask Floci "what S3 buckets exist?" — expect an (empty) JSON answer:
aws --endpoint-url http://localhost:4566 s3 ls

# 2. Create a bucket, list it:
aws --endpoint-url http://localhost:4566 s3 mb s3://hello
aws --endpoint-url http://localhost:4566 s3 ls

# 3. Write a file, read it back:
aws --endpoint-url http://localhost:4566 s3 cp --quiet /etc/hosts s3://hello/hosts
aws --endpoint-url http://localhost:4566 s3 cp s3://hello/hosts /tmp/hosts-back
diff /etc/hosts /tmp/hosts-back && echo "round-trip OK"
```

Expected: JSON responses, no errors, and — this is the whole point — the
*commands are the same as they'd be against real AWS.* Later, `make tf-apply`
runs Terraform's own AWS client against this same `:4566` endpoint to create
the streams, tables and Lambdas the project uses.

> Tip: add `--no-sign-request` or set `AWS_ACCESS_KEY_ID=test` to silence
> credential warnings — Floci doesn't care.

---

## Part 3 — how Floci *implements* the emulation

Now the part you asked about: what is Floci actually doing behind the scenes?

### 3.1 A single process pretending to be many services

Floci is **one program** that listens on port 4566. Every AWS service listed
earlier is a *route* the program handles: when a request comes in that looks
like "Kinesis PutRecord", it appends the message to an in-memory queue; when
one looks like "S3 GetObject", it serves the bytes from its local store.

So when your producer streams a vitals reading, this happens:

```
producer.py  --HTTP request-->  Floci's "Kinesis" handler (in memory)
Flink job    --HTTP request-->  Floci's "Kinesis" consumer (reads that memory)
Trino        --HTTP request-->  Floci's "S3" handler (stores/reads lake files)
```

The *wire behaviour* (payload formats, error shapes, pagination) is what's
emulated — that's why the same clients work against real AWS. What's *not*
emulated is the massive distributed machinery behind each service: there is no
real network of servers, no billing, no multi-tenant isolation. Data lives in
the emulator's process, and it is only as durable as your storage mode (§6.3).

### 3.2 …except Lambda and Flink, which are *real*

For two heavy services Floci cheats cleverly: instead of emulating them
internally, it **starts real Docker containers**. Floci holds `/var/run/docker.sock`
(that's why the compose file mounts it), and:

- **Lambda**: your Python function is packaged and run in an actual container;
  a "warm pool" keeps some paused so hot invocations feel instant.
- **Managed Flink**: Floci boots a real `apache/flink` JobManager + TaskManager
  to run the pipeline; Flink's own checkpoints savepoints are real.

Result: for things where the *runtime* matters (does your event handler
actually work?), you get the real thing; for things where only the *API shape*
matters (does your code call the right method?), you get a fast in-memory lookalike.

```
your code / Terraform / boto3            (account 000000000000, region us-east-1)
        │  every call → http://<floci>:4566
        ▼
┌──────────────────── FLOCI ────────────────────┐
│  single process, emulated in memory:          │
│   Kinesis · S3 · DynamoDB · SNS · Firehose    │
│   API GW · EventBridge · Glue · IAM           │
└───────────┬───────────────────────────────────┘
            │ /var/run/docker.sock
            ▼
     real Docker containers
      Lambda (warm pool) · Managed Flink
```

### 3.3 What "build against Floci" means for YOU

Here is the key insight you asked about: **you never write any Floci-specific
code.** There is no "Floci SDK". You write *normal AWS client code* (boto3,
`aws` CLI, Terraform/OpenToFu), and Floci is just a URL you point it at.
"Understanding the implementation" = understanding *this*:

1. Pick a service (say Kinesis), learn its normal AWS API calls.
2. In this repo, the endpoint is already redirected to Floci.
3. Run your code; Floci answers with AWS-shaped responses.
4. When it behaves differently from real AWS, you're reading the *emulator's
   limits*, not AWS docs — §6 lists the ones that matter here.

---

## Part 4 — the services this repo actually leans on

| Service | What we count on | Where |
|---|---|---|
| Kinesis | 24 h retention ⇒ a stopped job can `TRIM_HORIZON`-replay old records | `producer/producer.py`, `flink/sql/job.sql` |
| S3 | path-style buckets for the Iceberg warehouse | Trino, Flink sinks |
| Firehose | record-at-a-time flush locally (else tests wait minutes) | `docker-compose.yaml` `FLOCI_SERVICES_FIREHOSE_*` |
| Lambda (real Docker) | L1–L4 handlers behave like production | `lambdas/`, infra modules |
| DynamoDB + Streams | event-source mapping wakes up Lambda L1 | infra, `lambdas/L1` |
| SNS + API GW v2 | notifications + REST routing (API id pinned by a Floci tag) | infra modules |
| EventBridge Scheduler | cron L4 trigger with a **10 s tick** (not sub-minute!) | `infra/` scheduler |
| Glue catalog | **gap**: can't persist Trino Iceberg commits ⇒ this repo uses **Nessie** as the catalog | `docs/FOCI_VERIFICATION.md` |

---

## Part 5 — how THIS repo is wired to Floci (four concrete places)

Follow the data and you'll see the endpoint appears everywhere:

1. **Producer (`make produce`)** — boto3 reads `AWS_ENDPOINT_URL` and writes
   readings into the `vitals` Kinesis stream at `:4566`.
2. **Infrastructure (`make tf-apply`)** — OpenToFu's hashicorp AWS provider is
   given `endpoints = { s3 = "http://localhost:4566", dynamodb = …
   }` in `infra/providers.tf`, so `plan/apply` creates the buckets, streams,
   tables, IAM roles and the four Lambdas **inside Floci**.
3. **Consumers in Docker** — containers talk to Floci by its compose name:
   `http://floci:4566`. Trino uses it for S3; Flink's Kinesis connector uses
   it as `'aws.endpoint'`; Lambda sidecars reuse it.
4. **Tests** — the integration tests boot their *own* Floci per test run via
   `testcontainers-floci` (`from floci import FlociContainer`), so assertions
   never touch the running stack's state.

Change-of-reality check: if you one day wanted real AWS, you'd (a) point those
endpoints at the real region URLs, and (b) supply real credentials. The code
doesn't change. That portability is the whole reason this project exists.

---

## Part 6 — how Floci is *different* from real AWS (only what matters here)

- **Speed vs scale.** One process on your laptop — great for teaching, never
  for production load. If something OOMs, it's usually resource pressure, not
  Floci logic (we hit this with the Flink TaskManager; see FLINK_OPS.md).
- **Retention/timers.** Kinesis data survives ~24 h; the scheduler ticks every
  10 s. Think "a few seconds to a day", never "instant everywhere".
- **Storage modes.** `memory` = gone on restart (CI); `hybrid` = flushed to
  `./data/floci` every ~5 s (our dev default); `persistent`/`wal` = stricter.
  Restart the container and `hybrid` keeps your "cloud" as it was.
- **Enforcement is off.** IAM records roles/policies but doesn't block calls;
  credentials are never validated. It still teaches you the *shape* of IAM.
- **Deletion is honest-casual.** Things don't silently delete random state,
  but `make reset` wipes the emulated account deliberately.

---

## Part 7 — your learning path (a suggested order)

1. **Today:** read Parts 0–2 and run the hands-on at the end of Part 2.
2. Then `make workshop-run` (docs/WORKSHOP.md) — see the lake you "wrote to"
   being queried, and the TUMBLE/HOP tables proving the HOP overlap.
3. Browse `docs/GETTING_STARTED.md` for the full first-run narrative.
4. When curious about "does Floci support service X?", read
   `docs/FOCI_VERIFICATION.md` — it's a scored capability table from the docs.
5. To go deeper yourself: attach a terminal to Floci and watch every request
   it logs (`docker logs -f vitals-floci`) while you run a `make produce`
   cycle. That request log is the emulator's "implementation" made visible.

---

## Glossary (keep this tab open)

| Word | Meaning |
|---|---|
| AWS | Amazon's cloud: HTTP web services for storage, queues, compute, … |
| Emulator | Software that acts like something else well enough to fool clients |
| Endpoint | The URL your code sends AWS-style requests to (`http://localhost:4566`) |
| SDK / boto3 | Python library that turns your code into AWS HTTP requests |
| CLI (`aws`) | Terminal commands that make the same calls (with `--endpoint-url`) |
| Kinesis | Streaming message queue |
| S3 | Internet hard drive (buckets = folders, objects = files) |
| Iceberg | Open table format = "S3 files that SQL can pretend are a table" |
| Catalog | Registry of which "tables" exist and where (this repo: Nessie) |
| DynamoDB | Fast key→value database |
| Lambda | Function that AWS (Floci) runs when triggered — here in a real container |
| SNS | Simple notification fan-out ("publish to subscribers") |
| API Gateway | Gives Lambdas an HTTP URL to call |
| EventBridge | Scheduler/timer for "run this at …" |
| Firehose | Buffer-then-save stream records to S3 |
| IAM | Permissions system (emulated, not enforced by Floci) |
| IaC / Terraform / OpenToFu | Infrastructure-as-code: declare services in files, apply them |
| Account / region | AWS billing identity `000000000000` / data-centre location `us-east-1` |
| TRIM_HORIZON | "start reading from the oldest retained record" (replay) |

---

## Where this doc sits in the repo

- Pipeline: `docs/DATA_FLOW.md`, `docs/ARCHITECTURE.md`
- Tables: `docs/DATA_MODEL.md`
- Running the Flink job: `docs/FLINK_OPS.md`
- First-run tutorial: `docs/GETTING_STARTED.md`
- Floci capability evidence: `docs/FOCI_VERIFICATION.md`