# Citi Bike - Feature Store Demo

Predicts how many bikes will leave each Citi Bike station in the next hour, using a
feature store to keep training and serving consistent. The same code runs on a laptop
(Docker Compose) and on AWS; only the Feast profile and a few environment variables change.

| Layer | Local | AWS |
|---|---|---|
| Feature registry & serving | Feast 0.66 | Feast 0.66 |
| Offline store | Parquet + DuckDB | S3 + Glue + Athena |
| Online store | Redis | DynamoDB |
| Batch and stream compute | Spark 3.5.8 (local mode) | EMR Serverless 7.14 (Spark 3.5.8) |
| Event stream | Redpanda | MSK Serverless (IAM auth) |
| Pollers, API, dashboard | containers | ECS Fargate behind an ALB |

## How it works

```
 Citi Bike trip history (monthly CSV) ──► Spark batch ──► offline store ──► Feast point-in-time join ──► LightGBM
                                              │                                    ▲
                                    same transforms.py                             │ materialize
                                              │                                    │
 trip replayer ──► Kafka ─┐                   ▼                                    │
 GBFS station status ─────┼──► Kafka ──► Spark Structured Streaming ──► online store ──► FastAPI ──► dashboard
 Open-Meteo weather ──────┘                                              (Redis / DynamoDB)
```

**Features** (`feature_repo/definitions.py`):

| Feature view | Computed by | Entity | TTL | Features |
|---|---|---|---|---|
| `station_recent_activity` | Spark, batch **and** streaming, same code | station | 30 min | departures / arrivals in the last 15 min and hour |
| `station_hourly_profile` | Spark batch | station × hour of week | 90 days | mean departures / arrivals for this hour over the last 4 weeks |
| `weather_hourly` | backfill + stream | region | 3 h | temperature, precipitation, wind |
| `station_live_status` | stream (GBFS) | station | 10 min | bikes, e-bikes and docks available (not a model input yet: see below) |
| `profile_signals` | on-demand, inside Feast | – | – | typical net flow for this hour |

`demand_forecast_v1` is the model's feature service; `station_live_v1` feeds the dashboard.

**Things worth looking at**

- `src/citibike_fs/spark/transforms.py`: `station_activity` is called by the batch
  backfill over three months of Parquet and by the streaming job over a Kafka topic.
  `tests/test_transforms.py` runs it both ways over the same events and asserts identical rows.
- `src/citibike_fs/training/train.py`: the training set is built with
  `get_historical_features`, so every feature is joined as it was known at each label's
  timestamp. The model is compared against two baselines on a time-based split.
- `src/citibike_fs/serving/online.py`: Feast applies TTLs in offline joins but returns
  the last written value online, however old. This module applies the TTL online too,
  so a stale value is treated as missing in serving exactly as it was in training.
- `feature_repo/feature_store.local.yaml` vs `feature_store.aws.yaml`: the entire
  difference between environments on the feature-store side.

## Run it locally

Needs Docker with about 6 GB of memory. The default scope is Jersey City and Hoboken
(~110 stations, ~100k trips a month); set `CITIBIKE_SCOPE=nyc` for all of NYC
(~2,400 stations, millions of trips a month, much heavier).

```bash
make demo        # build, ingest 3 months of trips, compute features, train, start services
```

Then open the dashboard at http://localhost:8501 and the API at http://localhost:8000/docs.
Stream features appear within a couple of minutes of `make up`. Each step can be run
alone (`make help`), and `make test` runs the tests.

```bash
curl -s -X POST localhost:8000/predict -H 'content-type: application/json' \
  -d '{"station_ids": ["HB101", "JC115"]}' | python3 -m json.tool
```

The response includes each feature view's age and how many values were dropped as expired.

## Deploy to AWS

Needs Terraform ≥ 1.6, AWS CLI v2 with credentials, Docker and Python 3.

```bash
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars   # set allowed_cidrs to your IP
scripts/aws.sh deploy      # VPC, S3, Glue, Athena, MSK, EMR Serverless, ECS, ALB; builds and pushes both images
scripts/aws.sh bootstrap   # ingest, EMR batch job, feast apply, materialize, train, start the streaming job
scripts/aws.sh status      # URLs, service health, streaming job
scripts/aws.sh destroy     # stop streaming, drop Feast's DynamoDB tables, terraform destroy
```

`scripts/aws.sh update` rebuilds and redeploys after a code change, and
`scripts/aws.sh run '<command>'` runs any one-off command (for example a retrain) in a
Fargate task with the same image and permissions as the services.

Two images are built: `docker/Dockerfile` for everything on Fargate (the same image as
local), and `infra/emr/Dockerfile`, the EMR Serverless base image plus Python 3.11,
Feast and this package.

**Cost.** This runs continuously until destroyed. The largest items are MSK Serverless
(billed per cluster-hour, several hundred dollars a month on its own), the always-on EMR
Serverless streaming job, the NAT gateway, the load balancer and four small Fargate
services. Check current AWS pricing for your region, and run `scripts/aws.sh destroy`
when you are done. Setting `services_desired_count = 0` and `stop-streaming` pauses most
of the compute but not MSK, NAT or the ALB.

## Design notes and known simplifications

- **The trip stream is a replay.** Citi Bike publishes trips monthly, so there is no live
  trip feed. `ingest/replayer.py` replays the latest month shifted forward by whole weeks
  (keeping hour-of-week patterns) and emits departures and arrivals at their shifted times.
  Its offline copy goes to `stream_log/`, not the offline store. Station availability and
  weather are genuinely live.
- **Live availability is logged but not yet used by the model.** There is no public
  history of GBFS snapshots, so the streaming job writes each one to the offline store as
  well as online. Once a few weeks have accumulated, availability can join the training set.
  This is the usual log-and-wait pattern for new features.
- **Streaming latency is not modelled.** Offline, a 15-minute window is available at its end
  time; online it lands a few minutes later (watermark plus trigger interval).
- **Offline writes from the stream are at least once.** A retried micro-batch can append
  duplicate rows; the online store is unaffected. A production job would write to a table
  format with idempotent commits (Delta, Iceberg).
- **The historical weather is reanalysis and the live weather is a forecast model**, both
  hourly from Open-Meteo, so the definitions match but the sources differ slightly.
- **Orchestration is scripts** (Make locally, `scripts/aws.sh` on AWS). A scheduler such as
  Dagster or Step Functions would run the batch job, materialization and retraining daily.

## Layout

```
feature_repo/          Feast definitions and the local / AWS profiles
src/citibike_fs/
  ingest/              trip download, GBFS, weather, poller, trip replayer
  spark/               shared transforms, batch backfill, streaming job
  training/            training-set build and model training
  serving/             FastAPI app and TTL-aware online retrieval
  dashboard/           Streamlit dashboard
infra/terraform/       AWS infrastructure
infra/emr/             EMR Serverless image and job entry points
scripts/aws.sh         AWS deploy / bootstrap / operate
tests/                 transform and replay tests
```
