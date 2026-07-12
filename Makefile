# Local demo. `make demo` runs everything in order; `make help` lists the steps.
.DEFAULT_GOAL := help

COMPOSE := docker compose
RUN := $(COMPOSE) run --rm jobs
FEAST := feast -c feature_repo -f feature_repo/feature_store.local.yaml
SERVICES := poller replayer streaming api dashboard

.PHONY: help build infra ingest features apply materialize train up demo test logs ps down

help:
	@echo "make demo         build, ingest, compute features, train and start everything"
	@echo ""
	@echo "make build        build the image"
	@echo "make infra        start Redpanda (Kafka) and Redis"
	@echo "make ingest       download trips, stations and weather"
	@echo "make features     Spark batch job: offline features and training labels"
	@echo "make apply        register feature definitions with Feast"
	@echo "make materialize  load the latest batch features into Redis"
	@echo "make train        build a point-in-time training set and train the model"
	@echo "make up           start the poller, replayer, Spark streaming job, API and dashboard"
	@echo "make test         run the tests"
	@echo "make logs         follow the streaming pipeline logs"
	@echo "make down         stop everything (data in ./data is kept)"
	@echo ""
	@echo "Dashboard: http://localhost:8501   API: http://localhost:8000/docs"

build:
	$(COMPOSE) build jobs

infra:
	$(COMPOSE) up -d --wait redpanda redis

ingest: infra
	$(RUN) python -m citibike_fs.ingest.download_trips
	$(RUN) python -m citibike_fs.ingest.gbfs
	$(RUN) python -m citibike_fs.ingest.weather

features:
	$(RUN) python -m citibike_fs.spark.batch_features

apply:
	$(RUN) $(FEAST) apply

materialize: infra
	$(RUN) python -m citibike_fs.materialize

train:
	$(RUN) python -m citibike_fs.training.train

up: infra
	$(COMPOSE) up -d $(SERVICES)

demo: build ingest features apply materialize train up
	@echo "Dashboard: http://localhost:8501 (stream features fill in within a few minutes)"

test:
	$(RUN) pytest -q

logs:
	$(COMPOSE) logs -f --tail=20 streaming replayer poller

ps:
	$(COMPOSE) ps

down:
	$(COMPOSE) down
