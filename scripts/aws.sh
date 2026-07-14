#!/usr/bin/env bash
# AWS deployment helper. Needs: terraform >= 1.6, AWS CLI v2, docker, python3.
#
#   scripts/aws.sh deploy            create the infrastructure and push the images
#   scripts/aws.sh bootstrap         ingest, batch features, feast apply, materialize, train, start streaming
#   scripts/aws.sh update            rebuild and push the images, restart services and streaming
#   scripts/aws.sh run '<command>'   run a one-off command in the ops task, e.g. 'python -m citibike_fs.training.train'
#   scripts/aws.sh start-streaming | stop-streaming | status
#   scripts/aws.sh destroy           stop streaming, drop Feast's DynamoDB tables, terraform destroy
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TF_DIR="$ROOT/infra/terraform"
IMAGE_TAG="${IMAGE_TAG:-latest}"
STREAMING_JOB_NAME="streaming-features"
FEAST_AWS="feast -c /app/feature_repo -f /app/feature_repo/feature_store.aws.yaml"
KAFKA_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,software.amazon.msk:aws-msk-iam-auth:2.3.9"

tf() { terraform -chdir="$TF_DIR" "$@"; }
out() { tf output -raw "$1"; }
tfvar() { echo "var.$1" | tf console | tr -d '"'; }
json() { python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"; }
log() { printf '\n==> %s\n' "$*"; }

push_images() {
  local region app emr registry
  region="$(tfvar region)"
  app="$(out app_repository_url)"
  emr="$(out emr_repository_url)"
  registry="${app%%/*}"
  aws ecr get-login-password --region "$region" | docker login --username AWS --password-stdin "$registry"

  log "building and pushing $app:$IMAGE_TAG"
  docker build --platform linux/amd64 -f "$ROOT/docker/Dockerfile" -t "$app:$IMAGE_TAG" "$ROOT"
  docker push "$app:$IMAGE_TAG"

  log "building and pushing $emr:$IMAGE_TAG"
  docker build --platform linux/amd64 -f "$ROOT/infra/emr/Dockerfile" \
    --build-arg EMR_RELEASE="$(tfvar emr_release)" -t "$emr:$IMAGE_TAG" "$ROOT"
  docker push "$emr:$IMAGE_TAG"
}

cmd_deploy() {
  tf init -input=false
  log "creating the container registries first, so the images exist before anything uses them"
  tf apply -input=false -target=aws_ecr_repository.app -target=aws_ecr_repository.emr \
    -target=aws_ecr_repository_policy.emr
  push_images
  log "creating everything else"
  tf apply -input=false -var "image_tag=$IMAGE_TAG"
  log "done. Next: scripts/aws.sh bootstrap"
}

# Run a shell command in the ops task definition and wait for it to finish.
cmd_run() {
  local command="$1" cluster subnets task_arn exit_code
  cluster="$(out ecs_cluster)"
  subnets="$(tf output -json private_subnet_ids | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin)))')"
  log "ops task: $command"
  task_arn="$(aws ecs run-task \
    --region "$(out region)" \
    --cluster "$cluster" \
    --task-definition "$(out ops_task_definition)" \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$subnets],securityGroups=[$(out workload_security_group_id)],assignPublicIp=DISABLED}" \
    --overrides "{\"containerOverrides\":[{\"name\":\"ops\",\"command\":[\"sh\",\"-c\",$(json "$command")]}]}" \
    --query 'tasks[0].taskArn' --output text)"
  # The CLI waiter gives up after 10 minutes; keep waiting for longer tasks.
  for _ in $(seq 1 12); do
    aws ecs wait tasks-stopped --region "$(out region)" --cluster "$cluster" --tasks "$task_arn" && break
  done
  exit_code="$(aws ecs describe-tasks --region "$(out region)" --cluster "$cluster" --tasks "$task_arn" \
    --query 'tasks[0].containers[0].exitCode' --output text)"
  if [ "$exit_code" != "0" ]; then
    echo "ops task failed (exit code $exit_code). Logs:" >&2
    echo "  aws logs tail /ecs/$(tfvar name) --log-stream-names ops/ops/${task_arn##*/}" >&2
    exit 1
  fi
}

# Build the spark-submit parameters shared by the batch and streaming jobs.
spark_params() {
  local extra="$1" params=""
  params+="--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=/usr/bin/python3.11 "
  params+="--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=/usr/bin/python3.11 "
  params+="--conf spark.executorEnv.PYSPARK_PYTHON=/usr/bin/python3.11 "
  local -a env=(
    "CITIBIKE_ENV=aws"
    "CITIBIKE_SCOPE=$(out scope)"
    "DATA_ROOT=s3://$(out bucket)"
    "AWS_REGION=$(out region)"
    "AWS_DEFAULT_REGION=$(out region)"
    "KAFKA_BOOTSTRAP_SERVERS=$(out kafka_bootstrap_servers)"
    "KAFKA_AUTH=msk_iam"
    "ATHENA_DATABASE=$(out athena_database)"
    "ATHENA_WORKGROUP=$(out athena_workgroup)"
    "FEAST_REPO_PATH=/opt/citibike/feature_repo"
    "FEAST_USAGE=False"
  )
  for kv in "${env[@]}"; do
    params+="--conf spark.emr-serverless.driverEnv.$kv "
  done
  params+="--conf spark.driver.cores=2 --conf spark.driver.memory=4g "
  params+="--conf spark.executor.cores=2 --conf spark.executor.memory=4g $extra"
  echo "$params"
}

# Start an EMR Serverless job run and print its id.
start_job() {
  local name="$1" entrypoint="$2" mode="$3" extra="$4" driver
  driver="{\"sparkSubmit\":{\"entryPoint\":\"s3://$(out bucket)/code/$entrypoint\",\"sparkSubmitParameters\":$(json "$(spark_params "$extra")")}}"
  aws emr-serverless start-job-run \
    --region "$(out region)" \
    --application-id "$(out emr_application_id)" \
    --execution-role-arn "$(out emr_job_role_arn)" \
    --name "$name" \
    --mode "$mode" \
    --job-driver "$driver" \
    --configuration-overrides "{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://$(out bucket)/emr-logs/\"}}}" \
    --query jobRunId --output text
}

wait_job() {
  local job_id="$1" state
  while true; do
    state="$(aws emr-serverless get-job-run --region "$(out region)" \
      --application-id "$(out emr_application_id)" --job-run-id "$job_id" \
      --query jobRun.state --output text)"
    case "$state" in
      SUCCESS) return 0 ;;
      FAILED | CANCELLED)
        echo "EMR job $job_id ended $state. Driver logs: s3://$(out bucket)/emr-logs/applications/$(out emr_application_id)/jobs/$job_id/" >&2
        exit 1
        ;;
    esac
    sleep 20
  done
}

running_streaming_jobs() {
  aws emr-serverless list-job-runs --region "$(out region)" \
    --application-id "$(out emr_application_id)" --states RUNNING SCHEDULED PENDING SUBMITTED \
    --query "jobRuns[?name=='$STREAMING_JOB_NAME'].id" --output text
}

cmd_start_streaming() {
  if [ -n "$(running_streaming_jobs)" ]; then
    echo "streaming job already running"
    return
  fi
  log "starting the Spark Structured Streaming job"
  start_job "$STREAMING_JOB_NAME" streaming_features.py STREAMING \
    "--conf spark.dynamicAllocation.enabled=false --conf spark.executor.instances=1 --packages $KAFKA_PACKAGES"
}

cmd_stop_streaming() {
  local id
  for id in $(running_streaming_jobs); do
    log "cancelling streaming job $id"
    aws emr-serverless cancel-job-run --region "$(out region)" \
      --application-id "$(out emr_application_id)" --job-run-id "$id" >/dev/null
  done
}

cmd_bootstrap() {
  cmd_run "python -m citibike_fs.ingest.download_trips && python -m citibike_fs.ingest.gbfs && python -m citibike_fs.ingest.weather"
  log "Spark batch job: offline features and training labels"
  wait_job "$(start_job batch-features batch_features.py BATCH "")"
  cmd_run "$FEAST_AWS apply && python -m citibike_fs.materialize && python -m citibike_fs.training.train"
  cmd_start_streaming
  log "done. Dashboard: $(out dashboard_url)  (stream features fill in within a few minutes)"
}

cmd_update() {
  push_images
  local region cluster service
  region="$(out region)"
  cluster="$(out ecs_cluster)"
  for service in poller replayer api dashboard; do
    aws ecs update-service --region "$region" --cluster "$cluster" --service "$service" \
      --force-new-deployment --query 'service.serviceName' --output text
  done
  cmd_stop_streaming
  log "stopping the EMR application so it picks up the new image"
  # Stopping is refused until the cancelled streaming job has wound down, so keep asking.
  while [ "$(aws emr-serverless get-application --region "$region" --application-id "$(out emr_application_id)" \
    --query application.state --output text)" != "STOPPED" ]; do
    aws emr-serverless stop-application --region "$region" \
      --application-id "$(out emr_application_id)" >/dev/null 2>&1 || true
    sleep 15
  done
  aws emr-serverless update-application --region "$region" --application-id "$(out emr_application_id)" \
    --image-configuration "{\"imageUri\":\"$(out emr_repository_url):$IMAGE_TAG\"}" >/dev/null
  cmd_start_streaming
}

cmd_status() {
  echo "dashboard:  $(out dashboard_url)"
  echo "api:        $(out api_url)/docs"
  echo "streaming:  $(running_streaming_jobs || true)"
  aws ecs describe-services --region "$(out region)" --cluster "$(out ecs_cluster)" \
    --services poller replayer api dashboard \
    --query 'services[].[serviceName,runningCount,desiredCount]' --output table
}

cmd_destroy() {
  cmd_stop_streaming || true
  log "removing Feast's DynamoDB tables (created by feast apply, not by terraform)"
  cmd_run "$FEAST_AWS teardown" || echo "teardown failed; delete the citibike.* DynamoDB tables by hand" >&2
  tf destroy -input=false
}

case "${1:-}" in
  deploy) cmd_deploy ;;
  bootstrap) cmd_bootstrap ;;
  update) cmd_update ;;
  run) cmd_run "${2:?usage: scripts/aws.sh run '<command>'}" ;;
  start-streaming) cmd_start_streaming ;;
  stop-streaming) cmd_stop_streaming ;;
  status) cmd_status ;;
  destroy) cmd_destroy ;;
  *) sed -n '2,11p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
