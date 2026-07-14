output "dashboard_url" {
  value = "http://${aws_lb.this.dns_name}"
}

output "api_url" {
  value = "http://${aws_lb.this.dns_name}:8000"
}

output "region" {
  value = var.region
}

output "scope" {
  value = var.scope
}

output "bucket" {
  value = aws_s3_bucket.data.bucket
}

output "app_repository_url" {
  value = aws_ecr_repository.app.repository_url
}

output "emr_repository_url" {
  value = aws_ecr_repository.emr.repository_url
}

output "emr_release" {
  value = var.emr_release
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  value = aws_iam_role.emr_job.arn
}

output "ecs_cluster" {
  value = aws_ecs_cluster.this.name
}

output "ops_task_definition" {
  value = aws_ecs_task_definition.this["ops"].family
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "workload_security_group_id" {
  value = aws_security_group.workload.id
}

output "kafka_bootstrap_servers" {
  value = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}

output "athena_database" {
  value = aws_glue_catalog_database.features.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}
