resource "aws_emrserverless_application" "spark" {
  name          = var.name
  release_label = var.emr_release
  type          = "spark"

  # Built from infra/emr/Dockerfile: the EMR base image plus Python 3.11, Feast and this project.
  image_configuration {
    image_uri = "${aws_ecr_repository.emr.repository_url}:${var.image_tag}"
  }

  network_configuration {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.workload.id]
  }

  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  depends_on = [aws_ecr_repository_policy.emr]
}

# Job entry points. The project itself is installed in the image; these only call into it.
resource "aws_s3_object" "entrypoints" {
  for_each = fileset("${path.module}/../emr/entrypoints", "*.py")
  bucket   = aws_s3_bucket.data.id
  key      = "code/${each.value}"
  source   = "${path.module}/../emr/entrypoints/${each.value}"
  etag     = filemd5("${path.module}/../emr/entrypoints/${each.value}")
}
