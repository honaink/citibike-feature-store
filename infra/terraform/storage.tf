# S3 data lake, the Glue tables Athena reads, and the container registries.

resource "aws_s3_bucket" "data" {
  bucket = "${var.name}-${local.account_id}-${var.region}"
  # A demo bucket: `terraform destroy` removes it together with its contents.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-scratch"
    status = "Enabled"

    filter {
      prefix = "athena/"
    }

    expiration {
      days = 7
    }
  }
}

resource "aws_glue_catalog_database" "features" {
  name = replace(var.name, "-", "_")
}

locals {
  # Must match the Parquet written by the Spark jobs and the schemas in feature_repo/definitions.py.
  feature_tables = {
    station_recent_activity = [
      ["station_id", "string"],
      ["event_timestamp", "timestamp"],
      ["departures_15m", "bigint"],
      ["departures_1h", "bigint"],
      ["arrivals_15m", "bigint"],
      ["arrivals_1h", "bigint"],
    ]
    station_hourly_profile = [
      ["station_id", "string"],
      ["hour_of_week", "bigint"],
      ["event_timestamp", "timestamp"],
      ["avg_departures_4w", "double"],
      ["avg_arrivals_4w", "double"],
    ]
    station_live_status = [
      ["station_id", "string"],
      ["event_timestamp", "timestamp"],
      ["num_bikes_available", "bigint"],
      ["num_ebikes_available", "bigint"],
      ["num_docks_available", "bigint"],
      ["is_renting", "bigint"],
    ]
    weather_hourly = [
      ["region_id", "string"],
      ["event_timestamp", "timestamp"],
      ["temperature_c", "double"],
      ["precipitation_mm", "double"],
      ["wind_speed_kmh", "double"],
    ]
  }
}

resource "aws_glue_catalog_table" "features" {
  for_each      = local.feature_tables
  name          = each.key
  database_name = aws_glue_catalog_database.features.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "${local.data_root}/features/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = each.value

      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name          = var.name
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "${local.data_root}/athena/results/"
    }
  }
}

resource "aws_ecr_repository" "app" {
  name         = "${var.name}/app"
  force_delete = true
}

resource "aws_ecr_repository" "emr" {
  name         = "${var.name}/emr"
  force_delete = true
}

# EMR Serverless pulls the custom image itself and needs to be allowed to.
resource "aws_ecr_repository_policy" "emr" {
  repository = aws_ecr_repository.emr.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "EmrServerlessCustomImageSupport"
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = ["ecr:BatchGetImage", "ecr:DescribeImages", "ecr:GetDownloadUrlForLayer"]
      Condition = {
        ArnLike = {
          "aws:SourceArn" = "arn:aws:emr-serverless:${var.region}:${local.account_id}:/applications/*"
        }
      }
    }]
  })
}
