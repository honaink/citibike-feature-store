# One data-access policy shared by the ECS tasks and the EMR job role: both read and
# write the lake, talk to Kafka and use the feature store.

data "aws_iam_policy_document" "data_access" {
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }

  statement {
    sid       = "LakeBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.data.arn]
  }

  statement {
    sid = "FeastOnlineStore"
    actions = [
      "dynamodb:CreateTable",
      "dynamodb:DeleteTable",
      "dynamodb:DescribeTable",
      "dynamodb:UpdateTable",
      "dynamodb:TagResource",
      "dynamodb:PutItem",
      "dynamodb:GetItem",
      "dynamodb:DeleteItem",
      "dynamodb:UpdateItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:BatchGetItem",
      "dynamodb:Query",
      "dynamodb:Scan",
    ]
    # Feast names its tables "<project>.<feature view>".
    resources = ["arn:aws:dynamodb:${var.region}:${local.account_id}:table/citibike.*"]
  }

  statement {
    sid = "FeastOfflineStoreQueries"
    actions = [
      "athena:StartQueryExecution",
      "athena:StopQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.this.arn]
  }

  statement {
    sid = "FeastOfflineStoreCatalog"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      # Feast stages the entity dataframe as a temporary table for point-in-time joins.
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
    ]
    resources = [
      "arn:aws:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.features.arn,
      "arn:aws:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.features.name}/*",
    ]
  }

  statement {
    sid       = "KafkaCluster"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [aws_msk_serverless_cluster.this.arn]
  }

  statement {
    sid = "KafkaTopics"
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:DescribeTopicDynamicConfiguration",
      "kafka-cluster:AlterTopicDynamicConfiguration",
      "kafka-cluster:WriteData",
      "kafka-cluster:WriteDataIdempotently",
      "kafka-cluster:ReadData",
    ]
    resources = [local.kafka_topic_arns, aws_msk_serverless_cluster.this.arn]
  }

  statement {
    sid       = "KafkaGroups"
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = [local.kafka_group_arns]
  }
}

resource "aws_iam_policy" "data_access" {
  name   = "${var.name}-data-access"
  policy = data.aws_iam_policy_document.data_access.json
}

# --- ECS ---------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_execution" {
  name               = "${var.name}-ecs-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name               = "${var.name}-ecs-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "ecs_task" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.data_access.arn
}

# --- EMR Serverless ----------------------------------------------------------

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_job" {
  name               = "${var.name}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_job" {
  role       = aws_iam_role.emr_job.name
  policy_arn = aws_iam_policy.data_access.arn
}
