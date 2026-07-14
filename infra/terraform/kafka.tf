resource "aws_msk_serverless_cluster" "this" {
  cluster_name = var.name

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.kafka.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}

locals {
  # arn:aws:kafka:region:account:cluster/<name>/<uuid>  ->  .../topic/<name>/<uuid>/*
  kafka_topic_arns = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/")}/*"
  kafka_group_arns = "${replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")}/*"
}
