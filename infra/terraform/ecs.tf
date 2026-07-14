# The always-on Python services run on Fargate from the same image as the local stack.

resource "aws_ecs_cluster" "this" {
  name = var.name
}

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/${var.name}"
  retention_in_days = 14
}

locals {
  app_image = "${aws_ecr_repository.app.repository_url}:${var.image_tag}"

  app_environment = {
    CITIBIKE_ENV            = "aws"
    CITIBIKE_SCOPE          = var.scope
    DATA_ROOT               = local.data_root
    AWS_REGION              = var.region
    AWS_DEFAULT_REGION      = var.region
    KAFKA_BOOTSTRAP_SERVERS = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
    KAFKA_AUTH              = "msk_iam"
    ATHENA_DATABASE         = aws_glue_catalog_database.features.name
    ATHENA_WORKGROUP        = aws_athena_workgroup.this.name
    FEAST_USAGE             = "False"
    API_URL                 = "http://${aws_lb.this.dns_name}:8000"
  }

  services = {
    poller = {
      command = ["python", "-m", "citibike_fs.ingest.poller"]
      cpu     = 256
      memory  = 1024
      port    = null
    }
    replayer = {
      command = ["python", "-m", "citibike_fs.ingest.replayer"]
      cpu     = 512
      memory  = var.scope == "nyc" ? 8192 : 2048
      port    = null
    }
    api = {
      command = ["uvicorn", "citibike_fs.serving.api:app", "--host", "0.0.0.0", "--port", "8000"]
      cpu     = 512
      memory  = 2048
      port    = 8000
    }
    dashboard = {
      command = [
        "streamlit", "run", "src/citibike_fs/dashboard/app.py",
        "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true",
        "--browser.gatherUsageStats=false",
      ]
      cpu    = 256
      memory = 1024
      port   = 8501
    }
    # No service: run on demand for downloads, `feast apply`, materialisation and training.
    ops = {
      command = ["python", "-c", "print('pass a command override')"]
      cpu     = 2048
      memory  = 8192
      port    = null
    }
  }

  load_balanced = { for k, v in local.services : k => v if v.port != null }
}

resource "aws_ecs_task_definition" "this" {
  for_each                 = local.services
  family                   = "${var.name}-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name         = each.key
    image        = local.app_image
    essential    = true
    command      = each.value.command
    environment  = [for k, v in local.app_environment : { name = k, value = v }]
    portMappings = each.value.port == null ? [] : [{ containerPort = each.value.port, protocol = "tcp" }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.ecs.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = each.key
      }
    }
  }])
}

resource "aws_ecs_service" "this" {
  for_each        = { for k, v in local.services : k => v if k != "ops" }
  name            = each.key
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this[each.key].arn
  desired_count   = var.services_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets         = aws_subnet.private[*].id
    security_groups = [aws_security_group.workload.id]
  }

  dynamic "load_balancer" {
    for_each = each.value.port == null ? [] : [each.value.port]

    content {
      target_group_arn = aws_lb_target_group.this[each.key].arn
      container_name   = each.key
      container_port   = load_balancer.value
    }
  }

  depends_on = [aws_lb_listener.this, aws_nat_gateway.this]
}

# --- load balancer: dashboard on :80, API on :8000 ---------------------------

resource "aws_lb" "this" {
  name               = var.name
  load_balancer_type = "application"
  subnets            = aws_subnet.public[*].id
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "this" {
  for_each    = local.load_balanced
  name        = "${var.name}-${each.key}"
  vpc_id      = aws_vpc.this.id
  target_type = "ip"
  protocol    = "HTTP"
  port        = each.value.port

  health_check {
    path                = each.key == "api" ? "/health" : "/_stcore/health"
    healthy_threshold   = 2
    unhealthy_threshold = 5
    interval            = 15
    timeout             = 5
  }
}

resource "aws_lb_listener" "this" {
  for_each          = local.load_balanced
  load_balancer_arn = aws_lb.this.arn
  protocol          = "HTTP"
  port              = each.key == "api" ? 8000 : 80

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this[each.key].arn
  }
}
