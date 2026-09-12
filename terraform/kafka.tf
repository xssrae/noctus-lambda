locals {
  msk_topic_arn = var.kafka_msk_cluster_arn == "" ? "*" : "${replace(var.kafka_msk_cluster_arn, ":cluster/", ":topic/")}/*"
  msk_group_arn = var.kafka_msk_cluster_arn == "" ? "*" : "${replace(var.kafka_msk_cluster_arn, ":cluster/", ":group/")}/*"

  self_managed_access = concat(
    [for subnet_id in var.kafka_subnet_ids : {
      type = "VPC_SUBNET"
      uri  = "subnet:${subnet_id}"
    }],
    [for security_group_id in var.kafka_security_group_ids : {
      type = "VPC_SECURITY_GROUP"
      uri  = "security_group:${security_group_id}"
    }],
    var.kafka_auth_secret_arn == "" ? [] : [{
      type = var.kafka_auth_type
      uri  = var.kafka_auth_secret_arn
    }]
  )
}

resource "aws_sqs_queue" "kafka_failures" {
  name                      = "${var.project_name}-${var.environment}-kafka-failures"
  message_retention_seconds = 1209600

  tags = {
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}

resource "aws_iam_role_policy" "msk_access" {
  count = var.enable_kafka_trigger && var.kafka_source_type == "msk" ? 1 : 0

  name = "${var.project_name}-${var.environment}-msk-access"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka:DescribeClusterV2",
          "kafka:GetBootstrapBrokers",
          "kafka-cluster:Connect"
        ]
        Resource = var.kafka_msk_cluster_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData"
        ]
        Resource = local.msk_topic_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup"
        ]
        Resource = local.msk_group_arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "self_managed_secret_access" {
  count = var.enable_kafka_trigger && var.kafka_source_type == "self_managed" ? 1 : 0

  name = "${var.project_name}-${var.environment}-kafka-secret-access"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = var.kafka_auth_secret_arn
    }]
  })
}

resource "aws_lambda_event_source_mapping" "msk" {
  count = var.enable_kafka_trigger && var.kafka_source_type == "msk" ? 1 : 0

  event_source_arn  = var.kafka_msk_cluster_arn
  function_name     = aws_lambda_function.etl_lambda.arn
  topics            = [var.kafka_topic]
  starting_position = var.kafka_starting_position
  batch_size        = 100

  amazon_managed_kafka_event_source_config {
    consumer_group_id = var.kafka_consumer_group
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.kafka_failures.arn
    }
  }

  lifecycle {
    precondition {
      condition     = var.kafka_msk_cluster_arn != ""
      error_message = "kafka_msk_cluster_arn e obrigatorio quando o trigger MSK esta habilitado."
    }
  }
}

resource "aws_lambda_event_source_mapping" "self_managed" {
  count = var.enable_kafka_trigger && var.kafka_source_type == "self_managed" ? 1 : 0

  function_name     = aws_lambda_function.etl_lambda.arn
  topics            = [var.kafka_topic]
  starting_position = var.kafka_starting_position
  batch_size        = 100

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = join(",", var.kafka_bootstrap_servers)
    }
  }

  self_managed_kafka_event_source_config {
    consumer_group_id = var.kafka_consumer_group
  }

  dynamic "source_access_configuration" {
    for_each = local.self_managed_access
    content {
      type = source_access_configuration.value.type
      uri  = source_access_configuration.value.uri
    }
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.kafka_failures.arn
    }
  }

  lifecycle {
    precondition {
      condition     = length(var.kafka_bootstrap_servers) > 0
      error_message = "kafka_bootstrap_servers e obrigatorio para Kafka self-managed."
    }
    precondition {
      condition     = length(var.kafka_subnet_ids) > 0 && length(var.kafka_security_group_ids) > 0
      error_message = "Informe ao menos uma subnet e um security group para Kafka self-managed."
    }
    precondition {
      condition     = var.kafka_auth_secret_arn != ""
      error_message = "kafka_auth_secret_arn e obrigatorio para Kafka self-managed."
    }
  }
}

output "kafka_failure_queue_url" {
  value       = aws_sqs_queue.kafka_failures.url
  description = "Fila que recebe lotes Kafka descartados pela Lambda"
}
