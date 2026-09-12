# IAM Role para a Função Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-${var.environment}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}

# IAM Policy para permitir escrita de Logs e acesso ao S3
resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project_name}-${var.environment}-lambda-policy"
  description = "Politica de acesso para o Lambda ETL ler/escrever no S3 e logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Permissões do CloudWatch Logs (Always Free)
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      # Permissões S3 limitadas aos nossos buckets
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data.arn,
          "${aws_s3_bucket.raw_data.arn}/*",
          aws_s3_bucket.curated_data.arn,
          "${aws_s3_bucket.curated_data.arn}/*"
        ]
      },
      # Permissões adicionais de rede caso se conecte a uma VPC para acessar o Kafka
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs"
        ]
        Resource = "*"
      },
      # A Lambda envia os lotes Kafka descartados para a fila de falhas.
      {
        Effect = "Allow"
        Action = [
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:SendMessage"
        ]
        Resource = aws_sqs_queue.kafka_failures.arn
      }
    ]
  })
}

# Associar política à Role
resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Zipar o código da Lambda automaticamente
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda_function.py"
  output_path = "${path.module}/lambda_function.zip"
}

# Definir a função Lambda
resource "aws_lambda_function" "etl_lambda" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-${var.environment}-etl"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = 60  # Timeout de 1 minuto (bem dentro do limite de 15 min do Lambda)
  memory_size      = 256 # 256MB de RAM (ideal para transformações de tamanho pequeno/médio a baixo custo)
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = {
      CURATED_BUCKET_NAME = aws_s3_bucket.curated_data.id
      CURATED_PREFIX      = "fraud-assessments"
      ENVIRONMENT         = var.environment
    }
  }

  tags = {
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}

output "lambda_function_arn" {
  value       = aws_lambda_function.etl_lambda.arn
  description = "ARN da Função Lambda do ETL"
}
