resource "aws_s3_bucket" "raw_data" {
  bucket        = "${var.project_name}-${var.environment}-raw-data"
  force_destroy = true

  tags = {
    Name        = "${var.project_name}-raw-data"
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}

resource "aws_s3_bucket" "curated_data" {
  bucket        = "${var.project_name}-${var.environment}-curated-data"
  force_destroy = true

  tags = {
    Name        = "${var.project_name}-curated-data"
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}

# Bloquear acesso público para segurança
resource "aws_s3_bucket_public_access_block" "raw_data_block" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "curated_data_block" {
  bucket = aws_s3_bucket.curated_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Habilitar versionamento para integridade dos dados (opcional, mas recomendado)
resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Regra de Ciclo de Vida para o Bucket de Dados Brutos (Raw)
# Para evitar ultrapassar o limite de 5 GB do S3 Free Tier, apagamos dados brutos após 14 dias.
resource "aws_s3_bucket_lifecycle_configuration" "raw_data_lifecycle" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "expire_old_raw_files"
    status = "Enabled"

    filter {}

    expiration {
      days = 14
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

# Regra de Ciclo de Vida para os dados consolidados (Curated)
# Mantém os dados finais por 90 dias, depois expira para garantir que o limite de 5 GB nunca seja estourado.
resource "aws_s3_bucket_lifecycle_configuration" "curated_data_lifecycle" {
  bucket = aws_s3_bucket.curated_data.id

  rule {
    id     = "expire_old_curated_files"
    status = "Enabled"

    filter {}

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

output "raw_bucket_name" {
  value       = aws_s3_bucket.raw_data.id
  description = "Nome do bucket S3 de dados brutos"
}

output "curated_bucket_name" {
  value       = aws_s3_bucket.curated_data.id
  description = "Nome do bucket S3 de dados consolidados"
}
