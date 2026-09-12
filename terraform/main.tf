terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "Região AWS padrão para provisionamento dos recursos"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Ambiente de implantação (ex: dev, prod)"
}

variable "project_name" {
  type        = string
  default     = "zero-cost-etl"
  description = "Nome do projeto para uso em tags e nomes de recursos"
}

variable "enable_kafka_trigger" {
  type        = bool
  default     = false
  description = "Habilita o consumo do topico Kafka pela Lambda"
}

variable "kafka_source_type" {
  type        = string
  default     = "msk"
  description = "Tipo da origem Kafka: msk ou self_managed"

  validation {
    condition     = contains(["msk", "self_managed"], var.kafka_source_type)
    error_message = "kafka_source_type deve ser 'msk' ou 'self_managed'."
  }
}

variable "kafka_topic" {
  type        = string
  default     = "fraud-detection-topic"
  description = "Topico de resultados de fraude consumido pela Lambda"
}

variable "kafka_consumer_group" {
  type        = string
  default     = "noctus-curation-v1"
  description = "Consumer group exclusivo da camada Curated"
}

variable "kafka_starting_position" {
  type        = string
  default     = "LATEST"
  description = "Posicao inicial do event source mapping"

  validation {
    condition     = contains(["LATEST", "TRIM_HORIZON"], var.kafka_starting_position)
    error_message = "kafka_starting_position deve ser LATEST ou TRIM_HORIZON."
  }
}

variable "kafka_msk_cluster_arn" {
  type        = string
  default     = ""
  description = "ARN do cluster Amazon MSK quando kafka_source_type for msk"
}

variable "kafka_bootstrap_servers" {
  type        = list(string)
  default     = []
  description = "Brokers host:porta para Kafka self-managed"
}

variable "kafka_subnet_ids" {
  type        = list(string)
  default     = []
  description = "Subnets que permitem acesso ao Kafka self-managed"
}

variable "kafka_security_group_ids" {
  type        = list(string)
  default     = []
  description = "Security groups que permitem acesso ao Kafka self-managed"
}

variable "kafka_auth_type" {
  type        = string
  default     = "SASL_SCRAM_512_AUTH"
  description = "Tipo de autenticacao do Kafka self-managed"

  validation {
    condition = contains([
      "BASIC_AUTH",
      "SASL_SCRAM_256_AUTH",
      "SASL_SCRAM_512_AUTH"
    ], var.kafka_auth_type)
    error_message = "kafka_auth_type deve ser BASIC_AUTH, SASL_SCRAM_256_AUTH ou SASL_SCRAM_512_AUTH."
  }
}

variable "kafka_auth_secret_arn" {
  type        = string
  default     = ""
  sensitive   = true
  description = "ARN do segredo no Secrets Manager para Kafka self-managed"
}
