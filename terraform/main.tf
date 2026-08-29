terraform {
  required_version = ">= 1.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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
