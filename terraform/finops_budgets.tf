variable "notification_email" {
  type        = string
  default     = "alerta-finops@suastartup.com"
  description = "E-mail que recebera os alertas de custos da AWS"
}

# AWS Budget para monitorar e travar custos
# O AWS Budgets oferece 2 orçamentos gratuitos por conta (Always Free).
resource "aws_budgets_budget" "zero_cost_limit" {
  name              = "${var.project_name}-zero-cost-budget"
  budget_type       = "COST"
  limit_amount      = "0.01" # Alarme disparado se passar de 1 centavo de dólar
  limit_unit        = "USD"
  time_period_start = "2026-01-01_00:00"
  time_unit         = "MONTHLY"

  # Alerta 1: Custo Real atinge 80% do limite ($0.008)
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.notification_email]
  }

  # Alerta 2: Custo Previsto (Forecasted) atinge 100% do limite ($0.01)
  # Isso ajuda a prever cobranças antes que elas aconteçam no fechamento do mês
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.notification_email]
  }

  tags = {
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}
