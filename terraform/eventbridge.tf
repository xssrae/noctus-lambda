# Regra do EventBridge para executar o Lambda diariamente (Orquestrador)
resource "aws_cloudwatch_event_rule" "daily_etl_trigger" {
  name                = "${var.project_name}-${var.environment}-daily-trigger"
  description         = "Dispara a funcao Lambda do ETL diariamente a meia-noite"
  schedule_expression = "cron(0 0 * * ? *)" # Diariamente à meia-noite UTC

  tags = {
    Environment = var.environment
    FinOps      = "FreeTier"
  }
}

# Definir o alvo da regra (Função Lambda)
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_etl_trigger.name
  target_id = "TriggerLambdaETL"
  arn       = aws_lambda_function.etl_lambda.arn
}

# Permissão para o EventBridge invocar a Função Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.etl_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_etl_trigger.arn
}
