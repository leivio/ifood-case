# =============================================================================
# EventBridge Schedule (opcional, controlado por var.schedule_expression)
# =============================================================================

resource "aws_cloudwatch_event_rule" "schedule" {
  count = var.schedule_expression != "" ? 1 : 0

  name                = "tlc-${var.environment}-schedule"
  description         = "Schedule para o pipeline TLC NYC"
  schedule_expression = var.schedule_expression
  state               = "ENABLED"
}

resource "aws_cloudwatch_event_target" "orchestrator" {
  count = var.schedule_expression != "" ? 1 : 0

  rule      = aws_cloudwatch_event_rule.schedule[0].name
  target_id = "tlc-orchestrator"
  arn       = aws_lambda_function.orchestrator.arn

  # Payload default — você pode customizar passando outro JSON aqui
  input = jsonencode({
    datasets  = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]
    year      = 2023
    months    = [1, 2, 3, 4, 5]
    overwrite = true
  })
}

resource "aws_lambda_permission" "eventbridge_invoke" {
  count = var.schedule_expression != "" ? 1 : 0

  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule[0].arn
}
