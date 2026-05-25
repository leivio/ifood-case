output "worker_function_name" {
  value       = aws_lambda_function.worker.function_name
  description = "Nome da Lambda worker"
}

output "worker_function_arn" {
  value       = aws_lambda_function.worker.arn
  description = "ARN da Lambda worker"
}

output "orchestrator_function_name" {
  value       = aws_lambda_function.orchestrator.function_name
  description = "Nome da Lambda orchestrator"
}

output "orchestrator_function_arn" {
  value       = aws_lambda_function.orchestrator.arn
  description = "ARN da Lambda orchestrator"
}

output "requests_layer_arn" {
  value       = aws_lambda_layer_version.requests.arn
  description = "ARN da Layer requests"
}

output "step_function_arn" {
  value       = var.enable_step_function ? aws_sfn_state_machine.tlc_pipeline[0].arn : null
  description = "ARN da Step Function (se habilitada)"
}

output "invoke_orchestrator_cli" {
  value       = "aws lambda invoke --function-name ${aws_lambda_function.orchestrator.function_name} --payload '{}' --cli-binary-format raw-in-base64-out --region ${var.aws_region} response.json"
  description = "Comando pronto para disparar o pipeline"
}

output "tail_logs_cli" {
  value       = "aws logs tail /aws/lambda/${aws_lambda_function.worker.function_name} --follow --region ${var.aws_region}"
  description = "Comando para acompanhar os logs em tempo real"
}
