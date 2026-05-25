aws_region    = "us-east-1"
environment   = "prd"
owner         = "leivio"

target_bucket = "datalake-geral-trusted"
target_prefix = "landing/prd"

worker_memory_mb       = 1024
worker_timeout_seconds = 900
lambda_architecture    = "arm64"  # 20% mais barato; mude para "x86_64" se preferir
python_runtime         = "python3.12"

log_retention_days = 7

# Para rodar uma vez por mês todo dia 5 às 06h UTC, descomente:
# schedule_expression = "cron(0 6 5 * ? *)"
# schedule_expression = ""

# Step Function só faz sentido se quiser retry orquestrado e paralelismo controlado
# enable_step_function = false
