# Package source code

data "archive_file" "worker_zip" {
  type        = "zip"
  source_file = "${path.module}/src/lambda_tlc_worker.py"
  output_path = "${path.module}/.build/worker.zip"
}

data "archive_file" "orchestrator_zip" {
  type        = "zip"
  source_file = "${path.module}/src/lambda_tlc_orchestrator.py"
  output_path = "${path.module}/.build/orchestrator.zip"
}

# CloudWatch Log Groups

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/tlc-${var.environment}-worker"
  retention_in_days = var.log_retention_days
}

resource "aws_cloudwatch_log_group" "orchestrator" {
  name              = "/aws/lambda/tlc-${var.environment}-orchestrator"
  retention_in_days = var.log_retention_days
}

# Lambda Worker

resource "aws_lambda_function" "worker" {
  function_name = "tlc-${var.environment}-worker"
  description   = "Download TLC NYC file and stream to S3 via multipart upload"

  filename         = data.archive_file.worker_zip.output_path
  source_code_hash = data.archive_file.worker_zip.output_base64sha256

  runtime       = var.python_runtime
  architectures = [var.lambda_architecture]
  handler       = "lambda_tlc_worker.lambda_handler"
  role          = aws_iam_role.worker.arn

  memory_size = var.worker_memory_mb
  timeout     = var.worker_timeout_seconds

  layers = [aws_lambda_layer_version.requests.arn]

  environment {
    variables = {
      TARGET_BUCKET           = var.target_bucket
      TARGET_PREFIX           = var.target_prefix
      HTTP_TIMEOUT            = "60"
      MULTIPART_THRESHOLD_MB  = "50"
      LOG_LEVEL               = "INFO"
    }
  }

  ephemeral_storage {
    size = 512
  }

  depends_on = [
    aws_cloudwatch_log_group.worker,
    aws_iam_role_policy_attachment.worker,
  ]
}

# Lambda Orchestrator

resource "aws_lambda_function" "orchestrator" {
  function_name = "tlc-${var.environment}-orchestrator"
  description   = "Dispatch N worker invocations (one per file)"

  filename         = data.archive_file.orchestrator_zip.output_path
  source_code_hash = data.archive_file.orchestrator_zip.output_base64sha256

  runtime       = var.python_runtime
  architectures = [var.lambda_architecture]
  handler       = "lambda_tlc_orchestrator.lambda_handler"
  role          = aws_iam_role.orchestrator.arn

  memory_size = 256
  timeout     = 60

  environment {
    variables = {
      WORKER_FUNCTION_NAME = aws_lambda_function.worker.function_name
      INVOKE_MODE          = "async"
      LOG_LEVEL            = "INFO"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.orchestrator,
    aws_iam_role_policy_attachment.orchestrator,
  ]
}
