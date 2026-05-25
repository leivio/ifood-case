# =============================================================================
# Step Functions (opcional, controlado por var.enable_step_function)
# Usa Map state para distribuir downloads em paralelo com retry exponencial
# =============================================================================

data "aws_iam_policy_document" "sfn_trust" {
  count = var.enable_step_function ? 1 : 0

  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn" {
  count              = var.enable_step_function ? 1 : 0
  name               = "tlc-${var.environment}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust[0].json
}

data "aws_iam_policy_document" "sfn_permissions" {
  count = var.enable_step_function ? 1 : 0

  statement {
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.worker.arn,
      aws_lambda_function.orchestrator.arn,
    ]
  }

  statement {
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "sfn" {
  count  = var.enable_step_function ? 1 : 0
  name   = "tlc-${var.environment}-sfn-policy"
  policy = data.aws_iam_policy_document.sfn_permissions[0].json
}

resource "aws_iam_role_policy_attachment" "sfn" {
  count      = var.enable_step_function ? 1 : 0
  role       = aws_iam_role.sfn[0].name
  policy_arn = aws_iam_policy.sfn[0].arn
}

resource "aws_sfn_state_machine" "tlc_pipeline" {
  count    = var.enable_step_function ? 1 : 0
  name     = "tlc-${var.environment}-pipeline"
  role_arn = aws_iam_role.sfn[0].arn

  definition = jsonencode({
    Comment = "Pipeline TLC NYC com Map state e retry"
    StartAt = "BuildTasks"
    States = {
      BuildTasks = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.orchestrator.arn
          Payload = {
            datasets  = ["yellow_tripdata", "green_tripdata", "fhv_tripdata", "fhvhv_tripdata"]
            year      = 2023
            months    = [1, 2, 3, 4, 5]
            overwrite = true
          }
        }
        # Sobrescreve o INVOKE_MODE temporariamente
        ResultSelector = {
          "tasks.$" = "$.Payload.tasks"
        }
        Next = "DownloadInParallel"
      }
      DownloadInParallel = {
        Type           = "Map"
        ItemsPath      = "$.tasks"
        MaxConcurrency = 5
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "DownloadOne"
          States = {
            DownloadOne = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Parameters = {
                FunctionName = aws_lambda_function.worker.arn
                "Payload.$"  = "$"
              }
              Retry = [
                {
                  ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "States.TaskFailed"]
                  IntervalSeconds = 5
                  MaxAttempts     = 3
                  BackoffRate     = 2.0
                }
              ]
              End = true
            }
          }
        }
        End = true
      }
    }
  })
}
