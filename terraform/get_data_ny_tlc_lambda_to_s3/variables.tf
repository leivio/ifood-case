variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "us-east-1"
}

variable "environment" {
  type        = string
  description = "Environment (prd, uat, sit)"
  default     = "prd"
}

variable "owner" {
  type        = string
  description = "Resource owner"
  default     = "data-engineering"
}

variable "target_bucket" {
  type        = string
  description = "Target S3 bucket"
  default     = "datalake-geral-trusted"
}

variable "target_prefix" {
  type        = string
  description = "Prefix within bucket"
  default     = "landing/prd"
}

variable "worker_memory_mb" {
  type        = number
  description = "Worker Lambda memory in MB"
  default     = 1024

  validation {
    condition     = var.worker_memory_mb >= 128 && var.worker_memory_mb <= 10240
    error_message = "Memory must be between 128 and 10240 MB."
  }
}

variable "worker_timeout_seconds" {
  type        = number
  description = "Worker Lambda timeout in seconds (max 900)"
  default     = 900
}

variable "lambda_architecture" {
  type        = string
  description = "Architecture: x86_64 or arm64"
  default     = "arm64"

  validation {
    condition     = contains(["x86_64", "arm64"], var.lambda_architecture)
    error_message = "Architecture must be x86_64 or arm64."
  }
}

variable "python_runtime" {
  type        = string
  description = "Python runtime for Lambda"
  default     = "python3.12"
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch Logs retention in days"
  default     = 14
}

variable "schedule_expression" {
  type        = string
  description = "EventBridge cron expression (empty = no schedule)"
  default     = ""
}

variable "enable_step_function" {
  type        = bool
  description = "Create Step Function for orchestration with retry"
  default     = false
}
