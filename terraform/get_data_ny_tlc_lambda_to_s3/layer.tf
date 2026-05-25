# =============================================================================
# Layer "requests" — instalada via pip durante o terraform apply
# =============================================================================

resource "null_resource" "build_requests_layer" {
  # Re-executa se o requirements mudar
  triggers = {
    requirements = filemd5("${path.module}/layer/requirements.txt")
    architecture = var.lambda_architecture
    runtime      = var.python_runtime
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      cd ${path.module}/layer
      rm -rf python && mkdir -p python
      pip install \
        --target ./python \
        --platform ${var.lambda_architecture == "arm64" ? "manylinux2014_aarch64" : "manylinux2014_x86_64"} \
        --python-version 3.12 \
        --only-binary=:all: \
        --upgrade \
        -r requirements.txt
    EOT
  }
}

data "archive_file" "requests_layer" {
  type        = "zip"
  source_dir  = "${path.module}/layer"
  output_path = "${path.module}/.build/requests-layer.zip"
  excludes    = ["requirements.txt"]

  depends_on = [null_resource.build_requests_layer]
}

resource "aws_lambda_layer_version" "requests" {
  layer_name          = "tlc-${var.environment}-requests"
  description         = "requests lib para downloads HTTP"
  filename            = data.archive_file.requests_layer.output_path
  source_code_hash    = data.archive_file.requests_layer.output_base64sha256
  compatible_runtimes = [var.python_runtime]

  compatible_architectures = [var.lambda_architecture]
}
