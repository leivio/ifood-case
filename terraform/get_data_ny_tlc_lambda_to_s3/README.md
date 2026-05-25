## O que você precisa ter instalado

- Terraform >= 1.5
- AWS CLI configurado (com perfil ou variáveis de ambiente)
- Python 3.12 + pip
- Um bucket S3 já existente (`datalake-geral-trusted`)

## Fazendo o deploy

```bash
cp terraform.tfvars.example terraform.tfvars
# Ajuste o terraform.tfvars conforme seu ambiente

terraform init
terraform plan
terraform apply
```

O `apply` vai:
1. Instalar a biblioteca `requests` no diretório `layer/python/`
2. Empacotar o layer e as funções Lambda
3. Criar as roles e policies IAM necessárias
4. Subir as Lambdas com o layer anexado
5. Criar os log groups no CloudWatch
6. Criar o agendamento no EventBridge ou a Step Function (opcional, conforme configuração)

## Rodando o pipeline manualmente

Depois do deploy, o output já traz o comando pronto. Mas basicamente é esse aqui:

```bash
aws lambda invoke \
  --function-name tlc-prd-orchestrator \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  --region us-east-1 response.json

cat response.json
```

## Acompanhando a execução

Para ver os logs em tempo real:

```bash
aws logs tail /aws/lambda/tlc-prd-worker --follow --region us-east-1
```

## Baixando meses específicos

Por padrão o orquestrador decide o que baixar, mas dá pra passar o período manualmente:

```bash
aws lambda invoke \
  --function-name tlc-prd-orchestrator \
  --payload '{"year": 2024, "months": [1,2,3,4,5,6,7,8,9,10,11,12], "datasets": ["yellow_tripdata"]}' \
  --cli-binary-format raw-in-base64-out \
  response.json
```

## Ativando o agendamento mensal

Se quiser que o pipeline rode sozinho todo mês, adicione isso no `terraform.tfvars`:

```hcl
schedule_expression = "cron(0 6 5 * ? *)"
```

Depois é só rodar `terraform apply` de novo.

## Ativando a Step Function

A Step Function adiciona retry com backoff e controle de concorrência — útil se você estiver baixando muitos arquivos de uma vez.

```hcl
enable_step_function = true
```

Para disparar:

```bash
aws stepfunctions start-execution \
  --state-machine-arn <output:step_function_arn> \
  --input '{}'
```

## Atualizando o código das Lambdas

1. Edite `src/lambda_tlc_worker.py` ou `src/lambda_tlc_orchestrator.py`
2. Rode `terraform apply`

## Atualizando as dependências

1. Edite `layer/requirements.txt`
2. Rode `terraform apply`

## Destruindo tudo

```bash
terraform destroy
```