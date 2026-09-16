# Generic zip-packaged Lambda deployer. The ZIPs are pre-built by
# scripts/build_lambdas.py (`make build-lambdas`) so that each archive already
# bundles lambdas/common + shared packages (lambdas/common, producer, ml).
locals {
  env = {
    AWS_ENDPOINT_URL    = var.aws_endpoint_url
    LAMBDA_RUNTIME      = var.runtime
    ALERTS_TABLE        = var.alerts_table
    LATEST_VITALS_TABLE = var.latest_vitals_table
    LOCK_TABLE          = var.lock_table
    ALERT_TOPIC_ARN     = var.alert_topic_arn
    ML_BUCKET           = var.ml_bucket
  }
}

resource "aws_lambda_function" "this" {
  for_each      = var.functions
  function_name = each.key
  filename      = "${var.build_dir}/${each.key}.zip"
  role          = "arn:aws:iam::000000000000:role/lambda"
  handler       = each.value.handler
  runtime       = var.runtime

  timeout     = each.value.timeout
  memory_size = each.value.memory_size

  environment {
    variables = merge(local.env, each.value.env)
  }
}

# Real-AWS parity: the API Gateway / DDB stream principal needs explicit
# permission. Floci ignores IAM, but keeping these resources documents what
# deployments to real AWS will require.
resource "aws_lambda_permission" "apigateway" {
  for_each      = var.functions
  statement_id  = "AllowExecutionFromAPIGateway-${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this[each.key].function_name
  principal     = "apigateway.amazonaws.com"
}