# Healthcare Realtime Vitals Lakehouse — Floci-backed infrastructure.
# Composition of the modules defined under infra/modules/.

locals {
  # path.module is only resolvable in expressions (not variable defaults), so
  # the Lambda zip directory is derived here and overridable via TF_VAR.
  lambda_build_dir = (
    var.lambda_build_dir != ""
    ? var.lambda_build_dir
    : abspath("${path.module}/../lambdas/_build")
  )
}

module "s3_lake" {
  source      = "./modules/s3_lake"
  bucket_name = var.data_bucket_name
}

module "glue_catalog" {
  source = "./modules/glue_catalog"
  # glue_enabled = true  # only after the M1/M2 Floci spike (see module notes)
}

module "kinesis" {
  source      = "./modules/kinesis"
  stream_name = var.vitals_stream_name
}

module "dynamodb" {
  source = "./modules/dynamodb"
}

module "sns" {
  source = "./modules/sns"
}

# Lambdas in alphabetical order; each zip lands at lambdas/_build/<name>.zip
# from `make build-lambdas` (scripts/build_lambdas.py).
module "lambda" {
  source           = "./modules/lambda"
  runtime          = var.lambda_runtime
  build_dir        = local.lambda_build_dir
  aws_endpoint_url = var.floci_endpoint
  alert_topic_arn  = module.sns.arn

  functions = {
    l1_alert_notifier = {
      handler = "lambdas.l1_alert_notifier.lambda_function.lambda_handler"
      env     = { ALERTS_TABLE = "alerts", ALERT_TOPIC_ARN = module.sns.arn }
    }
    l2_firehose_transform = {
      handler = "lambdas.l2_firehose_transform.lambda_function.lambda_handler"
      env     = {}
    }
    l3_patients_api = {
      handler = "lambdas.l3_patients_api.lambda_function.lambda_handler"
      env     = { LATEST_VITALS_TABLE = "latest_vitals", ALERTS_TABLE = "alerts" }
    }
    l4_scheduled_iceberg = {
      handler = "lambdas.l4_scheduled_iceberg.lambda_function.lambda_handler"
      env     = { LOCK_TABLE = "maintenance-lock" }
    }
  }
}

module "firehose" {
  source               = "./modules/firehose"
  bucket_arn           = module.s3_lake.arn
  kinesis_stream_arn   = module.kinesis.arn
  lambda_transform_arn = module.lambda.functions["l2_firehose_transform"].arn
  depends_on           = [module.lambda]
}

module "apigateway" {
  source           = "./modules/apigateway"
  l3_function_arn  = module.lambda.functions["l3_patients_api"].arn
  l3_function_name = "l3_patients_api"
  depends_on       = [module.lambda]
}

module "events" {
  source           = "./modules/events"
  l4_function_arn  = module.lambda.functions["l4_scheduled_iceberg"].arn
  l4_function_name = "l4_scheduled_iceberg"
  depends_on       = [module.lambda]
}

# L1 consumes the alerts DDB stream (Firehose/Lambda/SNS all wired above).
resource "aws_lambda_event_source_mapping" "alerts_stream" {
  event_source_arn  = module.dynamodb.alerts_stream_arn
  function_name     = module.lambda.functions["l1_alert_notifier"].function_name
  starting_position = "TRIM_HORIZON"
  batch_size        = 100
}

output "api_endpoint" {
  value = module.apigateway.api_endpoint
}

output "stream_name" {
  value = module.kinesis.name
}

output "delivery_stream_arn" {
  value = module.firehose.arn
}

output "lake_bucket" {
  value = module.s3_lake.bucket.bucket
}

output "dynamodb_tables" {
  value = {
    alerts           = module.dynamodb.alerts_table
    latest_vitals    = module.dynamodb.latest_vitals_table
    maintenance_lock = module.dynamodb.lock_table
  }
}