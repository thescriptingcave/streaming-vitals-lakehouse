terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

# Floci (local/emulated AWS) ignores credentials; dummy keys satisfy the SDK.
# CI runs against the same localhost endpoint on a runner host (see ci/).
provider "aws" {
  region     = var.aws_region
  access_key = "local"
  secret_key = "local"

  s3_use_path_style = true

  endpoints {
    apigatewayv2       = var.floci_endpoint
    dynamodb           = var.floci_endpoint
    firehose           = var.floci_endpoint
    glue               = var.floci_endpoint
    kinesis            = var.floci_endpoint
    kinesisanalyticsv2 = var.floci_endpoint
    lambda             = var.floci_endpoint
    s3                 = var.floci_endpoint
    scheduler          = var.floci_endpoint
    sns                = var.floci_endpoint
    sts                = var.floci_endpoint
  }

  default_tags {
    tags = var.common_tags
  }
}