# Floci verifies Firehose with Lambda ProcessingConfiguration (Ok/Dropped/
# ProcessedFailed + .failures prefix) and BufferingHints.IntervalInSeconds.
resource "aws_kinesis_firehose_delivery_stream" "vitals_raw" {
  name        = var.delivery_stream_name
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = var.kinesis_stream_arn
    role_arn           = "arn:aws:iam::000000000000:role/firehose"
  }

  extended_s3_configuration {
    role_arn            = "arn:aws:iam::000000000000:role/firehose"
    bucket_arn          = var.bucket_arn
    prefix              = "raw/vitals/"
    error_output_prefix = "raw/failures/"
    compression_format  = "GZIP"
    buffering_size      = var.buffer_size_mbs
    buffering_interval  = var.buffer_interval_seconds

    dynamic "processing_configuration" {
      for_each = var.lambda_transform_arn != "" ? { on = true } : {}
      content {
        enabled = true
        processors {
          type = "Lambda"
          parameters {
            parameter_name  = "LambdaArn"
            parameter_value = var.lambda_transform_arn
          }
          parameters {
            parameter_name  = "BufferSizeInMBs"
            parameter_value = tostring(var.buffer_size_mbs)
          }
          parameters {
            parameter_name  = "BufferIntervalInSeconds"
            parameter_value = tostring(var.buffer_interval_seconds)
          }
        }
      }
    }
  }
}