output "delivery_stream" {
  value = aws_kinesis_firehose_delivery_stream.vitals_raw
}

output "arn" {
  value = aws_kinesis_firehose_delivery_stream.vitals_raw.arn
}