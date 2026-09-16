resource "aws_kinesis_stream" "vitals" {
  name             = var.stream_name
  shard_count      = var.stream_mode == "ON_DEMAND" ? null : var.shard_count
  retention_period = var.retention_hours
  stream_mode_details {
    stream_mode = var.stream_mode
  }
}