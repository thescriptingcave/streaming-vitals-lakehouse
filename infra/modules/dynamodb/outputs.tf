output "alerts_table" {
  value = aws_dynamodb_table.alerts
}

output "alerts_stream_arn" {
  value = aws_dynamodb_table.alerts.stream_arn
}

output "latest_vitals_table" {
  value = aws_dynamodb_table.latest_vitals
}

output "lock_table" {
  value = aws_dynamodb_table.maintenance_lock
}