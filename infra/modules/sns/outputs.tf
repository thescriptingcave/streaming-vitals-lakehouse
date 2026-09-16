output "topic" {
  value = aws_sns_topic.alerts
}

output "arn" {
  value = aws_sns_topic.alerts.arn
}