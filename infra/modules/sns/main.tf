resource "aws_sns_topic" "alerts" {
  name = var.topic_name
}