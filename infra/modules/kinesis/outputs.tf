output "stream" {
  value = aws_kinesis_stream.vitals
}

output "name" {
  value = aws_kinesis_stream.vitals.name
}

output "arn" {
  value = aws_kinesis_stream.vitals.arn
}