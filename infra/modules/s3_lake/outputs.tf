output "bucket" {
  value = aws_s3_bucket.lake
}

output "arn" {
  value = aws_s3_bucket.lake.arn
}