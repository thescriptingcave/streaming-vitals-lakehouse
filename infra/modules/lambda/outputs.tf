output "functions" {
  value = aws_lambda_function.this
}

output "arns" {
  value = { for k, fn in aws_lambda_function.this : k => fn.arn }
}