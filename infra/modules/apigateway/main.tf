# API Gateway v2 (HTTP) for the patients read API. Floci pins the API id via the
# manifest tag floci:override-id -> `patients` so CI/curl can predict hostnames
# ({id}.execute-api.localhost.floci.io:4566).
resource "aws_apigatewayv2_api" "patients" {
  name          = var.patients_api_name
  protocol_type = "HTTP"

  tags = { "floci:override-id" = var.api_override_id }
}

resource "aws_apigatewayv2_integration" "l3" {
  api_id                 = aws_apigatewayv2_api.patients.id
  integration_type       = "AWS_PROXY"
  integration_uri        = var.l3_function_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "latest_vitals" {
  api_id    = aws_apigatewayv2_api.patients.id
  route_key = "GET /patients/{id}/latest-vitals"
  target    = "integrations/${aws_apigatewayv2_integration.l3.id}"
}

resource "aws_apigatewayv2_route" "alerts" {
  api_id    = aws_apigatewayv2_api.patients.id
  route_key = "GET /patients/{id}/alerts"
  target    = "integrations/${aws_apigatewayv2_integration.l3.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.patients.id
  name        = "$default"
  auto_deploy = true
}

output "api" {
  value = aws_apigatewayv2_api.patients
}

output "api_endpoint" {
  value = "${aws_apigatewayv2_api.patients.api_endpoint}/patients"
}