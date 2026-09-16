# Alerts read-model: Flink writes new alerts; L1 consumes the stream,
# publishes to SNS, and marks `sent` (idempotent delivery).
resource "aws_dynamodb_table" "alerts" {
  name         = var.alerts_table
  billing_mode = var.billing_mode
  hash_key     = "patient_id"
  range_key    = "alert_id"

  attribute {
    name = "patient_id"
    type = "S"
  }
  attribute {
    name = "alert_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"
}

# Latest-vitals read model served by L3 (GET /patients/{id}/latest-vitals).
resource "aws_dynamodb_table" "latest_vitals" {
  name         = var.latest_vitals_table
  billing_mode = var.billing_mode
  hash_key     = "patient_id"

  attribute {
    name = "patient_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}

# Advisory lock for the daily L4 Iceberg maintenance/export run.
resource "aws_dynamodb_table" "maintenance_lock" {
  name         = var.lock_table
  billing_mode = var.billing_mode
  hash_key     = "lock"

  attribute {
    name = "lock"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
}