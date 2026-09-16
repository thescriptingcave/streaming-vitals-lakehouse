# EventBridge Scheduler (Floci-supported target: Lambda). Daily 02:00 UTC L4
# run -> DDB advisory lock + Iceberg maintenance + ML export.
resource "aws_scheduler_schedule" "daily_iceberg" {
  name                = "daily-iceberg-maintenance"
  group_name          = "default"
  schedule_expression = var.schedule_expression
  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = var.l4_function_arn
    role_arn = "arn:aws:iam::000000000000:role/scheduler"

    input = jsonencode({
      detail = { run_date = "{{ today }}" }
    })
  }
}