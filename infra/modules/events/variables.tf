variable "schedule_expression" {
  type    = string
  default = "cron(0 2 * * ? *)"
}

variable "l4_function_arn" {
  type = string
}

variable "l4_function_name" {
  type = string
}

variable "run_date_key" {
  type    = string
  default = "run_date"
}