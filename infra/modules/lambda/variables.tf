variable "runtime" {
  type    = string
  default = "python3.12"
}

variable "build_dir" {
  type = string
}

variable "aws_endpoint_url" {
  type    = string
  default = "http://localhost:4566"
}

variable "alerts_table" {
  type    = string
  default = "alerts"
}

variable "latest_vitals_table" {
  type    = string
  default = "latest_vitals"
}

variable "lock_table" {
  type    = string
  default = "maintenance-lock"
}

variable "alert_topic_arn" {
  type    = string
  default = ""
}

variable "ml_bucket" {
  type    = string
  default = "healthcare-lake"
}

# { name -> { handler, env?, timeout?, memory_size? } } — filenames are expected
# to already exist under var.build_dir (run `make build-lambdas` first).
# Handlers are dotted paths into the zip: "lambdas.<name>.lambda_function.lambda_handler".
variable "functions" {
  type = map(object({
    handler     = string
    env         = optional(map(string), {})
    timeout     = optional(number, 60)
    memory_size = optional(number, 128)
  }))
  default = {}
}