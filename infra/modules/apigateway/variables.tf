variable "patients_api_name" {
  type    = string
  default = "patients"
}

variable "api_override_id" {
  type    = string
  default = "patients"
}

variable "l3_function_arn" {
  type = string
}

variable "l3_function_name" {
  type = string
}

variable "vpc_lambda_default" {
  type    = bool
  default = true
}