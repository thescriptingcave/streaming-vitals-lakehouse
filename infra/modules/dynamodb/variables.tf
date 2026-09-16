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

variable "billing_mode" {
  type    = string
  default = "PAY_PER_REQUEST"
}