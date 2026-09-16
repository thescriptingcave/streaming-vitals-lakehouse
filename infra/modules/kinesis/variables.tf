variable "stream_name" {
  type    = string
  default = "vitals"
}

variable "shard_count" {
  type    = number
  default = 1
}

variable "retention_hours" {
  type    = number
  default = 24
}

variable "stream_mode" {
  type        = string
  default     = "ON_DEMAND"
  description = "PROVISIONED (requires shard_count) or ON_DEMAND."
  validation {
    condition     = contains(["PROVISIONED", "ON_DEMAND"], var.stream_mode)
    error_message = "stream_mode must be one of: PROVISIONED, ON_DEMAND."
  }
}