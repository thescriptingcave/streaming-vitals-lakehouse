variable "delivery_stream_name" {
  type    = string
  default = "vitals-raw"
}

variable "bucket_arn" {
  type = string
}

variable "kinesis_stream_arn" {
  type = string
}

variable "lambda_transform_arn" {
  type    = string
  default = ""
}

variable "buffer_interval_seconds" {
  type    = number
  default = 60
}

variable "buffer_size_mbs" {
  type    = number
  default = 5
}