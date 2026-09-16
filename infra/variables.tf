variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_account_id" {
  type    = string
  default = "000000000000"
}

variable "floci_endpoint" {
  type    = string
  default = "http://localhost:4566"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "data_bucket_name" {
  type    = string
  default = "healthcare-lake"
}

variable "vitals_stream_name" {
  type    = string
  default = "vitals"
}

# Catalog the Trino Iceberg connector talks to (docs/FOCI_VERIFICATION.md).
# Glue requires a Floci spike (UpdateTable/GetPartition gaps); Nessie is the
# proven default path.
variable "iceberg_catalog" {
  type    = string
  default = "nessie"
  validation {
    condition     = contains(["nessie", "glue"], var.iceberg_catalog)
    error_message = "iceberg_catalog must be one of: nessie, glue."
  }
}

variable "lambda_runtime" {
  type    = string
  default = "python3.12"
}

variable "lambda_build_dir" {
  type        = string
  default     = ""
  description = "Override for the prebuilt Lambda zip directory; empty uses <repo>/lambdas/_build."
}

variable "common_tags" {
  type = map(string)
  default = {
    project     = "healthcare-realtime-vitals-lakehouse"
    environment = "dev"
    managed_by  = "opentofu"
  }
}