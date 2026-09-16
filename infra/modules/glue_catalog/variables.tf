variable "catalog_database" {
  type    = string
  default = "healthcare"
}

variable "glue_enabled" {
  description = "Floci Glue lacks UpdateTable/GetPartition/Batch* (spike). Keep disabled until M1/M2 proves Trino glue-commit."
  type        = bool
  default     = false
}