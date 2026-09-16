# Floci emulates the Glue Data Catalog metadata service (CreateDatabase/GetDatabase
# are present) but NOT table/partition mutation (UpdateTable, GetPartition,
# BatchCreate/BatchUpdatePartition missing — see docs/FOCI_VERIFICATION.md).
# Trino's Iceberg `glue` catalog needs the metadata_location + snapshot commit,
# which relies on UpdateTable — so real Glue operational parity is unproven.
#
# SPIKE (M1/M2, milestone docs/TESTING.md M1): attempt table metadata via the
# Trino iceberg-glue properties with CFN/ALTER and confirm CREATE→INSERT rounds.
# Until proven, iceberg_catalog defaults to "nessie" and this module is inert.

locals {
  enabled = var.glue_enabled
}

# Kept for symmetry with the nessie path; creating the Glue database object
# today is harmless (CreateDatabase emulated) but the catalog default remains nessie.
resource "aws_glue_catalog_database" "healthcare" {
  count = local.enabled ? 1 : 0
  name  = var.catalog_database
}