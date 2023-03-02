locals {
  data_lake_bucket = "de_zoomcamp_data_lake"
}

variable "project" {
  description = "Your GPC project ID"
}

variable "region" {
  description = "Region for GPC resources."
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket."
  default = "STANDARD"
}

variable "BQ_DATASET" {  // equivalent to "schema"
  description = "BigQuery Dataset that raw data (from GCS) will be written to."
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
    description = "Name of the table to be used."
    type = string
    default = "ny_trips"
}

variable "TABLE_NAME_DBT" {
    description = "Name of the table to be used for dbt project."
    type = string
    default = "ny_trips_dbt"
}