variable "project" {
  description = "Project"
  type        = string
  default     = "de-zoomcamp-493207"
}

variable "region" {
  description = "Region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  type        = string
  default     = "US"
}

variable "gcp_credentials_file" {
  description = "My Credentials File"
  type        = string
  default     = "/home/hadoop/project-de-zoomcamp/.google/credentials/google_credentials.json"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset for stream analytics"
  type        = string
  default     = "healthcare_data_warehouse"
}

variable "gcs_data_lake_name" {
  description = "GCS bucket for storing the healthcare vitals stream"
  type        = string
  default     = "de-zoomcamp-493207-healthcare-lake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}
