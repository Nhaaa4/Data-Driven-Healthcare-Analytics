variable "project" {
  description = "Target GCP project ID."
  type        = string
  default     = "terraform-learning-486607"
}

variable "region" {
  description = "Region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Location."
  type        = string
  default     = "US"
}

variable "gcp_credentials_file" {
  description = "My Credentials File"
  type        = string
  default     = "./keys/de-zoomcamp-493207-d3f7354bdd98.json"
}
