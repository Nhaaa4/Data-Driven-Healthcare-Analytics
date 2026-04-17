terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = var.gcp_credentials_file != "" ? file(var.gcp_credentials_file) : null
  project     = var.project
  region      = var.region
}
