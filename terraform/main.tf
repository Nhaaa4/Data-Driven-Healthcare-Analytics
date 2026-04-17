resource "google_storage_bucket" "data_lake" {
  name          = var.gcs_data_lake_name
  location      = var.location
  storage_class = var.gcs_storage_class
  force_destroy = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }

    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }

    action {
      type = "Delete"
    }
  }
}


resource "google_bigquery_dataset" "healthcare_data_warehouse" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}
