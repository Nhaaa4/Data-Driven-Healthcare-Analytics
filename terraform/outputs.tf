output "data_lake_bucket_name" {
  description = "GCS bucket that stores the healthcare vitals stream"
  value       = google_storage_bucket.data_lake.name
}

output "data_lake" {
  description = "Partitioned Parquet landing path for the healthcare vitals stream"
  value       = "gs://${google_storage_bucket.data_lake.name}/"
}

output "bigquery_dataset_id" {
  description = "BigQuery dataset for downstream analytics"
  value       = google_bigquery_dataset.healthcare_data_warehouse.dataset_id
}
