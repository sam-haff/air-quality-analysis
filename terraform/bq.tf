resource "google_bigquery_dataset" "data-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
  delete_contents_on_destroy = true
}