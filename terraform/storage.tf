
resource "google_storage_bucket" "data-bucket" {
  name          = var.gcs_storage_name
  location      = var.location
  force_destroy = true
  storage_class = var.gcs_storage_class

  uniform_bucket_level_access = true
}
resource "google_storage_bucket" "kestra-storage-bucket" {
  name          = var.gcs_kestra_storage_name
  location      = var.location
  force_destroy = true
  storage_class = var.gcs_kestra_storage_class

  uniform_bucket_level_access = true
}