variable "credentials_file" {
  description = "Path To GCP Creds"
  default     = "./gcp/creds.json"
}

variable "project" {
  description = "Project Name"
  default     = "kestra-de"
}

variable "region" {
  description = "Project Region"
  default     = "us-central1"
}
variable "zone" {
  description = "Project Zone"
  default     = "us-central1-c"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "bq_dataset"
}

variable "gcs_storage_name" {
  description = "Bucket Name"
  default     = "kestra-de-main-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Sorage Class"
  default     = "STANDARD"
}

variable "gcs_kestra_storage_name" {
  description = "Bucket Name"
  default     = "kestra-service-bucket"
}

variable "gcs_kestra_storage_class" {
  description = "Bucket Sorage Class"
  default     = "STANDARD"
}

