resource "google_sql_database_instance" "instance" {
  provider = google

  name             = var.kestra_db_instance_name
  region           =  var.region
  database_version = "POSTGRES_17"

  deletion_protection = false

  depends_on = [google_service_networking_connection.db_private_vpc_connection]

  settings {
    // activation_policy = "NEVER" // uncomment to stop instance
    edition = "ENTERPRISE"
    tier = "db-f1-micro"
    ip_configuration {
      ipv4_enabled                                  = false
      private_network                               = data.google_compute_network.default.self_link
      enable_private_path_for_google_cloud_services = true
    }
  }
}

