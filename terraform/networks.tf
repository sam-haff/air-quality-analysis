data "google_compute_network" "default" {
    name = "default"
}
data "google_compute_subnetwork" "default" {
    name = "default"
}

resource "google_compute_global_address" "db_private_ip_address" {
  provider = google

  name          = "db-private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = data.google_compute_network.default.id
}

resource "google_service_networking_connection" "db_private_vpc_connection" {
  provider = google

  network                 = data.google_compute_network.default.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.db_private_ip_address.name]
  update_on_creation_fail = true
}