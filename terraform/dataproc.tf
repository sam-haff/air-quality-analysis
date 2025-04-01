# TODO: change

resource "google_dataproc_cluster" "cl" {
  name     = "cluster"
  project = var.project
  region = var.region
  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "e2-medium"
      #machine_type  = "c4-standard-2"#n4-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
        num_local_ssds = 0
      }
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.2-debian12"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER", "DELTA"]
    }
    endpoint_config {
      enable_http_port_access = true
    }
    gce_cluster_config {
        zone = var.zone
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = "zoomcamp-2@kestra-de.iam.gserviceaccount.com"
      service_account_scopes = [
        "cloud-platform"
      ]
      shielded_instance_config {
        enable_secure_boot = false
        enable_vtpm = false
        enable_integrity_monitoring = false
      }
      internal_ip_only = true
    }
  }
}