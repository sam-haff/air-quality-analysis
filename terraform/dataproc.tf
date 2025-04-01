# TODO: change


/*resource "google_dataproc_cluster" "mycluster" {
  name     = "mycluster"
  region   = "us-central1"
  graceful_decommission_timeout = "120s"
  #labels = {
  #  foo = "bar"
  #}

  cluster_config {
    staging_bucket = "dataproc-staging-bucket"

    master_config {
      num_instances = 1
      machine_type  = "e2-medium"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances    = 1
      machine_type     = "e2-medium"
      min_cpu_platform = "Intel Skylake"
    }

    preemptible_worker_config {
      num_instances = 0
    }

    # Override or set some custom properties
    software_config {
      image_version = "2.2.51-debian12"#"2.2-debian12""2.0.35-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }

    gce_cluster_config {
      # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
      service_account = google_service_account.default.email
      service_account_scopes = [
        "cloud-platform"
      ]
    }

    # You can define multiple initialization_action blocks
    initialization_action {
      script      = "gs://dataproc-initialization-actions/stackdriver/stackdriver.sh"
      timeout_sec = 500
    }
  }
}*/


resource "google_dataproc_cluster" "cl" {
  name     = "cluster-a"
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
      image_version = "2.2.51-debian12"#"2.2-debian12"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER", "DELTA"]
    }
    endpoint_config {
      enable_http_port_access = true
    }
    gce_cluster_config {
      #network = "https://www.googleapis.com/compute/v1/projects/kestra-de/global/networks/default"#"default"
      #subnetwork = "https://www.googleapis.com/compute/v1/projects/kestra-de/regions/us-central1/subnetworks/default"
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