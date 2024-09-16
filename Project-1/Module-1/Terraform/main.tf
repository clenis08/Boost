terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.41.0"
    }
  }
}

provider "google" {
  # Configuration options
  project     = "terraform-project-435522"
  region      = "us-central1"
}

resource "google_storage_bucket" "z2h-bucket" {
  name                        = "terra-buecket-terraform-project-435522"
  location                    = "US"
  force_destroy               = true
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }
    condition {
      age = 1
    }
  }

  versioning {
    enabled = true
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = "demo_dataset"
}