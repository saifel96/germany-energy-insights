terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = ">= 5.0.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

resource "google_storage_bucket" "raw_data" {
  name     = "${var.project}-raw-data"
  location = var.region
  force_destroy = true 
}
resource "google_storage_bucket" "processed-data" {
  name     = "${var.project}-processed-data"
  location = var.region
  force_destroy = true 
}

resource "google_bigquery_dataset" "smard_dataset" {
  dataset_id = "smard_data"
  location   = var.region
}
