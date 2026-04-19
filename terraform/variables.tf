variable "project" {
  description = "Die ID deines Google Cloud Projekts"
}

variable "region" {
  description = "Region für die Ressourcen"
  default     = "us-central1"
}

variable "zone" {
  description = "Spezifische Zone (z.B. für VMs)"
  default     = "us-central1-c"
}