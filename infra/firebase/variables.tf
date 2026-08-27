variable "project_id" {
  description = "Firebase and Cloud Run project."
  type        = string
  default     = "valtion-budjetti-data"
}

variable "data_project_id" {
  description = "Project containing the production BigQuery semantic layer."
  type        = string
  default     = "budjettihaukka-gpt"
}

variable "data_dataset_id" {
  description = "BigQuery dataset containing Budjettihaukka's semantic layer."
  type        = string
  default     = "valtiodata"
}

variable "region" {
  description = "Cloud Run, Artifact Registry, and Firestore region."
  type        = string
  default     = "europe-west1"
}

variable "hosting_site_id" {
  description = "Existing Firebase Hosting site ID."
  type        = string
  default     = "valtion-budjetti-data"
}

variable "api_service_name" {
  description = "Dedicated Cloud Run service for the analytics API."
  type        = string
  default     = "budjettihaukka-api"
}

variable "api_image" {
  description = "Immutable Artifact Registry image reference, preferably with a sha256 digest."
  type        = string
  default     = ""
}

variable "deploy_api" {
  description = "Create or update Cloud Run after an image has been built."
  type        = bool
  default     = false

  validation {
    condition     = !var.deploy_api || length(var.api_image) > 0
    error_message = "api_image must be set when deploy_api is true."
  }
}

variable "max_instances" {
  description = "Hard Cloud Run scale ceiling for cost control."
  type        = number
  default     = 2
}
