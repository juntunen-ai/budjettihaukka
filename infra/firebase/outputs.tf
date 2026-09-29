output "hosting_url" {
  value = google_firebase_hosting_site.default.default_url
}

output "artifact_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.api.repository_id}"
}

output "api_service_account" {
  value = google_service_account.api.email
}

output "api_url" {
  value = var.deploy_api ? google_cloud_run_v2_service.api[0].uri : null
}

output "admin_key" {
  description = "Enter this value in the UI admin view. Store it in a password manager."
  value       = random_password.admin_key.result
  sensitive   = true
}
