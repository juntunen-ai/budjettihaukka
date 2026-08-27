locals {
  required_services = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "firestore.googleapis.com",
    "firebase.googleapis.com",
    "firebasehosting.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
  ])
}

resource "google_project_service" "required" {
  for_each = local.required_services

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_firebase_project" "default" {
  provider = google-beta
  project  = var.project_id

  depends_on = [google_project_service.required]

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = google_firebase_project.default
  id = var.project_id
}

resource "google_firebase_web_app" "frontend" {
  provider     = google-beta
  project      = var.project_id
  display_name = "Budjettihaukka web"

  depends_on = [google_firebase_project.default]

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_firebase_hosting_site" "default" {
  provider        = google-beta
  project         = var.project_id
  site_id         = var.hosting_site_id
  app_id          = google_firebase_web_app.frontend.app_id
  deletion_policy = "PREVENT"
}

import {
  to = google_firebase_hosting_site.default
  id = "projects/${var.project_id}/sites/${var.hosting_site_id}"
}

resource "google_firestore_database" "default" {
  project                           = var.project_id
  name                              = "(default)"
  location_id                       = var.region
  type                              = "FIRESTORE_NATIVE"
  delete_protection_state           = "DELETE_PROTECTION_ENABLED"
  deletion_policy                   = "ABANDON"
  point_in_time_recovery_enablement = "POINT_IN_TIME_RECOVERY_DISABLED"

  depends_on = [google_project_service.required]
}

resource "google_artifact_registry_repository" "api" {
  project       = var.project_id
  location      = var.region
  repository_id = "budjettihaukka"
  description   = "Budjettihaukka production containers"
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-recent-releases"
    action = "KEEP"

    most_recent_versions {
      keep_count = 10
    }
  }

  depends_on = [google_project_service.required]
}

resource "google_service_account" "api" {
  project      = var.project_id
  account_id   = "budjettihaukka-api"
  display_name = "Budjettihaukka API runtime"
  description  = "Least-privilege identity for the Cloud Run analytics API"
}

resource "google_project_iam_member" "api_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = google_service_account.api.member
}

resource "google_project_iam_member" "api_firestore_user" {
  project = var.project_id
  role    = "roles/datastore.user"
  member  = google_service_account.api.member
}

resource "google_project_iam_member" "api_bigquery_job_user" {
  project = var.data_project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.api.member
}

resource "google_bigquery_dataset_iam_member" "api_data_viewer" {
  project    = var.data_project_id
  dataset_id = var.data_dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.api.member
}

resource "random_password" "admin_key" {
  length  = 40
  special = false
}

resource "google_secret_manager_secret" "admin_key" {
  project   = var.project_id
  secret_id = "budjettihaukka-admin-key"

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_version" "admin_key" {
  secret      = google_secret_manager_secret.admin_key.id
  secret_data = random_password.admin_key.result
}

resource "google_secret_manager_secret_iam_member" "api_admin_key" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.admin_key.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.api.member
}

resource "google_cloud_run_v2_service" "api" {
  count = var.deploy_api ? 1 : 0

  project             = var.project_id
  name                = var.api_service_name
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = true

  scaling {
    min_instance_count = 0
    max_instance_count = var.max_instances
  }

  template {
    service_account = google_service_account.api.email
    timeout         = "60s"

    scaling {
      min_instance_count = 0
      max_instance_count = var.max_instances
    }

    containers {
      image = var.api_image

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      env {
        name  = "BUDJETTIHAUKKA_PROJECT_ID"
        value = var.data_project_id
      }
      env {
        name  = "BUDJETTIHAUKKA_RUNTIME_PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "BUDJETTIHAUKKA_DATASET"
        value = var.data_dataset_id
      }
      env {
        name  = "BUDJETTIHAUKKA_TABLE"
        value = "valtiontalous_semantic_current"
      }
      env {
        name  = "BUDJETTIHAUKKA_ENABLE_LLM_QUERY_PLAN"
        value = "false"
      }
      env {
        name  = "BUDJETTIHAUKKA_QUESTION_LIBRARY_BACKEND"
        value = "firestore"
      }
      env {
        name  = "BUDJETTIHAUKKA_FIRESTORE_DATABASE"
        value = google_firestore_database.default.name
      }
      env {
        name  = "BUDJETTIHAUKKA_CORS_ORIGINS"
        value = "https://${var.hosting_site_id}.web.app,https://${var.hosting_site_id}.firebaseapp.com"
      }
      env {
        name = "BUDJETTIHAUKKA_ADMIN_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.admin_key.secret_id
            version = "latest"
          }
        }
      }

      startup_probe {
        initial_delay_seconds = 0
        timeout_seconds       = 5
        period_seconds        = 5
        failure_threshold     = 20

        http_get {
          path = "/health"
          port = 8080
        }
      }

      liveness_probe {
        initial_delay_seconds = 10
        timeout_seconds       = 5
        period_seconds        = 30

        http_get {
          path = "/health"
          port = 8080
        }
      }
    }
  }

  depends_on = [
    google_project_service.required,
    google_secret_manager_secret_iam_member.api_admin_key,
    google_project_iam_member.api_bigquery_job_user,
    google_bigquery_dataset_iam_member.api_data_viewer,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  count = var.deploy_api ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api[0].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
