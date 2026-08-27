#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-valtion-budjetti-data}"
DATA_PROJECT_ID="${DATA_PROJECT_ID:-budjettihaukka-gpt}"
REGION="${REGION:-europe-west1}"
SERVICE_NAME="${SERVICE_NAME:-budjettihaukka-api}"
REPOSITORY="${REPOSITORY:-budjettihaukka}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT_DIR}/infra/firebase"

for command in gcloud firebase npm terraform; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

billing_enabled="$(gcloud beta billing projects describe "${PROJECT_ID}" --format='value(billingEnabled)')"
if [[ "${billing_enabled}" != "True" && "${billing_enabled}" != "true" ]]; then
  echo "Cloud Billing must be enabled before Cloud Run can be deployed:" >&2
  echo "https://console.cloud.google.com/billing/linkedaccount?project=${PROJECT_ID}" >&2
  exit 2
fi

terraform -chdir="${TF_DIR}" init
state_resources="$(terraform -chdir="${TF_DIR}" state list 2>/dev/null || true)"
if [[ "${state_resources}" != *'google_cloud_run_v2_service.api[0]'* ]]; then
  terraform -chdir="${TF_DIR}" apply \
    -var="project_id=${PROJECT_ID}" \
    -var="data_project_id=${DATA_PROJECT_ID}" \
    -var="region=${REGION}" \
    -var="deploy_api=false"
fi

tag="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)-$(date -u +%Y%m%d%H%M%S)"
image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/api:${tag}"
gcloud builds submit "${ROOT_DIR}" --project="${PROJECT_ID}" --tag="${image}"

digest="$(gcloud artifacts docker images describe "${image}" --project="${PROJECT_ID}" --format='value(image_summary.digest)')"
immutable_image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/api@${digest}"

terraform -chdir="${TF_DIR}" apply \
  -var="project_id=${PROJECT_ID}" \
  -var="data_project_id=${DATA_PROJECT_ID}" \
  -var="region=${REGION}" \
  -var="deploy_api=true" \
  -var="api_image=${immutable_image}"

api_url="$(terraform -chdir="${TF_DIR}" output -raw api_url)"
curl --fail --silent --show-error --retry 12 --retry-all-errors --retry-delay 5 "${api_url}/health"

npm --prefix "${ROOT_DIR}/frontend" ci
VITE_BASE_PATH=/ VITE_API_BASE_URL= npm --prefix "${ROOT_DIR}/frontend" run build

firebase deploy --project="${PROJECT_ID}" --only firestore:rules,firestore:indexes
firebase deploy --project="${PROJECT_ID}" --only hosting

hosting_url="https://${PROJECT_ID}.web.app"
curl --fail --silent --show-error --retry 6 --retry-delay 3 "${hosting_url}/health"
echo
echo "Budjettihaukka deployed: ${hosting_url}"
