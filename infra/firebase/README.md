# Firebase production infrastructure

Production URL: <https://valtion-budjetti-data.web.app>

Release 2.2.0 was verified against Cloud Run revision health, a real
BigQuery-backed analytics request, visualization planning and Firestore
question logging.

This Terraform root manages Budjettihaukka's production resources in the
existing Firebase project `valtion-budjetti-data`:

- existing Firebase project and default Hosting site, imported into state
- Firebase Web App registration
- Firebase Authentication and Google sign-in configuration
- dedicated `budjettihaukka-api` Cloud Run service
- dedicated least-privilege runtime service account
- read-only access to `budjettihaukka-gpt.valtiodata`
- Firestore question library with direct browser access denied
- Secret Manager admin key
- Artifact Registry repository

Hosting files and rewrites are declarative in `/firebase.json` and deployed
with the Firebase CLI after Terraform has created the Cloud Run service.
The health endpoint remains public at `/health`. Analytics and admin API
routes require a valid Firebase ID token in production; admin reads also
require the Secret Manager key. `/healthz` is avoided because some
Cloud Run URL paths ending in `z` are reserved by the platform.

## Prerequisites

1. Enable Cloud Billing on `valtion-budjetti-data`.
2. Install Terraform 1.6 or newer.
3. Authenticate application default credentials:

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project valtion-budjetti-data
```

## Deployment

Use the repository deployment script from the repository root:

```bash
scripts/deploy_firebase.sh
```

The script first provisions Identity Platform and enables Google sign-in,
then builds an immutable container image, applies the token-protected Cloud
Run service, smoke-tests it, and only then publishes Firebase Hosting. The
Firebase CLI version is pinned by the script because declarative provider
deployment requires a current CLI. The script intentionally refuses to run
while billing is disabled.

Firebase's default Hosting domains are authorized automatically by the Auth
provider deploy. Do not duplicate them in `auth.providers.googleSignIn`;
`authorizedRedirectUris` is reserved here for local development origins.

The default cost guardrails are:

- Cloud Run minimum instances: 0
- Cloud Run maximum instances: 2
- BigQuery maximum bytes billed per query: 1 GB
- LLM query planning disabled in production

A Cloud Billing budget sends alerts but is not a hard spending cap. Add
rate limiting or App Check before opening the service to high-volume public
traffic.

Retrieve the protected admin key after apply:

```bash
terraform -chdir=infra/firebase output -raw admin_key
```

Terraform state contains sensitive values and is ignored by Git. Move the
state to a protected GCS backend before enabling unattended CI deployments.

## Verification and rollback

```bash
curl --fail https://valtion-budjetti-data.web.app/health
terraform -chdir=infra/firebase plan
```

Cloud Run images are deployed by immutable digest. To roll back, apply the
previous known-good digest as `api_image`, verify the API health endpoint,
then redeploy Hosting. Firebase Hosting versions can also be rolled back
from the Firebase console without changing BigQuery data.
