import google.auth

try:
    credentials, project = google.auth.default()
    print(f"Successfully authenticated with project: {project}")
except Exception as e:
    print(f"Authentication error: {e}")