"""Release-boundary checks for the publicly distributable application shell.

These tests must not require credentials, network access, or a running service.
"""
from pathlib import Path
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class PublicReleaseSafetyTests(unittest.TestCase):
    def test_private_analysis_engine_is_not_in_public_tree(self):
        self.assertFalse((ROOT / "services" / "analysis_orchestrator.py").exists())
        self.assertFalse((ROOT / "streamlit_app.py").exists())

    def test_private_layers_are_not_in_public_tree(self):
        # The analysis engine, data processing and their tests are not published.
        for path in [
            "services",
            "utils",
            "domain",
            "pages",
            "agent_data",
            "data/ontology",
            "data/sql",
            "firebase.staging.json",
            "infra/staging",
        ]:
            with self.subTest(path=path):
                self.assertFalse((ROOT / path).exists(), f"{path} must not be in the public tree")

    def test_mcp_server_is_not_in_public_tree(self):
        # The public MCP server is reachable over HTTP; its code is not published.
        for path in [
            "services/mcp_readiness",
            "Dockerfile.mcp",
            "requirements-mcp.txt",
            "data/mcp",
            "infra/firebase/mcp.tf",
            "infra/firebase/cloudbuild.mcp.yaml",
            "scripts/deploy_mcp.sh",
        ]:
            with self.subTest(path=path):
                self.assertFalse((ROOT / path).exists(), f"{path} must not be in the public tree")

    def test_private_release_scripts_are_not_in_public_tree(self):
        for path in [
            "scripts/run_live_checks.sh",
            "scripts/build_firebase_frontend.py",
            "scripts/deploy_staging.py",
        ]:
            with self.subTest(path=path):
                self.assertFalse((ROOT / path).exists(), f"{path} must not be in the public tree")

    def test_public_container_copies_only_the_shell(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        for private_dir in ["services", "utils", "domain", "data/ontology"]:
            self.assertNotIn(f"COPY {private_dir}", dockerfile)

    def test_public_deployment_script_refuses_production_deploy(self):
        # The guard runs before any gcloud, Firebase or Terraform commands.
        result = subprocess.run(
            ["bash", str(ROOT / "scripts" / "deploy_firebase.sh")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Refusing to deploy", result.stderr)

    def test_api_documents_unavailable_analysis_engine(self):
        app_source = (ROOT / "api" / "app.py").read_text(encoding="utf-8")
        self.assertIn("status_code=501", app_source)
        self.assertIn("Analyysimoottori ei kuulu", app_source)


if __name__ == "__main__":
    unittest.main()
