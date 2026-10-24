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
