"""Offline contract checks for the declarative Google Drive upload surface."""
import json
from pathlib import Path
import unittest
import yaml

ROOT = Path(__file__).resolve().parent

class DriveUploadContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = yaml.safe_load((ROOT / "openapi.yaml").read_text())
        cls.manifest = yaml.safe_load((ROOT / "manifest.yaml").read_text())
        cls.operations = {
            operation["operationId"]: (method.upper(), path, operation)
            for path, item in cls.spec["paths"].items()
            for method, operation in item.items()
            if isinstance(operation, dict) and "operationId" in operation
        }

    def test_existing_exposed_operations_keep_exact_effective_urls(self):
        routes = json.loads((ROOT / "legacy_operation_routes.json").read_text())
        for operation_id, expected in routes.items():
            method, path, _ = self.operations[operation_id]
            self.assertEqual({"method": method, "url": self.spec["servers"][0]["url"] + path}, expected)

    def test_upload_has_separate_route_and_original_oauth_identity(self):
        method, path, operation = self.operations["drive.files.beginUpload"]
        self.assertEqual(method, "POST")
        self.assertEqual(self.spec["servers"][0]["url"] + path,
                         "https://www.googleapis.com/upload/drive/v3/files")
        allowed = self.manifest["spec"]["allowedOperations"]
        self.assertEqual(allowed["drive.files.beginUpload"]["alias"], "files.beginUpload")
        self.assertEqual(allowed["drive.files.generateIds"]["alias"], "files.generateIds")
        self.assertEqual(operation["security"][0], self.operations["drive.files.create"][2]["security"][0])
        self.assertEqual(self.manifest["spec"]["connections"]["default"]["auth"]["scopes"],
                         ["https://www.googleapis.com/auth/drive"])

    def test_session_start_body_and_headers_do_not_accept_content_or_credentials(self):
        operation = self.operations["drive.files.beginUpload"][2]
        params = {p["name"]: p for p in operation["parameters"]}
        self.assertEqual(params["uploadType"]["schema"]["enum"], ["resumable"])
        self.assertTrue(params["uploadType"]["required"])
        self.assertEqual(params["X-Upload-Content-Type"]["in"], "header")
        self.assertEqual(params["X-Upload-Content-Length"]["in"], "header")
        body = operation["requestBody"]["content"]["application/json"]["schema"]
        self.assertEqual(set(body["properties"]), {"id", "name", "mimeType", "parents"})
        self.assertEqual(set(body["required"]), {"name", "mimeType"})
        self.assertNotIn("Authorization", params)
        self.assertNotIn("access_token", params)

    def test_start_response_exposes_location_without_claiming_ready_file(self):
        response = self.operations["drive.files.beginUpload"][2]["responses"]["200"]
        self.assertEqual(response["headers"]["Location"]["schema"]["format"], "uri")
        self.assertNotIn("content", response)
        method, path, operation = self.operations["drive.files.generateIds"]
        self.assertEqual(method, "GET")
        self.assertEqual(path, "/drive/v3/files/generateIds")
        self.assertTrue({"count", "space", "type"}.issubset({p["name"] for p in operation["parameters"]}))

if __name__ == "__main__":
    unittest.main()
