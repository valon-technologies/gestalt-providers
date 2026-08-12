import json
import re
import unittest
from pathlib import Path


PACKAGE_DIR = Path(__file__).parents[1]
MANIFEST = PACKAGE_DIR / "manifest.yaml"
OPENAPI = PACKAGE_DIR / "openapi.json"

EXPECTED_ALLOWED_OPERATIONS = {
    "drive.about.get",
    "drive.comments.create",
    "drive.comments.delete",
    "drive.comments.get",
    "drive.comments.list",
    "drive.comments.update",
    "drive.files.copy",
    "drive.files.create",
    "drive.files.delete",
    "drive.files.export",
    "drive.files.get",
    "drive.files.list",
    "drive.files.update",
    "drive.permissions.create",
    "drive.permissions.delete",
    "drive.permissions.list",
    "drive.permissions.update",
    "drive.replies.create",
    "drive.replies.delete",
    "drive.replies.get",
    "drive.replies.list",
    "drive.replies.update",
}


class GoogleDriveOpenAPIContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = json.loads(OPENAPI.read_text())

    def test_manifest_uses_bundled_spec(self):
        manifest = MANIFEST.read_text()
        self.assertRegex(manifest, r"(?m)^      document: openapi\.json$")

    def test_allowed_operations_remain_stable(self):
        manifest = MANIFEST.read_text()
        actual = set(
            re.findall(r"^    (drive\.[a-zA-Z.]+):$", manifest, re.MULTILINE)
        )
        self.assertEqual(actual, EXPECTED_ALLOWED_OPERATIONS)

    def test_files_get_supports_media_download(self):
        operation = self.spec["paths"]["/files/{fileId}"]["get"]
        parameters = {
            (parameter["name"], parameter["in"]): parameter
            for parameter in operation["parameters"]
        }

        self.assertIn(("fileId", "path"), parameters)
        self.assertIn(("acknowledgeAbuse", "query"), parameters)
        self.assertIn(("alt", "query"), parameters)
        self.assertEqual(parameters[("alt", "query")]["schema"]["enum"], ["json", "media"])

        responses = operation["responses"]["200"]["content"]
        self.assertIn("application/json", responses)
        self.assertEqual(responses["*/*"]["schema"], {"format": "binary", "type": "string"})


if __name__ == "__main__":
    unittest.main()
