import json
import tomllib
import unittest
import urllib.parse
from http import HTTPStatus
from pathlib import Path
from typing import Any, cast
from unittest import mock

import gestalt
import yaml

import internals.client as client_module
import provider as provider_module

PLUGIN_DIR = Path(__file__).resolve().parents[1]


class GmailCredentialContractTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_module.configure(
            "gmail", {"clientId": "client-id", "clientSecret": "client-secret"}
        )

    def tearDown(self) -> None:
        provider_module.configure(
            "gmail", {"clientId": "client-id", "clientSecret": "client-secret"}
        )

    def test_runtime_target_dispatches_static_python_operations(self) -> None:
        pyproject = tomllib.loads((PLUGIN_DIR / "pyproject.toml").read_text())
        self.assertEqual(pyproject["tool"]["gestalt"]["provider"], "provider:app")

        result = provider_module.app.execute(
            "messages.list", {}, gestalt.Request(token="")
        )

        self.assertEqual(result.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(json.loads(result.body), {"error": "token is required"})

    def test_manifest_and_schema_expose_subject_owned_oauth_contract(self) -> None:
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())
        allowed = manifest["spec"]["allowedOperations"]
        self.assertIn("messages.list", allowed)
        self.assertIn("messages.get", allowed)
        self.assertIn("messages.attachments.get", allowed)
        self.assertIn("threads.get", allowed)
        self.assertIn("labels.list", allowed)
        self.assertIn("getProfile", allowed)
        self.assertNotIn("gmail.users.messages.list", allowed)
        self.assertNotIn("gmail.users.messages.get", allowed)
        self.assertNotIn("gmail.users.messages.attachments.get", allowed)
        self.assertNotIn("gmail.users.threads.get", allowed)

        connections = manifest["spec"]["connections"]
        self.assertEqual(set(connections), {"default"})
        default_auth = connections["default"]["auth"]
        self.assertEqual(default_auth["type"], "oauth2")
        self.assertEqual(
            default_auth["authorizationUrl"],
            "https://accounts.google.com/o/oauth2/v2/auth",
        )
        self.assertEqual(
            default_auth["tokenUrl"], "https://oauth2.googleapis.com/token"
        )
        self.assertIn(
            "https://www.googleapis.com/auth/gmail.modify", default_auth["scopes"]
        )
        self.assertIn(
            "https://www.googleapis.com/auth/gmail.compose", default_auth["scopes"]
        )

        catalog = yaml.safe_load((PLUGIN_DIR / "catalog.yaml").read_text())
        attachment_operation = {
            operation["id"]: operation for operation in catalog["operations"]
        }["messages.attachments.get"]
        self.assertTrue(attachment_operation["read_only"])
        self.assertEqual(
            [parameter["name"] for parameter in attachment_operation["parameters"]],
            ["messageId", "attachmentId", "fields"],
        )
        self.assertEqual(
            {
                parameter["name"]
                for parameter in attachment_operation["parameters"]
                if parameter.get("required")
            },
            {"messageId", "attachmentId"},
        )

        schema = yaml.safe_load(
            (PLUGIN_DIR / "schemas" / "config.schema.yaml").read_text()
        )
        self.assertEqual(set(schema["properties"]), {"clientId", "clientSecret"})
        self.assertEqual(set(schema["required"]), {"clientId", "clientSecret"})
        self.assertFalse(schema["additionalProperties"])

    def test_user_token_read_uses_resolved_oauth_token(self) -> None:
        with mock.patch.object(
            client_module, "get_json", return_value={"messages": [{"id": "m-1"}]}
        ) as get_json:
            result = provider_module.messages_list(
                provider_module.MessagesListInput(q="subject:test"),
                gestalt.Request(
                    token="user-token",
                    credential=gestalt.Credential(mode="user"),
                ),
            )

        self.assertEqual(result, {"messages": [{"id": "m-1"}]})
        self.assertEqual(get_json.call_args.args[1], "user-token")

    def test_missing_user_token_read_is_unauthorized(self) -> None:
        with mock.patch.object(
            client_module,
            "get_json",
            side_effect=AssertionError("unexpected Gmail API call"),
        ):
            result = provider_module.messages_list(
                provider_module.MessagesListInput(),
                gestalt.Request(token="", credential=gestalt.Credential(mode="user")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_credential_mode_none_read_requires_token(self) -> None:
        with mock.patch.object(
            client_module,
            "get_json",
            side_effect=AssertionError("unexpected Gmail API call"),
        ):
            result = provider_module.messages_list(
                provider_module.MessagesListInput(),
                gestalt.Request(token="", credential=gestalt.Credential(mode="none")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_default_credential_mode_missing_token_read_is_unauthorized(self) -> None:
        with mock.patch.object(
            client_module,
            "get_json",
            side_effect=AssertionError("unexpected Gmail API call"),
        ):
            result = provider_module.messages_list(
                provider_module.MessagesListInput(),
                gestalt.Request(token=""),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_message_list_preserves_repeated_query_parameter_encoding(self) -> None:
        with mock.patch.object(
            client_module, "get_json", return_value={"messages": []}
        ) as get_json:
            provider_module.messages_list(
                provider_module.MessagesListInput(
                    q="from:alice@example.com",
                    labelIds=["INBOX", "Label_1"],
                    maxResults=25,
                    pageToken="next-page",
                    includeSpamTrash=True,
                ),
                gestalt.Request(
                    token="user-token",
                    credential=gestalt.Credential(mode="user"),
                ),
            )

        url = get_json.call_args.args[0]
        parsed = urllib.parse.urlparse(url)
        query = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(query["q"], ["from:alice@example.com"])
        self.assertEqual(query["labelIds"], ["INBOX", "Label_1"])
        self.assertEqual(query["maxResults"], ["25"])
        self.assertEqual(query["pageToken"], ["next-page"])
        self.assertEqual(query["includeSpamTrash"], ["true"])

    def test_message_list_omits_max_results_when_unset(self) -> None:
        with mock.patch.object(
            client_module, "get_json", return_value={"messages": []}
        ) as get_json:
            provider_module.messages_list(
                provider_module.MessagesListInput(q="from:alice@example.com"),
                gestalt.Request(
                    token="user-token",
                    credential=gestalt.Credential(mode="user"),
                ),
            )

        url = get_json.call_args.args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        self.assertEqual(query["q"], ["from:alice@example.com"])
        self.assertNotIn("maxResults", query)

    def test_thread_get_preserves_metadata_headers_and_raw_error_body(self) -> None:
        raw_error = {
            "error": {
                "code": 403,
                "message": "delegation denied",
                "status": "PERMISSION_DENIED",
            }
        }
        with mock.patch.object(
            client_module,
            "get_json",
            side_effect=client_module.GmailAPIError(
                403, "delegation denied", raw_body=raw_error
            ),
        ) as get_json:
            result = provider_module.threads_get(
                provider_module.ThreadGetInput(
                    id="thread-1",
                    format="metadata",
                    metadataHeaders=["From", "Subject"],
                ),
                gestalt.Request(
                    token="user-token",
                    credential=gestalt.Credential(mode="user"),
                ),
            )

        url = get_json.call_args.args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        self.assertEqual(query["format"], ["metadata"])
        self.assertEqual(query["metadataHeaders"], ["From", "Subject"])
        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, 403)
        self.assertEqual(response.body, raw_error)


if __name__ == "__main__":
    unittest.main()
