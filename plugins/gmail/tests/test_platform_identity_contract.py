import json
import unittest
import urllib.parse
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import Any, cast
from unittest import mock

import gestalt
import yaml

import internals.client as client_module
import internals.platform_identity as platform_identity
import provider as provider_module

PLUGIN_DIR = Path(__file__).resolve().parents[1]


class FakeHTTPResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "FakeHTTPResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def request_data(request: urllib.request.Request) -> bytes:
    data = request.data
    if isinstance(data, bytes):
        return data
    raise AssertionError("expected request data bytes")


class GmailPlatformIdentityContractTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_module.configure(
            "gmail", {"clientId": "client-id", "clientSecret": "client-secret"}
        )
        platform_identity.clear_token_cache()

    def tearDown(self) -> None:
        provider_module.configure(
            "gmail", {"clientId": "client-id", "clientSecret": "client-secret"}
        )
        platform_identity.clear_token_cache()

    def test_manifest_and_schema_expose_provider_backed_read_contract(self) -> None:
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())
        allowed = manifest["spec"]["allowedOperations"]
        self.assertIn("messages.list", allowed)
        self.assertIn("messages.get", allowed)
        self.assertIn("threads.get", allowed)
        self.assertIn("labels.list", allowed)
        self.assertIn("getProfile", allowed)
        self.assertNotIn("gmail.users.messages.list", allowed)
        self.assertNotIn("gmail.users.messages.get", allowed)
        self.assertNotIn("gmail.users.threads.get", allowed)
        platform_connection = manifest["spec"]["connections"]["platform"]
        self.assertEqual(platform_connection["mode"], "none")
        self.assertEqual(platform_connection["exposure"], "internal")
        self.assertNotIn("auth", platform_connection)

        schema = yaml.safe_load(
            (PLUGIN_DIR / "schemas" / "config.schema.yaml").read_text()
        )
        platform_schema = schema["properties"]["platformIdentity"]
        self.assertNotIn("const", platform_schema["properties"]["subjectEmail"])
        self.assertIn("scopes", platform_schema["properties"])
        self.assertNotIn("enum", platform_schema["properties"]["operations"]["items"])

    def test_platform_config_uses_deployment_policy(self) -> None:
        provider_module.configure(
            "gmail",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "platformIdentity": {
                    "enabled": True,
                    "subjectEmail": "brain-ingest@example.com",
                    "serviceAccountEmail": "gmail-ingest@example.iam.gserviceaccount.com",
                    "scopes": [
                        "https://www.googleapis.com/auth/gmail.readonly",
                        "https://www.googleapis.com/auth/gmail.metadata",
                    ],
                    "operations": ["messages.list", "messages.modify"],
                },
            },
        )

        self.assertEqual(
            provider_module._platform_identity_config.subject_email,
            "brain-ingest@example.com",
        )
        self.assertEqual(
            provider_module._platform_identity_config.scopes,
            (
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/gmail.metadata",
            ),
        )
        self.assertEqual(
            provider_module._platform_identity_config.operations,
            frozenset({"messages.list", "messages.modify"}),
        )

    def test_platform_config_requires_scopes(self) -> None:
        with self.assertRaisesRegex(ValueError, "scopes"):
            provider_module.configure(
                "gmail",
                {
                    "clientId": "client-id",
                    "clientSecret": "client-secret",
                    "platformIdentity": {
                        "enabled": True,
                        "subjectEmail": "brain-ingest@example.com",
                        "serviceAccountEmail": "gmail-ingest@example.iam.gserviceaccount.com",
                        "operations": ["messages.list"],
                    },
                },
            )

    def test_user_token_read_does_not_mint_platform_token(self) -> None:
        provider_module.configure(
            "gmail",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "platformIdentity": {
                    "enabled": True,
                    "subjectEmail": "brain-ingest@example.com",
                    "serviceAccountEmail": "gmail-ingest@example.iam.gserviceaccount.com",
                    "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
                    "operations": ["messages.list"],
                },
            },
        )
        with (
            mock.patch.object(
                provider_module,
                "platform_token_for_operation",
                side_effect=AssertionError("unexpected mint"),
            ),
            mock.patch.object(
                client_module, "get_json", return_value={"messages": [{"id": "m-1"}]}
            ) as get_json,
        ):
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
            provider_module,
            "platform_token_for_operation",
            side_effect=AssertionError("unexpected mint"),
        ):
            result = provider_module.messages_list(
                provider_module.MessagesListInput(),
                gestalt.Request(token="", credential=gestalt.Credential(mode="user")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_default_credential_mode_missing_token_read_is_unauthorized(self) -> None:
        with mock.patch.object(
            provider_module,
            "platform_token_for_operation",
            side_effect=AssertionError("unexpected mint"),
        ):
            result = provider_module.messages_list(
                provider_module.MessagesListInput(),
                gestalt.Request(token=""),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_credential_mode_none_read_mints_platform_token(self) -> None:
        provider_module.configure(
            "gmail",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "platformIdentity": {
                    "enabled": True,
                    "subjectEmail": "brain-ingest@example.com",
                    "serviceAccountEmail": "gmail-ingest@example.iam.gserviceaccount.com",
                    "scopes": ["https://www.googleapis.com/auth/gmail.readonly"],
                    "operations": ["messages.list"],
                },
            },
        )
        with (
            mock.patch.object(
                provider_module,
                "platform_token_for_operation",
                return_value="platform-token",
            ) as mint,
            mock.patch.object(
                client_module, "get_json", return_value={"messages": [{"id": "m-1"}]}
            ) as get_json,
        ):
            result = provider_module.messages_list(
                provider_module.MessagesListInput(q="to:athena@example.com"),
                gestalt.Request(
                    token="",
                    credential=gestalt.Credential(mode="none"),
                ),
            )

        self.assertEqual(result, {"messages": [{"id": "m-1"}]})
        mint.assert_called_once()
        self.assertEqual(
            mint.call_args.args[0].subject_email, "brain-ingest@example.com"
        )
        self.assertEqual(mint.call_args.args[1], "messages.list")
        self.assertEqual(get_json.call_args.args[1], "platform-token")

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

    def test_platform_token_transport_uses_keyless_dwd_jwt_flow(self) -> None:
        requests: list[urllib.request.Request] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: int = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            requests.append(request)
            url = request.full_url
            if url == platform_identity.METADATA_TOKEN_URL:
                self.assertEqual(request.get_header("Metadata-flavor"), "Google")
                return FakeHTTPResponse({"access_token": "metadata-token"})
            if url.startswith(platform_identity.IAM_SIGN_JWT_BASE_URL):
                self.assertEqual(
                    request.get_header("Authorization"), "Bearer metadata-token"
                )
                body = json.loads(request_data(request).decode("utf-8"))
                claims = json.loads(body["payload"])
                self.assertEqual(
                    claims["iss"], "gmail-ingest@example.iam.gserviceaccount.com"
                )
                self.assertEqual(claims["sub"], "brain-ingest@example.com")
                self.assertEqual(
                    claims["scope"],
                    "https://www.googleapis.com/auth/gmail.readonly "
                    "https://www.googleapis.com/auth/gmail.metadata",
                )
                self.assertEqual(claims["aud"], platform_identity.JWT_AUDIENCE)
                return FakeHTTPResponse({"signedJwt": "signed.jwt"})
            if url == platform_identity.OAUTH_TOKEN_URL:
                body = urllib.parse.parse_qs(request_data(request).decode("utf-8"))
                self.assertEqual(
                    body["grant_type"],
                    ["urn:ietf:params:oauth:grant-type:jwt-bearer"],
                )
                self.assertEqual(body["assertion"], ["signed.jwt"])
                return FakeHTTPResponse(
                    {"access_token": "gmail-access-token", "expires_in": 3600}
                )
            raise AssertionError(f"unexpected request: {url}")

        config = platform_identity.PlatformIdentityConfig(
            enabled=True,
            subject_email="brain-ingest@example.com",
            service_account_email="gmail-ingest@example.iam.gserviceaccount.com",
            scopes=(
                "https://www.googleapis.com/auth/gmail.readonly",
                "https://www.googleapis.com/auth/gmail.metadata",
            ),
            operations=frozenset({"messages.list"}),
        )
        with mock.patch.object(
            platform_identity.urllib.request, "urlopen", fake_urlopen
        ):
            token = platform_identity.platform_token_for_operation(
                config, "messages.list", now=1_777_908_400
            )

        self.assertEqual(token, "gmail-access-token")
        self.assertEqual(len(requests), 3)


if __name__ == "__main__":
    unittest.main()
