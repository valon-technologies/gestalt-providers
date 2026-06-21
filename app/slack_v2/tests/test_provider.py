import hashlib
import hmac
import json
import base64
import time
from collections.abc import Mapping
from http import HTTPStatus
import unittest
from typing import Any
from unittest import mock

import gestalt

from internals import events
import provider as provider_module


def signed_slack_request(
    payload: Mapping[str, Any],
    *,
    raw_body: bytes | None = None,
    signing_secret: str = "signing-secret",
    timestamp: int | None = None,
) -> gestalt.Request:
    if timestamp is None:
        timestamp = int(time.time())
    request_body = raw_body or json.dumps(payload, separators=(",", ":")).encode(
        "utf-8"
    )
    base_string = b"v0:" + str(timestamp).encode("utf-8") + b":" + request_body
    signature = (
        "v0="
        + hmac.new(
            signing_secret.encode("utf-8"), base_string, hashlib.sha256
        ).hexdigest()
    )
    return gestalt.Request(
        context=gestalt.HTTPSubjectRequest(
            headers={
                "X-Slack-Request-Timestamp": [str(timestamp)],
                "X-Slack-Signature": [signature],
            },
            raw_body=request_body,
        )
    )


def signed_slack_workflow_context_request(
    payload: Mapping[str, Any],
    *,
    raw_body: bytes | None = None,
    signing_secret: str = "signing-secret",
    timestamp: int | None = None,
) -> gestalt.Request:
    request = signed_slack_request(
        payload,
        raw_body=raw_body,
        signing_secret=signing_secret,
        timestamp=timestamp,
    )
    context = request.context
    assert isinstance(context, gestalt.HTTPSubjectRequest)
    return gestalt.Request(
        workflow={
            "http": {
                "headers": context.headers,
                "rawBodyBase64": base64.b64encode(context.raw_body).decode("ascii"),
            }
        },
    )


class FakeWorkflowClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.deliver_event_requests: list[gestalt.WorkflowDeliverEvent] = []

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def deliver_event(
        self, request: gestalt.WorkflowDeliverEvent
    ) -> gestalt.WorkflowEvent:
        self.deliver_event_requests.append(request)
        if self.fail:
            raise RuntimeError("workflow client unavailable")
        event = request.event
        if event is None:
            return gestalt.WorkflowEvent()
        return event


class SlackV2ProviderTests(unittest.TestCase):
    @mock.patch("provider.save_slack_event_registration")
    def test_register_slack_event_persists_registration(
        self, save_registration: mock.Mock
    ) -> None:
        result = provider_module.register_slack_event(
            provider_module.RegisterSlackEventInput(
                app_id="A123",
                client_id="123.456",
                client_secret="client-secret",
                signing_secret="signing-secret",
                display_name="Test Bot",
                bot_token="xoxb-test-bot",
                workflow_event_subject="slack_agent_default",
            ),
            gestalt.Request(),
        )

        save_registration.assert_called_once_with(
            app_id="A123",
            client_id="123.456",
            client_secret="client-secret",
            signing_secret="signing-secret",
            display_name="Test Bot",
            bot_token="xoxb-test-bot",
            workflow_event_subject="slack_agent_default",
        )
        self.assertEqual(
            result,
            {
                "ok": True,
                "app_id": "A123",
                "display_name": "Test Bot",
                "workflow_event_subject": "slack_agent_default",
            },
        )

    def test_register_slack_event_requires_app_id(self) -> None:
        result = provider_module.register_slack_event(
            provider_module.RegisterSlackEventInput(
                app_id="  ",
                client_id="123.456",
                client_secret="client-secret",
                signing_secret="signing-secret",
                display_name="Test Bot",
                bot_token="xoxb-test-bot",
                workflow_event_subject="slack_agent_default",
            ),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "app_id is required"})

    def test_register_slack_event_requires_bot_token(self) -> None:
        result = provider_module.register_slack_event(
            provider_module.RegisterSlackEventInput(
                app_id="A123",
                client_id="123.456",
                client_secret="client-secret",
                signing_secret="signing-secret",
                display_name="Test Bot",
                bot_token="  ",
                workflow_event_subject="slack_agent_default",
            ),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "bot_token is required"})

    def test_register_slack_event_requires_workflow_event_subject(self) -> None:
        result = provider_module.register_slack_event(
            provider_module.RegisterSlackEventInput(
                app_id="A123",
                client_id="123.456",
                client_secret="client-secret",
                signing_secret="signing-secret",
                display_name="Test Bot",
                bot_token="xoxb-test-bot",
            ),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "workflow_event_subject is required"})

    @mock.patch("provider.slack_get")
    @mock.patch("provider.load_default_bot_token")
    def test_get_user_display_name_returns_profile_display_name(
        self,
        load_default_bot_token: mock.Mock,
        slack_get: mock.Mock,
    ) -> None:
        load_default_bot_token.return_value = "xoxb-test-bot"
        slack_get.return_value = {
            "ok": True,
            "user": {
                "real_name": "Alice Smith",
                "profile": {"display_name": "alice"},
            },
        }

        result = provider_module.get_user_display_name(
            provider_module.GetUserDisplayNameInput(user_id="U123"),
            gestalt.Request(),
        )

        load_default_bot_token.assert_called_once_with()
        slack_get.assert_called_once_with(
            "users.info", {"user": "U123"}, "xoxb-test-bot"
        )
        self.assertEqual(result, {"user_id": "U123", "display_name": "alice"})

    @mock.patch("provider.slack_get")
    @mock.patch("provider.load_default_bot_token")
    def test_get_user_display_name_falls_back_to_real_name(
        self,
        load_default_bot_token: mock.Mock,
        slack_get: mock.Mock,
    ) -> None:
        load_default_bot_token.return_value = "xoxb-test-bot"
        slack_get.return_value = {
            "ok": True,
            "user": {
                "real_name": "Alice Smith",
                "profile": {"display_name": ""},
            },
        }

        result = provider_module.get_user_display_name(
            provider_module.GetUserDisplayNameInput(user_id="U123"),
            gestalt.Request(),
        )

        self.assertEqual(result, {"user_id": "U123", "display_name": "Alice Smith"})

    def test_get_user_display_name_requires_user_id(self) -> None:
        result = provider_module.get_user_display_name(
            provider_module.GetUserDisplayNameInput(user_id="  "),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "user_id is required"})

    @mock.patch("provider.load_default_bot_token")
    def test_get_user_display_name_returns_not_found_for_missing_registration(
        self, load_default_bot_token: mock.Mock
    ) -> None:
        load_default_bot_token.side_effect = gestalt.NotFoundError("missing")

        result = provider_module.get_user_display_name(
            provider_module.GetUserDisplayNameInput(user_id="U123"),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "default Slack event registration not found"}
        )

    @mock.patch("provider.slack_get")
    @mock.patch("provider.load_default_bot_token")
    def test_get_user_display_name_returns_not_found_for_missing_slack_user(
        self,
        load_default_bot_token: mock.Mock,
        slack_get: mock.Mock,
    ) -> None:
        load_default_bot_token.return_value = "xoxb-test-bot"
        slack_get.side_effect = provider_module.SlackAPIError(
            HTTPStatus.NOT_FOUND, "user_not_found"
        )

        result = provider_module.get_user_display_name(
            provider_module.GetUserDisplayNameInput(user_id="U123"),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "user not found for user_id 'U123'"}
        )

    @mock.patch("provider.load_default_workflow_event_subject")
    def test_get_workflow_event_subject_for_app_returns_registration(
        self, load_default_workflow_event_subject: mock.Mock
    ) -> None:
        load_default_workflow_event_subject.return_value = "slack_agent_default"

        result = provider_module.get_workflow_event_subject_for_app(
            provider_module.GetWorkflowEventSubjectForAppInput(app_id="A123"),
            gestalt.Request(),
        )

        load_default_workflow_event_subject.assert_called_once_with()
        self.assertEqual(
            result,
            {
                "app_id": "A123",
                "workflow_event_subject": "slack_agent_default",
            },
        )

    def test_get_workflow_event_subject_for_app_requires_app_id(self) -> None:
        result = provider_module.get_workflow_event_subject_for_app(
            provider_module.GetWorkflowEventSubjectForAppInput(app_id="  "),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "app_id is required"})

    @mock.patch("provider.load_default_workflow_event_subject")
    def test_get_workflow_event_subject_for_app_returns_not_found(
        self, load_default_workflow_event_subject: mock.Mock
    ) -> None:
        load_default_workflow_event_subject.side_effect = gestalt.NotFoundError(
            "missing"
        )

        result = provider_module.get_workflow_event_subject_for_app(
            provider_module.GetWorkflowEventSubjectForAppInput(app_id="A404"),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "default Slack event registration not found"}
        )

    @mock.patch("provider.save_debug_payload")
    def test_debug_record_smoke_run_stores_payload_by_event_id(
        self, save_payload: mock.Mock
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "team_id": "T123",
            "type": "event_callback",
            "event-context": "EC123",
            "event_context": "EC456",
            "event": {
                "type": "message",
                "text": "hello",
            },
        }

        result = provider_module.debug_record_smoke_run(
            provider_module.DebugRecordSmokeRunInput(payload=payload),
            gestalt.Request(),
        )

        save_payload.assert_called_once_with(event_id="Ev123", payload=payload)
        self.assertEqual(result, {"ok": True, "stored": True, "id": "Ev123"})

    @mock.patch("provider.save_debug_payload")
    def test_debug_record_smoke_run_requires_event_id(
        self, save_payload: mock.Mock
    ) -> None:
        result = provider_module.debug_record_smoke_run(
            provider_module.DebugRecordSmokeRunInput(payload={"api_app_id": "A123"}),
            gestalt.Request(),
        )

        save_payload.assert_not_called()
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "event_id is required"})

    @mock.patch("provider.load_debug_payload")
    def test_debug_get_smoke_run_payload_returns_stored_payload(
        self, load_payload: mock.Mock
    ) -> None:
        payload = {"event_id": "Ev123", "type": "event_callback"}
        load_payload.return_value = {"id": "Ev123", "payload": payload}

        result = provider_module.debug_get_smoke_run_payload(
            provider_module.DebugGetSmokeRunPayloadInput(event_id="Ev123"),
            gestalt.Request(),
        )

        load_payload.assert_called_once_with(event_id="Ev123")
        self.assertEqual(result, {"id": "Ev123", "payload": payload})

    def test_debug_get_smoke_run_payload_requires_event_id(self) -> None:
        result = provider_module.debug_get_smoke_run_payload(
            provider_module.DebugGetSmokeRunPayloadInput(event_id="  "),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "event_id is required"})

    @mock.patch("provider.load_debug_payload")
    def test_debug_get_smoke_run_payload_returns_not_found(
        self, load_payload: mock.Mock
    ) -> None:
        load_payload.side_effect = gestalt.NotFoundError("missing")

        result = provider_module.debug_get_smoke_run_payload(
            provider_module.DebugGetSmokeRunPayloadInput(event_id="Ev404"),
            gestalt.Request(),
        )

        load_payload.assert_called_once_with(event_id="Ev404")
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "debug payload not found for event_id 'Ev404'"}
        )

    @mock.patch("provider.load_debug_payload_ids")
    def test_debug_list_smoke_run_payload_ids_returns_ids(
        self, load_payload_ids: mock.Mock
    ) -> None:
        load_payload_ids.return_value = ["Ev123", "Ev456"]

        result = provider_module.debug_list_smoke_run_payload_ids({}, gestalt.Request())

        load_payload_ids.assert_called_once_with()
        self.assertEqual(result, {"ids": ["Ev123", "Ev456"]})

    def test_workflow_event_id_prefers_top_level_event_id(self) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "EvTopLevel",
            "event": {"event_id": "EvNested"},
        }

        self.assertEqual(
            events.workflow_event_id(app_id="A123", payload=payload),
            "slack_v2:EvTopLevel",
        )

    def test_workflow_event_id_ignores_nested_event_id(self) -> None:
        payload = {
            "api_app_id": "A123",
            "event": {"event_id": "EvNested"},
        }

        with self.assertRaisesRegex(ValueError, "event_id is required"):
            events.workflow_event_id(app_id="A123", payload=payload)

    @mock.patch("provider._verify_slack_signature", return_value=True)
    def test_handle_slack_event_requires_api_app_id(
        self, verify_slack_signature: mock.Mock
    ) -> None:
        result = provider_module.handle_slack_event({}, gestalt.Request())

        verify_slack_signature.assert_called_once_with({}, mock.ANY)
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "api_app_id is required"})

    @mock.patch("provider._verify_slack_signature", return_value=True)
    @mock.patch("provider.load_default_workflow_event_subject")
    def test_handle_slack_event_url_verification_returns_challenge(
        self,
        load_default_workflow_event_subject: mock.Mock,
        verify_slack_signature: mock.Mock,
    ) -> None:
        payload = {"type": "url_verification", "challenge": "challenge-token"}
        request = gestalt.Request()

        result = provider_module.handle_slack_event(
            payload,
            request,
        )

        verify_slack_signature.assert_called_once_with(payload, request)
        load_default_workflow_event_subject.assert_not_called()
        self.assertEqual(result, {"challenge": "challenge-token"})

    @mock.patch("provider.load_default_workflow_event_subject")
    @mock.patch("provider._verify_slack_signature", return_value=False)
    def test_handle_slack_event_rejects_invalid_signature(
        self,
        verify_slack_signature: mock.Mock,
        load_default_workflow_event_subject: mock.Mock,
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }
        request = gestalt.Request()

        result = provider_module.handle_slack_event(payload, request)

        verify_slack_signature.assert_called_once_with(payload, request)
        load_default_workflow_event_subject.assert_not_called()
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(result.body, {"error": "invalid Slack signature"})

    @mock.patch("provider.load_default_workflow_event_subject")
    @mock.patch("provider._verify_slack_signature", return_value=True)
    def test_handle_slack_event_requires_event_id(
        self,
        verify_slack_signature: mock.Mock,
        load_default_workflow_event_subject: mock.Mock,
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "type": "event_callback",
            "event": {"event_id": "EvNested"},
        }

        result = provider_module.handle_slack_event(payload, gestalt.Request())

        verify_slack_signature.assert_called_once_with(payload, mock.ANY)
        load_default_workflow_event_subject.assert_not_called()
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "event_id is required"})

    @mock.patch("provider.load_default_workflow_event_subject")
    @mock.patch("provider._verify_slack_signature", return_value=True)
    def test_handle_slack_event_returns_not_found_for_missing_default_registration(
        self,
        verify_slack_signature: mock.Mock,
        load_default_workflow_event_subject: mock.Mock,
    ) -> None:
        load_default_workflow_event_subject.side_effect = gestalt.NotFoundError(
            "missing"
        )
        payload = {"api_app_id": "A123", "event_id": "Ev123", "type": "event_callback"}

        result = provider_module.handle_slack_event(payload, gestalt.Request())

        verify_slack_signature.assert_called_once_with(payload, mock.ANY)
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "default Slack event registration not found"}
        )

    @mock.patch("provider._verify_slack_signature", return_value=True)
    @mock.patch("provider.load_default_workflow_event_subject")
    def test_handle_slack_event_delivers_workflow_event_to_default_subject(
        self,
        load_default_workflow_event_subject: mock.Mock,
        verify_slack_signature: mock.Mock,
    ) -> None:
        load_default_workflow_event_subject.return_value = "slack_agent_default"
        workflow_client = FakeWorkflowClient()
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "team_id": "T123",
            "type": "event_callback",
            "event": {
                "type": "message",
                "text": "hello",
            },
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            result = provider_module.handle_slack_event(payload, gestalt.Request())

        verify_slack_signature.assert_called_once_with(payload, mock.ANY)
        load_default_workflow_event_subject.assert_called_once_with()
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        request = workflow_client.deliver_event_requests[0]
        self.assertEqual(request.provider_name, "")
        event = request.event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.id, "slack_v2:Ev123")
        self.assertEqual(event.source, "slack_v2")
        self.assertEqual(event.type, "slack_v2.event.received")
        self.assertEqual(event.subject, "slack_agent_default")
        self.assertEqual(event.data["slack"]["event_id"], "Ev123")
        self.assertEqual(
            result,
            {
                "ok": True,
                "delivered": True,
                "app_id": "A123",
                "workflow_event_subject": "slack_agent_default",
                "workflow_event_id": "slack_v2:Ev123",
                "workflow_provider": "",
            },
        )

    @mock.patch("provider.load_default_signing_secret", return_value="signing-secret")
    def test_verify_slack_signature_accepts_matching_event_signature(
        self, load_default_signing_secret: mock.Mock
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }

        result = provider_module._verify_slack_signature(
            payload, signed_slack_request(payload)
        )

        load_default_signing_secret.assert_called_once_with()
        self.assertTrue(result)

    @mock.patch("provider.load_default_signing_secret", return_value="signing-secret")
    def test_verify_slack_signature_accepts_workflow_context_headers(
        self, load_default_signing_secret: mock.Mock
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }

        result = provider_module._verify_slack_signature(
            payload, signed_slack_workflow_context_request(payload)
        )

        load_default_signing_secret.assert_called_once_with()
        self.assertTrue(result)

    @mock.patch("provider.load_default_signing_secret", return_value="signing-secret")
    def test_verify_slack_signature_rejects_mismatched_event_signature(
        self, load_default_signing_secret: mock.Mock
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }

        result = provider_module._verify_slack_signature(
            payload, signed_slack_request(payload, signing_secret="wrong-secret")
        )

        load_default_signing_secret.assert_called_once_with()
        self.assertFalse(result)

    @mock.patch("provider.load_default_signing_secret", return_value="signing-secret")
    def test_verify_slack_signature_uses_raw_request_body(
        self, load_default_signing_secret: mock.Mock
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }
        raw_body = b'{"type":"event_callback","event_id":"Ev123","api_app_id":"A123"}'

        result = provider_module._verify_slack_signature(
            payload, signed_slack_workflow_context_request(payload, raw_body=raw_body)
        )

        load_default_signing_secret.assert_called_once_with()
        self.assertTrue(result)

    @mock.patch("provider.load_default_signing_secret", return_value="signing-secret")
    def test_verify_slack_signature_rejects_stale_timestamp(
        self, load_default_signing_secret: mock.Mock
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }
        stale_timestamp = int(time.time()) - 301

        result = provider_module._verify_slack_signature(
            payload, signed_slack_request(payload, timestamp=stale_timestamp)
        )

        load_default_signing_secret.assert_not_called()
        self.assertFalse(result)

    @mock.patch("provider.load_default_signing_secret", return_value="signing-secret")
    def test_verify_slack_signature_accepts_url_verification_against_default_registration(
        self, load_default_signing_secret: mock.Mock
    ) -> None:
        payload = {"type": "url_verification", "challenge": "challenge-token"}

        result = provider_module._verify_slack_signature(
            payload, signed_slack_request(payload)
        )

        load_default_signing_secret.assert_called_once_with()
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
