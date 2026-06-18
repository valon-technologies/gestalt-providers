import unittest
from http import HTTPStatus
from unittest import mock

import gestalt

from internals import events
import provider as provider_module


class FakeWorkflowClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.deliver_event_requests: list[gestalt.WorkflowDeliverEvent] = []

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def deliver_event(self, request: gestalt.WorkflowDeliverEvent) -> gestalt.WorkflowEvent:
        self.deliver_event_requests.append(request)
        if self.fail:
            raise RuntimeError("workflow client unavailable")
        event = request.event
        if event is None:
            return gestalt.WorkflowEvent()
        return event


class SlackV2ProviderTests(unittest.TestCase):
    @mock.patch("provider.save_slack_event_registration")
    def test_register_slack_event_persists_registration(self, save_registration: mock.Mock) -> None:
        result = provider_module.register_slack_event(
            provider_module.RegisterSlackEventInput(
                app_id="A123",
                client_id="123.456",
                client_secret="client-secret",
                signing_secret="signing-secret",
                display_name="Test Bot",
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
                workflow_event_subject="slack_agent_default",
            ),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "app_id is required"})

    def test_register_slack_event_requires_workflow_event_subject(self) -> None:
        result = provider_module.register_slack_event(
            provider_module.RegisterSlackEventInput(
                app_id="A123",
                client_id="123.456",
                client_secret="client-secret",
                signing_secret="signing-secret",
                display_name="Test Bot",
            ),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "workflow_event_subject is required"})

    @mock.patch("provider.load_workflow_event_subject_for_app")
    def test_get_workflow_event_subject_for_app_returns_registration(
        self, load_workflow_event_subject: mock.Mock
    ) -> None:
        load_workflow_event_subject.return_value = "slack_agent_default"

        result = provider_module.get_workflow_event_subject_for_app(
            provider_module.GetWorkflowEventSubjectForAppInput(app_id="A123"),
            gestalt.Request(),
        )

        load_workflow_event_subject.assert_called_once_with(app_id="A123")
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

    @mock.patch("provider.load_workflow_event_subject_for_app")
    def test_get_workflow_event_subject_for_app_returns_not_found(
        self, load_workflow_event_subject: mock.Mock
    ) -> None:
        load_workflow_event_subject.side_effect = gestalt.NotFoundError("missing")

        result = provider_module.get_workflow_event_subject_for_app(
            provider_module.GetWorkflowEventSubjectForAppInput(app_id="A404"),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "registration not found for app_id 'A404'"}
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

        result = provider_module.debug_list_smoke_run_payload_ids(
            {}, gestalt.Request()
        )

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

    def test_handle_slack_event_requires_api_app_id(self) -> None:
        result = provider_module.handle_slack_event({}, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "api_app_id is required"})

    @mock.patch("provider._verify_slack_signature", return_value=True)
    @mock.patch("provider.load_workflow_event_subject_for_app")
    def test_handle_slack_event_url_verification_returns_challenge(
        self,
        load_workflow_event_subject: mock.Mock,
        verify_slack_signature: mock.Mock,
    ) -> None:
        payload = {"type": "url_verification", "challenge": "challenge-token"}
        request = gestalt.Request()

        result = provider_module.handle_slack_event(
            payload,
            request,
        )

        verify_slack_signature.assert_called_once_with(payload, request)
        load_workflow_event_subject.assert_not_called()
        self.assertEqual(result, {"challenge": "challenge-token"})

    @mock.patch("provider.load_workflow_event_subject_for_app")
    @mock.patch("provider._verify_slack_signature", return_value=False)
    def test_handle_slack_event_rejects_invalid_signature(
        self,
        verify_slack_signature: mock.Mock,
        load_workflow_event_subject: mock.Mock,
    ) -> None:
        payload = {
            "api_app_id": "A123",
            "event_id": "Ev123",
            "type": "event_callback",
        }
        request = gestalt.Request()

        result = provider_module.handle_slack_event(payload, request)

        verify_slack_signature.assert_called_once_with(payload, request)
        load_workflow_event_subject.assert_not_called()
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(result.body, {"error": "invalid Slack signature"})

    @mock.patch("provider.load_workflow_event_subject_for_app")
    def test_handle_slack_event_requires_event_id(
        self, load_workflow_event_subject: mock.Mock
    ) -> None:
        result = provider_module.handle_slack_event(
            {
                "api_app_id": "A123",
                "type": "event_callback",
                "event": {"event_id": "EvNested"},
            },
            gestalt.Request(),
        )

        load_workflow_event_subject.assert_not_called()
        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "event_id is required"})

    @mock.patch("provider.load_workflow_event_subject_for_app")
    def test_handle_slack_event_returns_not_found_for_unknown_app(
        self, load_workflow_event_subject: mock.Mock
    ) -> None:
        load_workflow_event_subject.side_effect = gestalt.NotFoundError("missing")

        result = provider_module.handle_slack_event(
            {"api_app_id": "A404", "event_id": "Ev404", "type": "event_callback"},
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "registration not found for app_id 'A404'"}
        )

    @mock.patch("provider.load_workflow_event_subject_for_app")
    def test_handle_slack_event_delivers_workflow_event(
        self, load_workflow_event_subject: mock.Mock
    ) -> None:
        load_workflow_event_subject.return_value = "slack_agent_default"
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

        load_workflow_event_subject.assert_called_once_with(app_id="A123")
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


if __name__ == "__main__":
    unittest.main()
