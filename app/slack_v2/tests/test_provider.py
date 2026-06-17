import unittest
from http import HTTPStatus
from unittest import mock

import gestalt

import provider as provider_module


class FakeWorkflowClient:
    def __init__(self, *, fail: bool = False, definition: gestalt.WorkflowDefinition | None = None) -> None:
        self.fail = fail
        self.definition = definition
        self.deliver_event_requests: list[gestalt.WorkflowDeliverEvent] = []
        self.get_definition_requests: list[gestalt.WorkflowGetDefinition] = []

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def get_definition(
        self, request: gestalt.WorkflowGetDefinition
    ) -> gestalt.WorkflowDefinition:
        self.get_definition_requests.append(request)
        if self.definition is None:
            raise RuntimeError("workflow definition not found")
        return self.definition

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
                workflow_definition_id="cfg_slack_agent_default",
            ),
            gestalt.Request(),
        )

        save_registration.assert_called_once_with(
            app_id="A123",
            client_id="123.456",
            client_secret="client-secret",
            signing_secret="signing-secret",
            display_name="Test Bot",
            workflow_definition_id="cfg_slack_agent_default",
        )
        self.assertEqual(
            result,
            {
                "ok": True,
                "app_id": "A123",
                "display_name": "Test Bot",
                "workflow_definition_id": "cfg_slack_agent_default",
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
                workflow_definition_id="cfg_slack_agent_default",
            ),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "app_id is required"})

    @mock.patch("provider.load_workflow_definition_id_for_app")
    def test_get_workflow_definition_id_for_app_returns_registration(
        self, load_workflow_definition_id: mock.Mock
    ) -> None:
        load_workflow_definition_id.return_value = "cfg_slack_agent_default"

        result = provider_module.get_workflow_definition_id_for_app(
            provider_module.GetWorkflowDefinitionIdForAppInput(app_id="A123"),
            gestalt.Request(),
        )

        load_workflow_definition_id.assert_called_once_with(app_id="A123")
        self.assertEqual(
            result,
            {
                "app_id": "A123",
                "workflow_definition_id": "cfg_slack_agent_default",
            },
        )

    def test_get_workflow_definition_id_for_app_requires_app_id(self) -> None:
        result = provider_module.get_workflow_definition_id_for_app(
            provider_module.GetWorkflowDefinitionIdForAppInput(app_id="  "),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "app_id is required"})

    @mock.patch("provider.load_workflow_definition_id_for_app")
    def test_get_workflow_definition_id_for_app_returns_not_found(
        self, load_workflow_definition_id: mock.Mock
    ) -> None:
        load_workflow_definition_id.side_effect = gestalt.NotFoundError("missing")

        result = provider_module.get_workflow_definition_id_for_app(
            provider_module.GetWorkflowDefinitionIdForAppInput(app_id="A404"),
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "registration not found for app_id 'A404'"}
        )

    def test_handle_slack_event_returns_url_verification_challenge(self) -> None:
        result = provider_module.handle_slack_event(
            {"type": "url_verification", "challenge": "challenge-token"},
            gestalt.Request(),
        )

        self.assertEqual(result, {"challenge": "challenge-token"})

    def test_handle_slack_event_requires_api_app_id(self) -> None:
        result = provider_module.handle_slack_event({}, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(result.body, {"error": "api_app_id is required"})

    @mock.patch("provider.load_workflow_definition_id_for_app")
    def test_handle_slack_event_returns_not_found_for_unknown_app(
        self, load_workflow_definition_id: mock.Mock
    ) -> None:
        load_workflow_definition_id.side_effect = gestalt.NotFoundError("missing")

        result = provider_module.handle_slack_event(
            {"api_app_id": "A404", "type": "event_callback"},
            gestalt.Request(),
        )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body, {"error": "registration not found for app_id 'A404'"}
        )

    @mock.patch("provider.load_workflow_definition_id_for_app")
    def test_handle_slack_event_delivers_workflow_event(
        self, load_workflow_definition_id: mock.Mock
    ) -> None:
        load_workflow_definition_id.return_value = "cfg_slack_agent_default"
        workflow_client = FakeWorkflowClient(
            definition=gestalt.WorkflowDefinition(
                id="cfg_slack_agent_default",
                provider_name="local",
                activations=[
                    gestalt.WorkflowActivation(
                        id="slack_event",
                        event={
                            "match": {
                                "source": "slack_v2",
                                "type": "slack_v2.event.received",
                                "subject": "cfg_slack_agent_default",
                            }
                        },
                    )
                ],
            )
        )
        payload = {
            "api_app_id": "A123",
            "team_id": "T123",
            "type": "event_callback",
            "event": {
                "event_id": "Ev123",
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

        load_workflow_definition_id.assert_called_once_with(app_id="A123")
        self.assertEqual(
            workflow_client.get_definition_requests,
            [
                gestalt.WorkflowGetDefinition(
                    definition_id="cfg_slack_agent_default"
                )
            ],
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        request = workflow_client.deliver_event_requests[0]
        self.assertEqual(request.provider_name, "local")
        event = request.event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.id, "slack_v2:Ev123")
        self.assertEqual(event.source, "slack_v2")
        self.assertEqual(event.type, "slack_v2.event.received")
        self.assertEqual(event.subject, "cfg_slack_agent_default")
        self.assertEqual(
            result,
            {
                "ok": True,
                "delivered": True,
                "app_id": "A123",
                "workflow_definition_id": "cfg_slack_agent_default",
                "workflow_event_id": "slack_v2:Ev123",
                "workflow_provider": "local",
            },
        )

    @mock.patch("provider.load_workflow_definition_id_for_app")
    def test_handle_slack_event_uses_definition_fallback_match_when_lookup_fails(
        self, load_workflow_definition_id: mock.Mock
    ) -> None:
        load_workflow_definition_id.return_value = "cfg_slack_agent_default"
        workflow_client = FakeWorkflowClient(definition=None)
        payload = {
            "api_app_id": "A123",
            "type": "event_callback",
            "event": {"type": "message", "text": "hello"},
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            result = provider_module.handle_slack_event(payload, gestalt.Request())

        request = workflow_client.deliver_event_requests[0]
        event = request.event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.type, "slack_v2.event.received")
        self.assertEqual(event.subject, "cfg_slack_agent_default")
        self.assertTrue(event.id.startswith("slack_v2:A123:"))
        self.assertEqual(
            result["workflow_definition_id"],
            "cfg_slack_agent_default",
        )


if __name__ == "__main__":
    unittest.main()
