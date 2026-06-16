import unittest
from http import HTTPStatus
from unittest import mock

import gestalt

import provider as provider_module


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

    def test_handle_slack_event_returns_hello_world(self) -> None:
        result = provider_module.handle_slack_event({}, gestalt.Request())

        self.assertEqual(result, "hello world")


if __name__ == "__main__":
    unittest.main()
