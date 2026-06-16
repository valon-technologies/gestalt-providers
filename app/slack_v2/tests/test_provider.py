import unittest

import gestalt

import provider as provider_module


class SlackV2ProviderTests(unittest.TestCase):
    def test_register_slack_event_is_noop(self) -> None:
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

        self.assertEqual(result, {})

    def test_handle_slack_event_returns_hello_world(self) -> None:
        result = provider_module.handle_slack_event({}, gestalt.Request())

        self.assertEqual(result, "hello world")


if __name__ == "__main__":
    unittest.main()
