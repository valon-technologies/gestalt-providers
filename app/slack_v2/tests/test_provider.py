import unittest

import gestalt

import provider as provider_module


class SlackV2ProviderTests(unittest.TestCase):
    def test_handle_slack_event_returns_hello_world(self) -> None:
        result = provider_module.handle_slack_event({}, gestalt.Request())

        self.assertEqual(result, "hello world")


if __name__ == "__main__":
    unittest.main()
