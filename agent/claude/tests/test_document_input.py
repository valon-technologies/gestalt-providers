from __future__ import annotations

import asyncio
import base64
import unittest
from types import SimpleNamespace
from unittest import mock

import httpx

from internals import document_input as pdf
from internals.config import ClaudeAgentConfig

URL = "https://files.example.test/private/download/synthetic-capability"
PREFIXES = ("https://files.example.test/private/download/",)
PDF = b"%PDF-1.7\nsynthetic test input"


def messages(url: str = URL, mime: str = "application/pdf") -> list[dict]:
    return [{"role": "user", "text": "Transcribe", "parts": [{"image_ref": {"uri": url, "mime_type": mime}}]}]


class DocumentInputTests(unittest.IsolatedAsyncioTestCase):
    def client(self, handler):
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=False)

    def test_explicit_configuration_and_exact_url_contract(self):
        self.assertEqual(pdf.document_urls(messages(), PREFIXES), [URL])
        self.assertEqual(pdf.document_urls([{"role": "user", "text": "ordinary text"}], ()), [])
        for url in [
            URL + "\n",
            URL + "\t",
            URL + "#x",
            URL + "?x=y",
            URL.replace("https:", "http:"),
            URL.replace("files.example.test", "@files.example.test"),
            URL.replace("files.example.test", "files.example.test.evil"),
            URL.replace("files.example.test", "user:secret@bad／.example"),
            URL.replace("files.example.test", "files.example.test:broken"),
            URL.replace("synthetic-capability", "%2e%2e"),
        ]:
            with self.subTest(url=url), self.assertRaises(pdf.DocumentInputError) as caught:
                pdf.document_urls(messages(url), PREFIXES)
            self.assertNotIn(url, str(caught.exception))
            self.assertNotIn("secret", str(caught.exception))
        for inputs, prefixes in [(messages(), ()), (messages() * 2, PREFIXES)]:
            with self.assertRaises(pdf.DocumentInputError):
                pdf.document_urls(inputs, prefixes)
        for prefixes in [(), PREFIXES]:
            self.assertEqual(pdf.document_urls(messages(mime="image/png"), prefixes), [])
        from internals.claude_runner import _messages_to_prompt

        self.assertIn('"image_ref"', _messages_to_prompt(messages(mime="image/png")))
        self.assertNotIn(URL, _messages_to_prompt(messages()))
        config = ClaudeAgentConfig.from_dict(name="claude", raw_config={"documentUrlPrefixes": list(PREFIXES)})
        self.assertEqual(config.document_url_prefixes, PREFIXES)
        for prefix in [
            "http://files.example.test/private/",
            "https://files.example.test/",
            "https://user:secret@files.example.test/private/",
            "https://files.example.test/private/?query",
        ]:
            with self.assertRaises(ValueError):
                ClaudeAgentConfig.from_dict(name="claude", raw_config={"documentUrlPrefixes": [prefix]})

    async def test_native_document_block_contains_original_bytes_not_reference_text(self):
        with mock.patch.object(pdf, "_download_pdf", new=mock.AsyncMock(return_value=PDF)):
            result = [message async for message in pdf.native_document_prompt("Transcribe page 1", [URL])]
        content = result[0]["message"]["content"]
        self.assertEqual(content[0], {"type": "text", "text": "Transcribe page 1"})
        self.assertEqual(
            content[1],
            {
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": base64.b64encode(PDF).decode()},
            },
        )
        self.assertNotIn(URL, str(result))
        with (
            mock.patch.object(pdf, "_download_pdf", new=mock.AsyncMock(return_value=PDF)),
            mock.patch.object(pdf, "MAX_NATIVE_INPUT_BYTES", 200),
        ):
            with self.assertRaisesRegex(pdf.DocumentInputError, "native input limit"):
                [message async for message in pdf.native_document_prompt("x" * 1000, [URL])]

    async def test_private_url_is_redacted_from_normalized_httpx_and_httpcore_logs(self):
        import logging

        for url in [
            URL,
            URL.replace("files.example.test", "files.example.test:443"),
            URL.replace("files.example.test", "FILES.EXAMPLE.TEST"),
        ]:
            with (
                self.assertLogs("httpx", level="INFO") as captured,
                mock.patch.object(
                    pdf.httpx, "AsyncClient", return_value=self.client(lambda _: httpx.Response(200, content=PDF))
                ),
            ):
                self.assertEqual(await pdf._download_pdf(url), PDF)
            self.assertNotIn("synthetic-capability", str(captured.output))
            self.assertIn("[private PDF link]", str(captured.output))
        with self.assertLogs("httpcore.http11", level="DEBUG") as captured, pdf._redact_download_logs(URL):
            logging.getLogger("httpcore.http11").debug("request target=%r", b"/private/download/synthetic-capability")
        self.assertNotIn("synthetic-capability", str(captured.output))

    async def test_actual_sdk_serializes_native_document_with_session_id(self):
        import json
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

        transport = mock.Mock(write=mock.AsyncMock())
        client = ClaudeSDKClient(options=ClaudeAgentOptions())
        client.__dict__["_query"] = object()
        client.__dict__["_transport"] = transport
        with mock.patch.object(pdf, "_download_pdf", new=mock.AsyncMock(return_value=PDF)):
            await client.query(pdf.native_document_prompt("Verbatim page 1", [URL]), session_id="synthetic-session")
        wire = json.loads(transport.write.await_args.args[0])
        self.assertEqual(wire["session_id"], "synthetic-session")
        self.assertEqual(wire["message"]["content"][1]["type"], "document")
        self.assertEqual(base64.b64decode(wire["message"]["content"][1]["source"]["data"]), PDF)
        self.assertNotIn(URL, str(wire))

    async def test_download_requires_complete_bounded_original_pdf(self):
        with mock.patch.object(
            pdf.httpx, "AsyncClient", return_value=self.client(lambda _: httpx.Response(200, content=PDF))
        ):
            self.assertEqual(await pdf._download_pdf(URL), PDF)
        cases = [
            httpx.Response(302, headers={"Location": "https://evil.test/private"}),
            httpx.Response(200, content=b"not pdf"),
            httpx.Response(200, headers={"Content-Length": "1000"}, content=PDF),
            httpx.Response(200, headers={"Content-Length": "-1"}, content=PDF),
            httpx.Response(200, headers={"Content-Length": str(pdf.MAX_PDF_BYTES + 1)}, content=PDF),
            httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=httpx.ByteStream(PDF)),
        ]
        for response in cases:
            with (
                self.subTest(response=response),
                mock.patch.object(pdf.httpx, "AsyncClient", return_value=self.client(lambda _: response)),
            ):
                with self.assertRaises(pdf.DocumentInputError):
                    await pdf._download_pdf(URL)
        with (
            mock.patch.object(pdf, "MAX_PDF_BYTES", 10),
            mock.patch.object(
                pdf.httpx,
                "AsyncClient",
                return_value=self.client(lambda _: httpx.Response(200, stream=httpx.ByteStream(PDF))),
            ),
        ):
            with self.assertRaisesRegex(pdf.DocumentInputError, "input limit"):
                await pdf._download_pdf(URL)

    async def test_total_deadline_cancels_dripping_stream_and_closes_it(self):
        class Drip(httpx.AsyncByteStream):
            closed = False
            canceled = False

            async def __aiter__(self):
                try:
                    while True:
                        await asyncio.sleep(0.01)
                        yield b"%PDF-"
                except asyncio.CancelledError:
                    self.canceled = True
                    raise

            async def aclose(self):
                self.closed = True

        stream = Drip()
        with (
            mock.patch.object(pdf, "DOWNLOAD_TIMEOUT_SECONDS", 0.035),
            mock.patch.object(
                pdf.httpx, "AsyncClient", return_value=self.client(lambda _: httpx.Response(200, stream=stream))
            ),
        ):
            with self.assertRaisesRegex(pdf.DocumentInputError, "timed out"):
                await pdf._download_pdf(URL)
        self.assertTrue(stream.canceled)
        self.assertTrue(stream.closed)

    async def test_call_cancellation_releases_stream_without_background_thread(self):
        class Blocked(httpx.AsyncByteStream):
            closed = False

            async def __aiter__(self):
                await asyncio.Event().wait()
                yield b"unused"

            async def aclose(self):
                self.closed = True

        stream = Blocked()
        with mock.patch.object(
            pdf.httpx, "AsyncClient", return_value=self.client(lambda _: httpx.Response(200, stream=stream))
        ):
            task = asyncio.create_task(pdf._download_pdf(URL))
            await asyncio.sleep(0.01)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(stream.closed)


class RunnerPdfCancellationTests(unittest.IsolatedAsyncioTestCase):
    def runner(self):
        from internals.claude_runner import ClaudeSDKRunner

        return ClaudeSDKRunner(
            ClaudeAgentConfig.from_dict(name="claude", raw_config={"documentUrlPrefixes": list(PREFIXES)})
        )

    async def invoke(self, runner):
        return await runner._run_turn(
            session=SimpleNamespace(
                session_id="synthetic", tool_source=0, listed_tools=[], metadata={}, prepared_workspace=None
            ),
            turn_id="pdf-cancel",
            model="sonnet",
            messages=messages(),
            request_context=None,
            schema=None,
            permission_mode="dontAsk",
            timeout_seconds=10,
        )

    async def test_cancel_during_download_cancels_input_before_creating_model_client(self):
        from internals import claude_runner

        runner = self.runner()
        started = asyncio.Event()
        canceled = asyncio.Event()

        async def blocked_download(_url):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                canceled.set()
                raise

        with (
            mock.patch.object(pdf, "_download_pdf", new=blocked_download),
            mock.patch.object(claude_runner, "ClaudeSDKClient") as client,
        ):
            task = asyncio.create_task(self.invoke(runner))
            await started.wait()
            runner.cancel_turn("pdf-cancel")
            with self.assertRaises(claude_runner.ClaudeExecutionCanceled):
                await task
            self.assertTrue(canceled.is_set())
            client.assert_not_called()

    async def test_cancel_at_download_completion_never_submits_document(self):
        from internals import claude_runner

        runner = self.runner()

        async def finishes_after_cancel(_url):
            runner.cancel_turn("pdf-cancel")
            return PDF

        with (
            mock.patch.object(pdf, "_download_pdf", new=finishes_after_cancel),
            mock.patch.object(claude_runner, "ClaudeSDKClient") as client,
        ):
            with self.assertRaises(claude_runner.ClaudeExecutionCanceled):
                await self.invoke(runner)
            client.assert_not_called()
