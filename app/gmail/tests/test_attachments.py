import base64
import unittest
from email import policy
from email.parser import BytesParser
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt

from internals.mime import MIMEAttachment, MIMEParams, build_mime, decode_base64url
import internals.mime as mime_module
import provider as provider_module


def attachment(
    filename: str = "example.pdf", data: bytes = b"\x00\xffbinary\r\n"
) -> MIMEAttachment:
    return MIMEAttachment(
        filename, "application/pdf", base64.b64encode(data).decode("ascii")
    )


class GmailAttachmentTests(unittest.TestCase):
    def test_multipart_preserves_binary_unicode_name_and_plain_html_alternative(
        self,
    ) -> None:
        file = attachment("contrat-é.pdf")
        raw = build_mime(
            MIMEParams(
                to="recipient@example.com",
                subject="Résumé",
                body="Plain body",
                html_body="<p>HTML body</p>",
                cc="copy@example.com",
                bcc="blind@example.com",
                attachments=(file,),
            )
        )
        parsed = BytesParser(policy=policy.default).parsebytes(decode_base64url(raw))
        self.assertEqual(parsed.get_content_type(), "multipart/mixed")
        self.assertEqual(str(parsed["Subject"]), "Résumé")
        self.assertEqual(str(parsed["Bcc"]), "blind@example.com")
        plain = parsed.get_body(preferencelist=("plain",))
        html = parsed.get_body(preferencelist=("html",))
        assert plain is not None and html is not None
        self.assertEqual(plain.get_content().strip(), "Plain body")
        self.assertEqual(html.get_content().strip(), "<p>HTML body</p>")
        parts = list(parsed.iter_attachments())
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0].get_filename(), "contrat-é.pdf")
        self.assertEqual(
            parts[0].get_payload(decode=True), base64.b64decode(file.content_base64)
        )
        self.assertFalse(parsed.defects)

    def test_empty_attachment_list_keeps_existing_plain_message_behavior(self) -> None:
        raw = build_mime(
            MIMEParams(to="recipient@example.com", subject="Hi", body="Hello")
        )
        parsed = BytesParser(policy=policy.default).parsebytes(decode_base64url(raw))
        self.assertFalse(parsed.is_multipart())
        self.assertEqual(parsed.get_content(), "Hello")

    def test_multiple_files_and_empty_files_round_trip(self) -> None:
        raw = build_mime(
            MIMEParams(
                to="recipient@example.com",
                subject="Hi",
                body="Hello",
                attachments=(attachment(data=b""), attachment("second.pdf", b"next")),
            )
        )
        parsed = BytesParser(policy=policy.default).parsebytes(decode_base64url(raw))
        self.assertEqual(
            [part.get_payload(decode=True) for part in parsed.iter_attachments()],
            [b"", b"next"],
        )

    def test_attachment_count_and_combined_decoded_size_are_bounded(self) -> None:
        with self.assertRaisesRegex(ValueError, "At most"):
            build_mime(
                MIMEParams(
                    to="a@example.com",
                    subject="Hi",
                    body="Hi",
                    attachments=(attachment(),) * 21,
                )
            )
        with mock.patch.object(mime_module, "MAX_ATTACHMENT_BYTES", 5):
            build_mime(
                MIMEParams(
                    to="a@example.com",
                    subject="Hi",
                    body="Hi",
                    attachments=(attachment(data=b"12345"),),
                )
            )
            with self.assertRaisesRegex(ValueError, "20 MiB"):
                build_mime(
                    MIMEParams(
                        to="a@example.com",
                        subject="Hi",
                        body="Hi",
                        attachments=(
                            attachment(data=b"123"),
                            attachment("other.pdf", b"456"),
                        ),
                    )
                )

    def test_malformed_base64_mime_and_filename_inputs_fail_before_send(self) -> None:
        cases = [
            MIMEAttachment("good.pdf", "application/pdf", "%%%"),
            MIMEAttachment("good.pdf", "application/pdf", "Zh=="),
            MIMEAttachment("good.pdf", "application/pdf", "Zg="),
            MIMEAttachment("good.pdf", "application/pdf\r\nX-Bad: yes", "Zg=="),
            MIMEAttachment("bad\r\nBcc: evil@example.com", "application/pdf", "Zg=="),
            MIMEAttachment("../bad.pdf", "application/pdf", "Zg=="),
            MIMEAttachment("bad\\name.pdf", "application/pdf", "Zg=="),
        ]
        for file in cases:
            with self.subTest(file=file), self.assertRaises(ValueError):
                build_mime(
                    MIMEParams(
                        to="a@example.com", subject="Hi", body="Hi", attachments=(file,)
                    )
                )

    def test_recipient_and_subject_headers_cannot_inject_fields(self) -> None:
        for field in ("to", "cc", "bcc", "subject"):
            values: dict[str, Any] = {
                "to": "a@example.com",
                "subject": "Hi",
                "body": "Hi",
                "attachments": (attachment(),),
                field: "safe@example.com\r\nBcc: evil@example.com",
            }
            with self.subTest(field=field), self.assertRaises(ValueError):
                build_mime(MIMEParams(**values))

    def test_send_create_and_update_publish_attachment_schemas_and_valid_mime(
        self,
    ) -> None:
        file = attachment()
        model_file = provider_module.AttachmentInput(
            filename=file.filename,
            content_type=file.content_type,
            content_base64=file.content_base64,
        )
        operations = [
            (
                provider_module.messages_send,
                provider_module.SendMessageInput,
                "post_json",
                False,
            ),
            (
                provider_module.drafts_create,
                provider_module.CreateDraftInput,
                "post_json",
                True,
            ),
            (
                provider_module.drafts_update,
                provider_module.UpdateDraftInput,
                "put_json",
                True,
            ),
        ]
        for handler, model, method, nested in operations:
            kwargs: dict[str, Any] = {
                "to": "recipient@example.com",
                "subject": "Example",
                "body": "Hello",
                "attachments": [model_file],
            }
            if model is provider_module.UpdateDraftInput:
                kwargs["id"] = "draft-1"
            with (
                self.subTest(operation=handler.__name__),
                mock.patch.object(
                    provider_module.client_module,
                    method,
                    return_value={"id": "result-1"},
                ) as send,
            ):
                result = handler(
                    model(**kwargs), gestalt.Request(token="synthetic-token")
                )
                self.assertIsInstance(result, dict)
                payload = send.call_args.args[1]
                raw = payload["message"]["raw"] if nested else payload["raw"]
                parsed = BytesParser(policy=policy.default).parsebytes(
                    decode_base64url(raw)
                )
                self.assertEqual(
                    next(parsed.iter_attachments()).get_payload(decode=True),
                    base64.b64decode(file.content_base64),
                )

    def test_runtime_dispatch_decodes_nested_attachment_objects(self) -> None:
        file = attachment()
        with mock.patch.object(
            provider_module.client_module, "post_json", return_value={"id": "sent-1"}
        ) as send:
            result = provider_module.app.execute(
                "messages.send",
                {
                    "to": "recipient@example.com",
                    "subject": "Example",
                    "body": "Hello",
                    "attachments": [
                        {
                            "filename": file.filename,
                            "content_type": file.content_type,
                            "content_base64": file.content_base64,
                        }
                    ],
                },
                gestalt.Request(token="synthetic-token"),
            )
        self.assertEqual(result.status, HTTPStatus.OK)
        parsed = BytesParser(policy=policy.default).parsebytes(
            decode_base64url(send.call_args.args[1]["raw"])
        )
        self.assertEqual(next(parsed.iter_attachments()).get_filename(), "example.pdf")
        catalog = provider_module.app.catalog_dict()
        for operation_id in ("messages.send", "drafts.create", "drafts.update"):
            operation = next(
                item for item in catalog["operations"] if item["id"] == operation_id
            )
            self.assertTrue(
                any(
                    item["name"] == "attachments" and item["type"] == "array"
                    for item in operation["parameters"]
                )
            )

    def test_invalid_attachments_return_400_without_contacting_gmail(self) -> None:
        for handler, model in [
            (provider_module.messages_send, provider_module.SendMessageInput),
            (provider_module.drafts_create, provider_module.CreateDraftInput),
            (provider_module.drafts_update, provider_module.UpdateDraftInput),
        ]:
            kwargs: dict[str, Any] = {
                "to": "recipient@example.com",
                "subject": "Example",
                "body": "Hello",
                "attachments": [
                    provider_module.AttachmentInput(
                        filename="example.pdf",
                        content_type="application/pdf",
                        content_base64="bad!",
                    )
                ],
            }
            if model is provider_module.UpdateDraftInput:
                kwargs["id"] = "draft-1"
            with (
                self.subTest(operation=handler.__name__),
                mock.patch.object(provider_module.client_module, "post_json") as post,
                mock.patch.object(provider_module.client_module, "put_json") as put,
            ):
                result = handler(
                    model(**kwargs), gestalt.Request(token="synthetic-token")
                )
                self.assertEqual(
                    cast(gestalt.Response, result).status, HTTPStatus.BAD_REQUEST
                )
                post.assert_not_called()
                put.assert_not_called()


if __name__ == "__main__":
    unittest.main()
