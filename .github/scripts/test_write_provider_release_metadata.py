import pathlib
import subprocess
import tempfile
import textwrap
import unittest


SCRIPT = pathlib.Path(__file__).with_name("write_provider_release_metadata.py")


class WriteProviderReleaseMetadataIntegrationTest(unittest.TestCase):
    def test_prefers_glibc_archive_when_both_linux_variants_exist(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            manifest = root / "manifest.yaml"
            output = root / "dist"
            output.mkdir()

            manifest.write_text(
                textwrap.dedent(
                    """\
                    source: github.com/valon-technologies/gestalt-providers/agent/simple
                    kind: agent
                    """
                ),
                encoding="utf-8",
            )

            (output / "gestalt-plugin-simple_v0.0.1-alpha.8_linux_amd64.tar.gz").write_bytes(
                b"glibc"
            )
            (output / "gestalt-plugin-simple_v0.0.1-alpha.8_linux_amd64_musl.tar.gz").write_bytes(
                b"musl"
            )

            subprocess.run(
                [
                    "python3",
                    str(SCRIPT),
                    "--manifest",
                    str(manifest),
                    "--output-dir",
                    str(output),
                    "--version",
                    "0.0.1-alpha.8",
                ],
                check=True,
            )

            metadata = (output / "provider-release.yaml").read_text(encoding="utf-8")
            self.assertIn("linux/amd64:", metadata)
            self.assertIn(
                "path: gestalt-plugin-simple_v0.0.1-alpha.8_linux_amd64.tar.gz",
                metadata,
            )
            self.assertNotIn(
                "path: gestalt-plugin-simple_v0.0.1-alpha.8_linux_amd64_musl.tar.gz",
                metadata,
            )


if __name__ == "__main__":
    unittest.main()
