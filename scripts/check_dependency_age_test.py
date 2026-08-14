#!/usr/bin/env python3
import io
import sys
import unittest
from contextlib import redirect_stderr
from datetime import datetime, timedelta, timezone

import check_dependency_age as cda

GO_MOD_BASE = """module github.com/uber/cadence

go 1.23

require (
\tgithub.com/stretchr/testify v1.8.0
\tgo.uber.org/zap v1.24.0 // indirect
)

require github.com/Shopify/sarama v1.38.1

// require github.com/commented/out v9.9.9
"""

GO_MOD_HEAD = """module github.com/uber/cadence

go 1.23

require (
\tgithub.com/stretchr/testify v1.9.0
\tgo.uber.org/zap v1.24.0 // indirect
\tgithub.com/new/dep v0.1.0
)

require github.com/Shopify/sarama v1.38.1
"""


class ParseTest(unittest.TestCase):
    def test_parses_block_inline_indirect_and_ignores_comments(self):
        got = cda.parse_go_mod_requires(GO_MOD_BASE)
        self.assertEqual(got, {
            "github.com/stretchr/testify": "v1.8.0",
            "go.uber.org/zap": "v1.24.0",
            "github.com/Shopify/sarama": "v1.38.1",
        })


class NewRequirementsTest(unittest.TestCase):
    def test_bumped_and_new_modules_are_reported_unchanged_are_not(self):
        got = sorted(cda.new_requirements(GO_MOD_BASE, GO_MOD_HEAD))
        self.assertEqual(got, [
            ("github.com/new/dep", "v0.1.0"),
            ("github.com/stretchr/testify", "v1.9.0"),
        ])

    def test_new_file_reports_everything(self):
        got = sorted(cda.new_requirements(None, GO_MOD_BASE))
        self.assertEqual(len(got), 3)


class EscapeTest(unittest.TestCase):
    def test_uppercase_letters_are_bang_escaped(self):
        self.assertEqual(
            cda.escape_module_path("github.com/Shopify/sarama"),
            "github.com/!shopify/sarama",
        )


class PseudoVersionTest(unittest.TestCase):
    def test_pseudo_version_timestamp_is_parsed(self):
        got = cda.pseudo_version_time("v0.0.0-20240102150405-abcdef123456")
        self.assertEqual(
            got, datetime(2024, 1, 2, 15, 4, 5, tzinfo=timezone.utc))

    def test_plain_semver_returns_none(self):
        self.assertIsNone(cda.pseudo_version_time("v1.2.3"))


class FindViolationsTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 8, 13, tzinfo=timezone.utc)

    def test_young_version_violates_old_version_passes(self):
        times = {
            ("a.com/young", "v1.0.0"): self.now - timedelta(days=5),
            ("a.com/old", "v1.0.0"): self.now - timedelta(days=20),
        }
        fetch = lambda m, v: times.get((m, v))
        got = cda.find_violations(
            [("a.com/young", "v1.0.0"), ("a.com/old", "v1.0.0")],
            threshold_days=14, now=self.now, fetch_time=fetch)
        self.assertEqual([(m, v) for m, v, _ in got],
                         [("a.com/young", "v1.0.0")])

    def test_unknown_time_warns_but_does_not_violate(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            got = cda.find_violations(
                [("a.com/unknown", "v1.0.0")],
                threshold_days=14, now=self.now, fetch_time=lambda m, v: None)
        self.assertEqual(got, [])
        self.assertIn("WARN", stderr.getvalue())

    def test_pseudo_version_fallback_when_proxy_has_no_time(self):
        pairs = [("a.com/pseudo", "v0.0.0-20260810000000-abcdef123456")]
        got = cda.find_violations(
            pairs, threshold_days=14, now=self.now, fetch_time=lambda m, v: None)
        self.assertEqual(len(got), 1)


GO_MOD_REPLACE_BASE = """module github.com/uber/cadence

go 1.23

require github.com/foo/bar v1.0.0

replace github.com/foo/bar => github.com/foo/bar v1.0.1
"""

GO_MOD_REPLACE_HEAD = """module github.com/uber/cadence

go 1.23

require github.com/foo/bar v1.0.0

replace github.com/foo/bar => github.com/evil/fork v0.0.0-20260812000000-abcdef123456

replace (
\tgithub.com/baz/qux v2.0.0 => github.com/baz/qux v2.1.0
\tgithub.com/local/dep => ../localdep
)
"""


class ParseReplacesTest(unittest.TestCase):
    def test_inline_block_and_filesystem_replaces(self):
        got = cda.parse_go_mod_replaces(GO_MOD_REPLACE_HEAD)
        self.assertEqual(got, {
            "github.com/foo/bar":
                ("github.com/evil/fork", "v0.0.0-20260812000000-abcdef123456"),
            "github.com/baz/qux v2.0.0": ("github.com/baz/qux", "v2.1.0"),
            "github.com/local/dep": None,
        })


class NewReplacementsTest(unittest.TestCase):
    def test_changed_and_added_replacements_reported_filesystem_skipped(self):
        got = sorted(cda.new_replacements(GO_MOD_REPLACE_BASE,
                                          GO_MOD_REPLACE_HEAD))
        self.assertEqual(got, [
            ("github.com/baz/qux", "v2.1.0"),
            ("github.com/evil/fork", "v0.0.0-20260812000000-abcdef123456"),
        ])

    def test_unchanged_replacement_not_reported(self):
        got = cda.new_replacements(GO_MOD_REPLACE_BASE, GO_MOD_REPLACE_BASE)
        self.assertEqual(got, [])


class ProxyErrorTest(unittest.TestCase):
    def test_proxy_error_propagates_out_of_find_violations(self):
        def fetch(_module, _version):
            raise cda.ProxyError("connection refused")
        now = datetime(2026, 8, 13, tzinfo=timezone.utc)
        with self.assertRaises(cda.ProxyError):
            cda.find_violations([("a.com/x", "v1.0.0")], threshold_days=14,
                                now=now, fetch_time=fetch)

    def test_http_404_returns_none_but_transport_error_raises(self):
        import urllib.error

        def opener_404(url, timeout):
            raise urllib.error.HTTPError(url, 404, "not found", {}, None)

        def opener_refused(url, timeout):
            raise urllib.error.URLError("connection refused")

        self.assertIsNone(cda.fetch_publish_time(
            "a.com/x", "v1.0.0", urlopen=opener_404))
        with self.assertRaises(cda.ProxyError):
            cda.fetch_publish_time(
                "a.com/x", "v1.0.0", urlopen=opener_refused, retries=1)


if __name__ == "__main__":
    unittest.main()
