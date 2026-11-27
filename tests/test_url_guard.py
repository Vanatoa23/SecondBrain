"""SSRF / URL-guard tests for pipeline/extractors.py — no DB, no network required.

Covers the _assert_public_url guard inside extract_url: non-http(s) schemes,
loopback / link-local (cloud metadata) / IPv6-loopback hosts, and the
dispatch path extract(source_type="url", ...). The public-URL happy path is
tested by monkeypatching requests.Session.get and socket.getaddrinfo so no real
network call or DNS lookup happens.
"""

import contextlib
import io
import os
import socket
import sys
import unittest.mock as mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.extractors import ExtractionResult, _assert_public_url, extract, extract_url


# --- non-http(s) schemes must raise before any network call -----------------

def test_file_scheme_refused():
    try:
        extract_url("file:///etc/passwd")
    except ValueError:
        return
    raise AssertionError("extract_url('file:///etc/passwd') did not raise ValueError")


def test_ftp_scheme_refused():
    try:
        extract_url("ftp://example.com/x")
    except ValueError:
        return
    raise AssertionError("extract_url('ftp://example.com/x') did not raise ValueError")


# --- non-public resolved IPs must raise --------------------------------------

def test_loopback_ipv4_refused():
    try:
        extract_url("http://127.0.0.1:5433/")
    except ValueError:
        return
    raise AssertionError("extract_url('http://127.0.0.1:5433/') did not raise ValueError")


def test_cloud_metadata_endpoint_refused():
    try:
        extract_url("http://169.254.169.254/latest/meta-data")
    except ValueError:
        return
    raise AssertionError("extract_url('http://169.254.169.254/latest/meta-data') did not raise ValueError")


def test_loopback_ipv6_refused():
    try:
        extract_url("http://[::1]/")
    except ValueError:
        return
    raise AssertionError("extract_url('http://[::1]/') did not raise ValueError")


def test_localhost_refused_via_dispatch():
    try:
        extract(source_type="url", payload="http://localhost/x")
    except ValueError:
        return
    raise AssertionError("extract(source_type='url', payload='http://localhost/x') did not raise ValueError")


# --- public URL passes validation, then the (mocked) fetch runs --------------

class _FakeStreamResponse:
    """Minimal stand-in for a streamed requests.Response (allow_redirects=False,
    stream=True): status_code, headers, iter_content, raise_for_status, close."""

    def __init__(self, status_code=200, headers=None, body=b"", encoding="utf-8"):
        self.status_code = status_code
        self.headers = dict(headers or {})
        self._body = body
        self.encoding = encoding
        self.closed = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
        return None

    def iter_content(self, chunk_size=65536):
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i:i + chunk_size]

    def close(self):
        self.closed = True


# DNS mock: example.com resolves to a public IP; every other host falls through
# to the REAL getaddrinfo (so IP literals like 169.254.169.254 resolve to
# themselves and the guard rejects them on the address check).
_REAL_GETADDRINFO = socket.getaddrinfo
_PUBLIC_DNS = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]


def _fake_dns(host, *args, **kwargs):
    if host == "example.com":
        return _PUBLIC_DNS
    return _REAL_GETADDRINFO(host, *args, **kwargs)


def test_public_url_fetches_with_mocked_network():
    fake = _FakeStreamResponse(
        body=b"<html><body><p>Hello article</p></body></html>"
    )
    with mock.patch("socket.getaddrinfo", side_effect=_fake_dns), \
         mock.patch("requests.Session.get", return_value=fake) as fake_get:
        result = extract_url("https://example.com/article")
    assert isinstance(result, ExtractionResult), type(result)
    assert "Hello article" in result.text, result.text
    assert result.mime_type == "text/html"
    assert result.source_uri == "https://example.com/article"
    # the guard passed, so the session fetch was called exactly once, no auto
    # redirects, streamed, with timeout=30
    assert fake_get.call_count == 1
    assert fake_get.call_args.kwargs.get("timeout") == 30
    assert fake_get.call_args.kwargs.get("allow_redirects") is False
    assert fake_get.call_args.kwargs.get("stream") is True


# --- redirect handling (SSRF redirect-bypass regression) ----------------------
# extract_url follows redirects MANUALLY and re-runs _assert_public_url on
# every Location before requesting it.


def test_redirect_to_cloud_metadata_refused():
    """302 -> http://169.254.169.254/... must raise ValueError BEFORE the
    second request is made (the Location is re-validated)."""
    redirect = _FakeStreamResponse(
        status_code=302, headers={"Location": "http://169.254.169.254/latest/meta-data"}
    )
    with mock.patch("socket.getaddrinfo", side_effect=_fake_dns), \
         mock.patch("requests.Session.get", return_value=redirect) as fake_get:
        try:
            extract_url("https://example.com/start")
        except ValueError:
            pass
        else:
            raise AssertionError("302 to the metadata endpoint was not refused")
    assert fake_get.call_count == 1, "the redirect target must never be requested"


def test_redirect_to_public_url_followed_and_revalidated():
    """302 -> another public URL (relative Location) is followed; the target
    is re-validated (getaddrinfo is consulted for it) and its content returned."""
    redirect = _FakeStreamResponse(
        status_code=302, headers={"Location": "/final"}  # relative Location
    )
    final = _FakeStreamResponse(
        body=b"<html><body><p>Final destination text</p></body></html>"
    )
    dns_hosts = []

    def recording_dns(host, *args, **kwargs):
        dns_hosts.append(host)
        return _fake_dns(host, *args, **kwargs)

    with mock.patch("socket.getaddrinfo", side_effect=recording_dns), \
         mock.patch("requests.Session.get", side_effect=[redirect, final]) as fake_get:
        result = extract_url("https://example.com/start")
    assert "Final destination text" in result.text, result.text
    assert result.source_uri == "https://example.com/start"
    assert fake_get.call_count == 2
    # second request went to the resolved absolute URL
    assert fake_get.call_args_list[1].args[0] == "https://example.com/final"
    # both hops were DNS-validated (initial URL + redirect target)
    assert dns_hosts.count("example.com") == 2, dns_hosts


def test_too_many_redirects_refused():
    """More than 5 redirect hops -> ValueError (loop is capped)."""
    responses = [
        _FakeStreamResponse(
            status_code=302, headers={"Location": f"https://example.com/hop{i}"}
        )
        for i in range(10)
    ]
    with mock.patch("socket.getaddrinfo", side_effect=_fake_dns), \
         mock.patch("requests.Session.get", side_effect=responses) as fake_get:
        try:
            extract_url("https://example.com/start")
        except ValueError as exc:
            assert "redirect" in str(exc).lower(), exc
        else:
            raise AssertionError("unbounded redirect chain was not refused")
    # 1 initial request + 5 followed redirects, then refusal (never the 7th)
    assert fake_get.call_count == 6, fake_get.call_count


def test_oversize_body_refused():
    """A streamed body larger than the 10 MB cap aborts with ValueError."""
    big = _FakeStreamResponse(body=b"x" * (10 * 1024 * 1024 + 1))
    with mock.patch("socket.getaddrinfo", side_effect=_fake_dns), \
         mock.patch("requests.Session.get", return_value=big):
        try:
            extract_url("https://example.com/big")
        except ValueError as exc:
            assert "cap" in str(exc).lower() or "byte" in str(exc).lower(), exc
        else:
            raise AssertionError("oversize body was not refused")


def test_redirect_without_location_refused():
    """A 302 with no Location header is a clean ValueError, not a crash."""
    redirect = _FakeStreamResponse(status_code=302, headers={})
    with mock.patch("socket.getaddrinfo", side_effect=_fake_dns), \
         mock.patch("requests.Session.get", return_value=redirect):
        try:
            extract_url("https://example.com/start")
        except ValueError:
            pass
        else:
            raise AssertionError("302 without Location did not raise ValueError")


# --- non-global address ranges (audit-E/F LOW): CGNAT / NAT64 / 192.0.0.9 ----
# _assert_public_url now rejects ANY resolved IP with `not ip.is_global`
# (covers 100.64.0.0/10 CGNAT shared space) plus the NAT64 well-known prefix
# 64:ff9b::/96 and 192.0.0.9/192.0.0.10 explicitly, on top of the existing
# loopback/private/link-local/reserved/multicast/unspecified checks.


def test_cgnat_shared_address_refused():
    try:
        extract_url("http://100.64.0.1/")
    except ValueError:
        return
    raise AssertionError("extract_url('http://100.64.0.1/') (CGNAT 100.64.0.0/10) did not raise ValueError")


def test_nat64_well_known_prefix_refused():
    try:
        extract_url("http://[64:ff9b::7f00:1]/")
    except ValueError:
        return
    raise AssertionError("extract_url('http://[64:ff9b::7f00:1]/') (NAT64 64:ff9b::/96) did not raise ValueError")


def test_port_control_anycast_192_0_0_9_refused():
    try:
        extract_url("http://192.0.0.9/")
    except ValueError:
        return
    raise AssertionError("extract_url('http://192.0.0.9/') did not raise ValueError")


def test_public_ip_passes_validation_with_mocked_dns():
    """A hostname resolving to a public IP passes _assert_public_url; the same
    host resolving to a CGNAT address is refused (mocked getaddrinfo)."""
    public = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 80))]
    cgnat = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("100.64.0.1", 80))]
    nat64 = [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("64:ff9b::7f00:1", 80, 0, 0))]
    with mock.patch("socket.getaddrinfo", return_value=public):
        _assert_public_url("http://public.example.org/page")  # must not raise
    for bad_dns in (cgnat, nat64):
        with mock.patch("socket.getaddrinfo", return_value=bad_dns):
            try:
                _assert_public_url("http://public.example.org/")
            except ValueError:
                pass
            else:
                raise AssertionError(f"mocked DNS answer {bad_dns[0][4][0]} was not refused")


# --- vision error sanitization (audit-E/F LOW) -------------------------------
# ALL error text on the vision path (raised errors + the stderr fallback print
# in extract_image) goes through pipeline.extractors._sanitize, which strips
# the VISION_API_KEY value, Bearer ..., Authorization: ..., and URL-embedded
# api_key= params.


def test_vision_error_text_is_sanitized():
    import requests
    from PIL import Image
    from pipeline import extractors

    secret = "vk-SECRET"
    env = {
        "VISION_API_URL": f"https://vision.example.test/v1?api_key={secret}",
        "VISION_MODEL": "vision-test-model",
        "VISION_API_KEY": secret,
    }
    # a real tiny PNG so PIL can open the payload without mocking Image
    buf = io.BytesIO()
    Image.new("RGB", (8, 8), (255, 0, 0)).save(buf, format="PNG")
    png_bytes = buf.getvalue()

    # a provider error that echoes the request URL (with ?api_key=...) and
    # the Authorization header — exactly what requests exceptions carry
    leaky = (
        "HTTPSConnectionPool(host='vision.example.test', port=443): Max retries "
        f"exceeded with url: /v1/chat/completions?api_key={secret} (Caused by "
        f"ConnectTimeoutError(...)) request headers: Authorization: Bearer {secret}, "
        "Accept-Encoding: gzip"
    )

    with mock.patch.dict(os.environ, env), \
         mock.patch("requests.post", side_effect=requests.exceptions.ConnectTimeout(leaky)):
        # (1) the raised error from _extract_image_vision carries no secret
        try:
            extractors._extract_image_vision(png_bytes)
        except RuntimeError as exc:
            msg = str(exc)
        else:
            raise AssertionError("_extract_image_vision did not raise on ConnectTimeout")
        assert secret not in msg, msg
        assert "Bearer" not in msg, msg
        assert "api_key=" + secret not in msg, msg
        assert "<redacted" in msg, msg

        # (2) the stderr fallback print in extract_image is sanitized too,
        # and the OCR fallback still runs
        fake_ocr = mock.Mock(
            return_value=ExtractionResult(text="ocr text", mime_type="image/*", source_uri=None)
        )
        with mock.patch.object(extractors, "_extract_image_ocr", fake_ocr):
            err_buf = io.StringIO()
            with contextlib.redirect_stderr(err_buf):
                res = extractors.extract_image(png_bytes, source_uri="mem://img")
        assert fake_ocr.call_count == 1
        assert res.text == "ocr text"
        err = err_buf.getvalue()
        assert "falling back to OCR" in err, err
        assert secret not in err, err
        assert "Bearer" not in err, err
        assert "api_key=" + secret not in err, err


def test_sanitize_strips_all_secret_forms():
    from pipeline.extractors import _sanitize

    with mock.patch.dict(os.environ, {"VISION_API_KEY": "vk-SECRET",
                                      "GITHUB_TOKEN": "gh-TOKEN"}):
        out = _sanitize(
            "key vk-SECRET and gh-TOKEN; Bearer abc.def-123; "
            "Authorization: Bearer abc.def-123; "
            "https://x.test/v1?api_key=vk-SECRET&other=1"
        )
    assert "vk-SECRET" not in out, out
    assert "gh-TOKEN" not in out, out
    assert "abc.def-123" not in out, out
    assert "Bearer" not in out and "bearer <redacted>" in out, out
    assert "Authorization: <redacted>" in out, out
    # F-B (audit-B F2/A4): the WHOLE URL query string / fragment is redacted
    # (bridge-fix pattern), not just the api_key value — no query secret can
    # survive in error text.
    assert "https://x.test/v1?<redacted>" in out, out
    assert "api_key" not in out and "other=1" not in out, out


# --- github blob sha validation (audit-E/F LOW) ------------------------------
# _fetch_blob_text validates the sha against ^[0-9a-f]{40,64}$ BEFORE any URL
# is built or any request is made.


def test_github_blob_sha_validation():
    import base64 as _b64
    from pipeline.github_import import _fetch_blob_text

    bad_shas = ["", "abc123", "z" * 40, "a" * 39, "a" * 65, "A" * 40,
                "../etc/passwd", "a" * 40 + "\n", None, 123]
    with mock.patch("requests.get") as get:
        for bad in bad_shas:
            try:
                _fetch_blob_text("https://api.github.com/repos/o/r", bad)
            except ValueError:
                pass
            else:
                raise AssertionError(f"bad blob sha {bad!r} did not raise ValueError")
        assert get.call_count == 0, "no request may be made for an invalid sha"

    class _Resp:
        status_code = 200
        text = ""

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    payload = {"encoding": "base64",
               "content": _b64.b64encode(b"hello repo file").decode()}
    for good in ("a" * 40, "0123456789abcdef" * 4):  # 40 and 64 hex chars
        with mock.patch("requests.get", return_value=_Resp(payload)) as get:
            text = _fetch_blob_text("https://api.github.com/repos/o/r", good)
        assert text == "hello repo file", text
        assert get.call_count == 1
        assert get.call_args.args[0].endswith(f"/git/blobs/{good}")


if __name__ == "__main__":
    failed = 0
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 — report and continue
                failed += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
            else:
                print(f"PASS {name}")
    if failed:
        print(f"{failed} test(s) FAILED.")
        sys.exit(1)
    print("All tests passed.")
