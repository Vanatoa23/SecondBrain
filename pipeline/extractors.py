"""Content extractors — turn raw input into searchable text.

Each extractor returns an ExtractionResult:

    text       — the searchable text that will be embedded
    mime_type  — what the original was
    source_uri — where the original lives (may be None for inline text)

Security note: content fetched from the web (extract_url) is UNTRUSTED DATA.
It is stored as content and returned as data only — never executed, rendered
as instructions, or treated as commands. extract_url additionally refuses to
fetch any URL that is not a public http(s) address (SSRF guard, see
_assert_public_url below).

Phase 5 audit hooks: before the vision POST to {VISION_API_URL}/chat/completions,
audit.check_external_send + an "external_send" event run (see
_audit_gate_external_send); extract_url / extract_youtube log an
"external_fetch" (purpose="ingest") event before fetching inbound content —
with endpoint=<hostname> + url_sha256=<sha256(url)[:16]> only, NEVER the raw
URL (it may carry ?api_key=... secrets). When the repo-root audit module is
not importable these degrade to the local checks only — UNLESS
SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL is enabled, in which case outbound sends
FAIL CLOSED (RuntimeError: audit module unavailable — refusing).

Redirects on the vision POST are followed MANUALLY (allow_redirects=False,
max 3 hops): every 3xx Location is resolved against the current URL and
re-gated (audit.check_external_send + an external_send event with the new
endpoint host) BEFORE the re-POST, so a redirect can never bypass the gate.
"""


from __future__ import annotations

import io
from dataclasses import dataclass


@dataclass
class ExtractionResult:
    text: str
    mime_type: str
    source_uri: str | None


def _sanitize(message) -> str:
    """Strip secrets from an error/log string before it is printed or raised.

    Mirrors pipeline.extract._sanitize, extended for every API key this
    module can touch: the VISION_API_KEY / GITHUB_TOKEN / EXTRACT_API_KEY
    values are removed wherever they occur, plus any Bearer token,
    Authorization header, and URL-embedded api_key= query parameter — so a
    failing provider call (whose exception text often echoes the request
    URL and headers) can never leak credentials into stderr or an exception.
    """
    import os
    import re

    s = str(message)
    # Redact URL query strings / fragments FIRST (a signed URL may carry
    # ?api_key=... secrets): https://host/path?x=y#frag -> https://host/path?<redacted>
    # (same pattern as the bridge _sanitize_error fix).
    s = re.sub(r"(?i)(https?://[^?#\s]+)[?#][^\s]*", r"\1?<redacted>", s)
    s = re.sub(r"(?i)\bbearer\s+[^\s,;\"']+", "bearer <redacted>", s)
    s = re.sub(r"(?i)(authorization\s*[:=]\s*)[^\s,;\"']+", r"\1<redacted>", s)
    s = re.sub(r"(?i)([?&]api_key=)[^&#\s]+", r"\1<redacted>", s)
    for var in ("VISION_API_KEY", "GITHUB_TOKEN", "EXTRACT_API_KEY"):
        val = os.environ.get(var)
        if val:
            s = s.replace(val, "<redacted-api-key>")
    return s


# ---------------------------------------------------------------------------
# Phase 5 audit hooks (repo-root audit.py — worker W1). The audit module is
# imported lazily inside the functions: when it is not importable, outbound
# calls degrade to the local scheme/SSRF checks only, and no events are
# logged. log_event never raises by contract; the wrappers guard anyway.
# ---------------------------------------------------------------------------


def _load_audit():
    """Lazily import the repo-root `audit` module; None when unavailable."""
    try:
        import audit
        return audit
    except Exception:
        return None


def _approval_required() -> bool:
    """True when SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL enables the gate —
    mirrors audit.check_external_send / audit._gate_enabled EXACTLY: unset
    -> False; any value not in ("", "0", "false", "no", "off")
    (case/whitespace-insensitive) -> True (unrecognized values fail closed)."""
    import os

    v = os.environ.get("SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL")
    return v is not None and v.strip().lower() not in ("", "0", "false", "no", "off")


def _safe_host(url: str) -> str | None:
    """Extract hostname without raising ValueError on malformed URLs."""
    from urllib.parse import urlsplit

    try:
        return urlsplit(url).hostname
    except ValueError:
        return None


def _url_origin(url: str) -> tuple[str, str, int | None]:
    """Return normalized (scheme, host, effective_port) for origin comparison."""
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(url)
        scheme = (parts.scheme or "").lower()
        host = (parts.hostname or "").lower()
        port = parts.port
    except ValueError:
        return ("", "", None)
    if port is None:
        port = 443 if scheme == "https" else (80 if scheme == "http" else None)
    return (scheme, host, port)


def _strip_auth_headers(headers: dict) -> dict:
    """Return a copy of headers with Authorization removed (case-insensitive)."""
    return {k: v for k, v in headers.items() if k.lower() != "authorization"}


def _fail_closed_when_audit_missing(audit) -> None:
    """Fail closed (audit-A finding 3): when external approval is required but
    the audit module is not importable, refuse instead of proceeding silently.
    When the requirement is unset, a missing audit module stays a no-op."""
    if audit is None and _approval_required():
        raise RuntimeError(
            "external approval required but audit module unavailable — refusing"
        )


def _audit_gate_external_send(api_url: str, *, purpose: str,
                              model: str | None, payload: dict) -> None:
    """Gate + audit-log an OUTBOUND send (vision POST), BEFORE the request.

    audit.check_external_send(api_url) raises PermissionError when
    SECONDBRAIN_REQUIRE_EXTERNAL_APPROVAL is enabled and the host is not
    allowlisted; the PermissionError becomes a clean RuntimeError through
    _sanitize. After the gate passes, one "external_send" event is logged.

    Fail closed: when approval is required but the audit module is missing,
    raises RuntimeError instead of proceeding (audit-A finding 3).
    """
    import json

    audit = _load_audit()
    if audit is None:
        _fail_closed_when_audit_missing(audit)
        return
    try:
        audit.check_external_send(api_url)
    except PermissionError as exc:
        raise RuntimeError(
            _sanitize(
                f"external send to host {_safe_host(api_url)!r} refused "
                f"(external approval required): {exc}"
            )
        ) from exc
    body_bytes = json.dumps(payload).encode("utf-8")
    try:
        audit.log_event(
            "external_send",
            direction="out",
            endpoint=_safe_host(api_url),
            purpose=purpose,
            model=model,
            payload_bytes=len(body_bytes),
        )
    except Exception:
        pass  # audit logging must never break the caller


def _regate_redirect_hop(new_url: str, *, purpose: str,
                         model: str | None, payload: dict) -> None:
    """Re-gate a redirect hop BEFORE re-POSTing (audit-A finding 2).

    Runs audit.check_external_send(new_url) — the PermissionError propagates
    UNCONVERTED, so a 3xx to a non-allowlisted host is refused before any
    request is made to it — and logs one "external_send" event with the NEW
    endpoint host. Fail closed when approval is required but audit is missing.
    """
    import json

    audit = _load_audit()
    if audit is None:
        _fail_closed_when_audit_missing(audit)
        return
    audit.check_external_send(new_url)  # PermissionError propagates
    body_bytes = json.dumps(payload).encode("utf-8")
    try:
        audit.log_event(
            "external_send",
            direction="out",
            endpoint=_safe_host(new_url),
            purpose=purpose,
            model=model,
            payload_bytes=len(body_bytes),
        )
    except Exception:
        pass  # audit logging must never break the caller


def _audit_log_external_fetch(url: str) -> None:
    """Audit-trail an INBOUND third-party fetch (no gate — ingest side).

    NEVER logs the raw URL — it may carry ?api_key=... secrets (audit-B
    F2/A4): only the endpoint hostname and a 16-hex sha256 of the full URL.
    """
    import hashlib
    from urllib.parse import urlsplit

    audit = _load_audit()
    if audit is None:
        return
    try:
        audit.log_event(
            "external_fetch",
            endpoint=urlsplit(url).hostname,
            url_sha256=hashlib.sha256(url.encode("utf-8")).hexdigest()[:16],
            purpose="ingest",
        )
    except Exception:
        pass  # audit logging must never break the caller


def _assert_public_url(url: str) -> list[str]:
    """SSRF guard: refuse to fetch anything that is not a public http(s) URL.

    Only the http/https schemes are allowed, and the hostname must resolve
    to public IP addresses only. If ANY resolved IP is not globally
    routable (not ip.is_global — this covers loopback, private, link-local
    169.254.0.0/16 cloud metadata, CGNAT shared space 100.64.0.0/10, and
    more), or is loopback/private/link-local/reserved/multicast/unspecified,
    the URL is rejected before any request is made.

    Returns the validated IP addresses (as strings) the hostname resolved
    to — callers PIN the actual fetch to these, so the hostname is resolved
    exactly once and a DNS-rebind between check and connect cannot redirect
    the request (see _PinnedHTTPAdapter / _fetch_pinned below).
    """
    import ipaddress
    import socket
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    if parts.scheme.lower() not in ("http", "https"):
        raise ValueError(f"refusing to fetch URL with non-http(s) scheme: {parts.scheme!r}")
    hostname = parts.hostname
    if not hostname:
        raise ValueError(f"refusing to fetch URL without a hostname: {url!r}")
    port = parts.port or (443 if parts.scheme.lower() == "https" else 80)
    try:
        infos = socket.getaddrinfo(hostname, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise ValueError(f"refusing to fetch URL with unresolvable hostname {hostname!r}: {exc}") from exc
    # Non-global blocks that some interpreter versions still report as
    # is_global=True: CGNAT 100.64.0.0/10, the NAT64 well-known prefix
    # 64:ff9b::/96, and the 192.0.0.9/192.0.0.10 anycast services. Refused
    # explicitly in addition to `not ip.is_global` so the guard holds across
    # Python versions.
    non_public_nets = (
        ipaddress.ip_network("100.64.0.0/10"),
        ipaddress.ip_network("64:ff9b::/96"),
        ipaddress.ip_network("192.0.0.9/32"),
        ipaddress.ip_network("192.0.0.10/32"),
    )
    validated: list[str] = []
    for _family, _socktype, _proto, _canonname, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if (
            not ip.is_global
            or ip.is_loopback
            or ip.is_private
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
            or any(ip in net for net in non_public_nets)
        ):
            raise ValueError("refusing to fetch non-public URL")
        validated.append(str(ip))
    return validated


def extract_text(raw: str, source_uri: str | None = None) -> ExtractionResult:
    """Plain text passed directly (e.g. pasted into the dropbox)."""
    return ExtractionResult(text=raw, mime_type="text/plain", source_uri=source_uri)


# Redirect-following limits for extract_url (SSRF hardening): redirects are
# followed MANUALLY (allow_redirects=False) so every Location header is
# re-validated by _assert_public_url before the next request — requests'
# built-in redirect following would happily chase a 302 to 169.254.169.254.
_MAX_URL_REDIRECTS = 5
# Hard cap on the response body (streamed, so a huge or endless body cannot
# exhaust memory): read in chunks and abort past 10 MB.
_MAX_URL_BYTES = 10 * 1024 * 1024
_REDIRECT_STATUSES = (301, 302, 303, 307, 308)


def _read_capped(resp, max_bytes: int = _MAX_URL_BYTES) -> bytes:
    """Read a streamed response body in chunks, aborting past max_bytes."""
    chunks: list[bytes] = []
    total = 0
    for chunk in resp.iter_content(chunk_size=65536):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise ValueError(
                f"response body exceeds the {max_bytes} byte cap "
                f"({_MAX_URL_BYTES // (1024 * 1024)} MB) — aborting fetch"
            )
        chunks.append(chunk)
    return b"".join(chunks)


def _pinned_adapter(pinned_ip: str, orig_host: str):
    """Build a requests HTTPAdapter whose connection pool dials the
    PRE-VALIDATED `pinned_ip` instead of re-resolving the hostname at connect
    time.

    This closes the DNS-rebind TOCTOU: the hostname is resolved exactly once
    (by _assert_public_url, which also validates every returned IP), and the
    HTTP request is pinned to that result — a second, attacker-controlled
    DNS answer can never redirect the fetch. The original hostname is still
    used where it matters:

      * Host header — set explicitly on the request in send();
      * TLS SNI + certificate hostname — via the connection's
        server_hostname (conn_kw on https pools).
    """
    import requests
    from urllib.parse import urlsplit

    class _PinnedAdapter(requests.adapters.HTTPAdapter):
        def send(self, request, **kwargs):
            parts = urlsplit(request.url)
            default_port = 443 if parts.scheme.lower() == "https" else 80
            host = orig_host
            if ":" in host and not host.startswith("["):
                host = f"[{host}]"  # re-bracket IPv6 literals for the header
            request.headers["Host"] = (
                host if parts.port in (None, default_port) else f"{host}:{parts.port}"
            )
            return super().send(request, **kwargs)

        def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
            pool = super().get_connection_with_tls_context(
                request, verify, proxies=proxies, cert=cert
            )
            # Pin the pool's dial target to the validated IP (pool.host
            # drives the TCP connect; the Host header is explicit and TLS
            # SNI is fixed via server_hostname, so nothing else changes).
            pool.host = pinned_ip
            if getattr(pool, "scheme", "") == "https":
                pool.conn_kw.setdefault("server_hostname", orig_host)
            return pool

    return _PinnedAdapter()


def _fetch_pinned(url: str, ips: list[str], timeout: float = 30):
    """GET `url` through a session whose adapter is pinned to ips[0] — the
    hostname is never resolved again (no proxy either: the dial must be the
    pinned IP). Returns the streamed, non-redirect-following response."""
    import requests
    from urllib.parse import urlsplit

    if not ips:
        raise ValueError(f"no validated IP to pin the fetch of {url!r} to")
    hostname = urlsplit(url).hostname or ""
    adapter = _pinned_adapter(ips[0], hostname)
    session = requests.Session()
    session.trust_env = False  # no env proxies — the dial must be the pinned IP
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    try:
        return session.get(
            url,
            timeout=timeout,
            headers={"User-Agent": "SecondBrain/1.0"},
            allow_redirects=False,  # manual redirect loop — re-pin every hop
            stream=True,            # body read in capped chunks below
        )
    finally:
        session.close()


def extract_url(url: str) -> ExtractionResult:
    """Fetch a URL and extract readable text from the HTML.

    The fetched page is untrusted data (see module docstring). SSRF guard:
    _assert_public_url runs before EVERY request and RETURNS the validated
    IPs; the fetch itself goes through _fetch_pinned, whose adapter dials
    exactly that IP (Host header and TLS SNI stay on the original hostname),
    so the hostname is resolved once per request and a DNS rebind between
    validation and connect cannot redirect it. Redirects are followed
    MANUALLY (allow_redirects=False, max _MAX_URL_REDIRECTS hops; each
    Location, absolute or relative, is re-validated and re-pinned before it
    is fetched). The response is streamed with a hard 10 MB body cap and
    always closed (finally). All error text is routed through _sanitize, so a
    signed URL's query string / fragment can never leak into an exception
    message (audit-B F2/A4).
    """
    try:
        return _extract_url_inner(url)
    except ValueError as exc:
        raise ValueError(_sanitize(str(exc))) from None


def _extract_url_inner(url: str) -> ExtractionResult:
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin

    # Phase 5 audit trail: inbound third-party fetch (no gate) before any request.
    _audit_log_external_fetch(url)

    current = url
    for hop in range(_MAX_URL_REDIRECTS + 1):
        ips = _assert_public_url(current)  # one resolution, returns validated IPs
        resp = _fetch_pinned(current, ips, timeout=30)
        try:
            if resp.status_code in _REDIRECT_STATUSES:
                location = resp.headers.get("Location")
                if not location:
                    raise ValueError(
                        f"redirect status {resp.status_code} without a Location header from {current!r}"
                    )
                if hop >= _MAX_URL_REDIRECTS:
                    raise ValueError(
                        f"too many redirects (>{_MAX_URL_REDIRECTS}) while fetching {url!r}"
                    )
                current = urljoin(current, location)  # resolves relative Locations too
                continue
            resp.raise_for_status()
            body = _read_capped(resp)
        finally:
            resp.close()  # never leak the socket, on success or error
        # Charset: trust Content-Type charset= when explicitly present; when
        # omitted, requests defaults text/* to ISO-8859-1, so prefer
        # apparent_encoding sniffed from the (already capped) body.
        resp._content = body  # the stream was consumed by _read_capped
        content_type = resp.headers.get("Content-Type") or ""
        has_explicit_charset = "charset=" in content_type.lower()
        apparent = getattr(resp, "apparent_encoding", None)
        encoding = (
            resp.encoding
            if (has_explicit_charset and resp.encoding)
            else (apparent or resp.encoding or "utf-8")
        )
        html = body.decode(encoding, errors="replace")
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return ExtractionResult(text=text, mime_type="text/html", source_uri=url)
    raise ValueError(f"too many redirects (>{_MAX_URL_REDIRECTS}) while fetching {url!r}")


def _extract_pdf_pypdf(path_or_bytes: str | bytes) -> ExtractionResult:
    """Fallback PDF text extraction with pypdf (no ML dependencies)."""
    from pypdf import PdfReader

    if isinstance(path_or_bytes, str):
        reader = PdfReader(path_or_bytes)
        source_uri = path_or_bytes
    else:
        reader = PdfReader(io.BytesIO(path_or_bytes))
        source_uri = None
    text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return ExtractionResult(text=text, mime_type="application/pdf", source_uri=source_uri)


def extract_pdf(path_or_bytes: str | bytes) -> ExtractionResult:
    """Extract text from a PDF file path or raw bytes.

    Tries Docling first (structure-aware parsing exported as Markdown); on
    ImportError (docling not installed) or ANY conversion failure it falls
    back to the plain pypdf extractor (_extract_pdf_pypdf). The docling import
    is lazy, so importing this module never pays for the heavy torch chain.
    """
    source_uri = path_or_bytes if isinstance(path_or_bytes, str) else None
    try:
        from docling.datamodel.base_models import DocumentStream
        from docling.document_converter import DocumentConverter
    except ImportError:
        return _extract_pdf_pypdf(path_or_bytes)
    try:
        if isinstance(path_or_bytes, str):
            source = path_or_bytes
        else:
            # convert() takes Path/str/DocumentStream — wrap raw bytes.
            source = DocumentStream(name="input.pdf", stream=io.BytesIO(path_or_bytes))
        converter = DocumentConverter()
        markdown = converter.convert(source).document.export_to_markdown()
    except Exception:
        return _extract_pdf_pypdf(path_or_bytes)
    return ExtractionResult(text=markdown, mime_type="application/pdf", source_uri=source_uri)


# Vision-LLM interpretation (extract_image): enabled when VISION_API_URL and
# VISION_MODEL are set; VISION_API_KEY is optional (Bearer auth when present).
# The endpoint must be OpenAI-compatible (POST {VISION_API_URL}/chat/completions).
# Redirects on the vision POST are followed MANUALLY (allow_redirects=False,
# max _MAX_API_REDIRECTS hops, re-gated per hop) — see _extract_image_vision.
_API_REDIRECT_STATUSES = (301, 302, 303, 307, 308)
_MAX_API_REDIRECTS = 3
_VISION_PROMPT = (
    "Describe this image/screenshot in detail. Transcribe ALL visible text "
    "exactly. Then summarize what it shows."
)
# Cap on the base64 image payload sent to the vision endpoint (~4MB); larger
# images are downscaled with PIL (aspect kept) until they fit.
_VISION_MAX_BYTES = 4 * 1024 * 1024
_VISION_TIMEOUT_S = 120


def _encode_image_for_vision(img) -> tuple[bytes, str]:
    """Encode a PIL image for the vision request, downscaling until <= _VISION_MAX_BYTES.

    Returns (data, mime). JPEG sources in RGB stay JPEG (small for photos);
    everything else is re-encoded as PNG (safe for any mode). Downscaling
    halves both dimensions, so the aspect ratio is preserved.
    """
    mime = "image/png"
    save_kwargs: dict = {"format": "PNG"}
    if (img.format or "").upper() == "JPEG" and img.mode == "RGB":
        mime = "image/jpeg"
        save_kwargs = {"format": "JPEG", "quality": 85}
    while True:
        buf = io.BytesIO()
        img.save(buf, **save_kwargs)
        data = buf.getvalue()
        if len(data) <= _VISION_MAX_BYTES or (img.width <= 64 and img.height <= 64):
            return data, mime
        img = img.resize((max(1, img.width // 2), max(1, img.height // 2)))


def _extract_image_vision(path_or_bytes: str | bytes) -> str:
    """Describe an image via an OpenAI-compatible vision endpoint.

    Raises on ANY failure (missing env, network error, bad status, malformed
    response) — the caller (extract_image) falls back to OCR in that case.
    """
    import base64
    import os
    import sys

    import requests
    from PIL import Image
    from urllib.parse import urlsplit

    api_url = os.environ.get("VISION_API_URL")
    model = os.environ.get("VISION_MODEL")
    if not api_url or not model:
        raise RuntimeError(
            _sanitize("VISION_API_URL and VISION_MODEL must both be set for the vision path")
        )

    # Validate the endpoint URL: only http/https schemes are accepted, and a
    # plain-http endpoint that is NOT loopback gets a loud warning (the image
    # payload would cross the network unencrypted) — but it is not blocked,
    # since local/tunnelled setups legitimately use http.
    vparts = urlsplit(api_url)
    if vparts.scheme.lower() not in ("http", "https"):
        raise RuntimeError(
            _sanitize(
                f"VISION_API_URL must use an http(s) scheme, got {vparts.scheme!r}"
            )
        )
    if vparts.scheme.lower() == "http":
        vhost = (vparts.hostname or "").lower()
        non_loopback = True
        if vhost in ("localhost",) or vhost == "::1" or vhost == "[::1]":
            non_loopback = False
        else:
            import ipaddress
            import socket as _socket

            try:
                ip = ipaddress.ip_address(vhost)
                non_loopback = not ip.is_loopback
            except ValueError:
                # hostname: resolve and check every address
                try:
                    infos = _socket.getaddrinfo(
                        vhost, vparts.port or 80, proto=_socket.IPPROTO_TCP
                    )
                    non_loopback = any(
                        not ipaddress.ip_address(sa[0]).is_loopback
                        for _f, _t, _p, _c, sa in infos
                    ) if infos else True
                except _socket.gaierror:
                    non_loopback = True  # unresolvable — treat as remote, warn
        if non_loopback:
            print(
                "WARNING: ==========================================================\n"
                f"WARNING: VISION_API_URL uses plain http for a NON-loopback host "
                f"({vhost!r}) — the image payload is sent UNENCRYPTED over the\n"
                "WARNING: network. Use https (or a loopback/SSH-tunnel endpoint).\n"
                "WARNING: ==========================================================",
                file=sys.stderr,
            )

    if isinstance(path_or_bytes, str):
        img = Image.open(path_or_bytes)
    else:
        img = Image.open(io.BytesIO(path_or_bytes))
    data, mime = _encode_image_for_vision(img)

    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get("VISION_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    payload = {
        "model": model,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": _VISION_PROMPT},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{base64.b64encode(data).decode('ascii')}"},
                    },
                ],
            }
        ],
    }
    # All request/response error text is routed through _sanitize: requests
    # exceptions and HTTPError echo the request URL (which may carry
    # ?api_key=...) and headers (Authorization: Bearer ...), so the raw
    # exception must never propagate or be printed as-is.
    # Phase 5 external-send gate: BEFORE the POST to {VISION_API_URL}/chat/completions.
    _audit_gate_external_send(api_url, purpose="vision", model=model, payload=payload)
    # Redirects are followed MANUALLY (allow_redirects=False, max
    # _MAX_API_REDIRECTS hops): every 3xx Location is resolved against the
    # current URL and re-gated via _regate_redirect_hop BEFORE the re-POST
    # (audit-A finding 2), so a redirect can never bypass the external-send
    # gate. A 3xx without Location or too many hops raises ValueError.
    from urllib.parse import urljoin as _urljoin

    current_url = api_url.rstrip("/") + "/chat/completions"
    current_headers = headers
    hops = 0
    while True:
        try:
            resp = requests.post(
                current_url,
                headers=current_headers,
                json=payload,
                timeout=_VISION_TIMEOUT_S,
                allow_redirects=False,  # manual, re-gated redirect loop
            )
        except Exception as exc:
            raise RuntimeError(
                _sanitize(f"vision endpoint request failed ({type(exc).__name__}: {exc})")
            ) from exc
        if resp.status_code in _API_REDIRECT_STATUSES:
            location = resp.headers.get("Location")
            if not location:
                raise ValueError(
                    _sanitize(f"vision endpoint returned redirect status "
                              f"{resp.status_code} without a Location header "
                              f"(from {_sanitize(current_url)[:200]})")
                )
            if hops >= _MAX_API_REDIRECTS:
                raise ValueError(
                    _sanitize(f"vision endpoint redirected more than "
                              f"{_MAX_API_REDIRECTS} times (last hop from "
                              f"{_sanitize(current_url)[:200]} to {_sanitize(location)[:200]})")
                )
            new_url = _urljoin(current_url, location)  # absolute or relative
            hops += 1
            # Re-gate + re-log EVERY hop before re-POSTing; PermissionError
            # propagates — refused before dialing the new host.
            _regate_redirect_hop(new_url, purpose="vision", model=model, payload=payload)
            if _url_origin(new_url) != _url_origin(current_url):
                current_headers = _strip_auth_headers(current_headers)
            current_url = new_url
            continue
        try:
            resp.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                _sanitize(f"vision endpoint request failed ({type(exc).__name__}: {exc})")
            ) from exc
        break
    try:
        content = resp.json()["choices"][0]["message"]["content"]
    except Exception as exc:
        raise RuntimeError(
            _sanitize(f"malformed vision endpoint response ({type(exc).__name__}: {exc})")
        ) from exc
    if isinstance(content, list):  # some providers return a list of parts
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError(_sanitize("vision endpoint returned empty content"))
    return content


def _extract_image_ocr(path_or_bytes: str | bytes, source_uri: str | None = None) -> ExtractionResult:
    """OCR an image to extract any text it contains (pytesseract fallback path)."""
    import pytesseract
    from PIL import Image

    if isinstance(path_or_bytes, str):
        img = Image.open(path_or_bytes)
        source_uri = path_or_bytes
    else:
        img = Image.open(io.BytesIO(path_or_bytes))
    text = pytesseract.image_to_string(img)
    return ExtractionResult(text=text, mime_type="image/*", source_uri=source_uri)


def extract_image(path_or_bytes: str | bytes, source_uri: str | None = None) -> ExtractionResult:
    """Interpret an image: vision-LLM first, pytesseract OCR as fallback.

    When the VISION_API_URL and VISION_MODEL env vars are set, the image is
    sent to the OpenAI-compatible endpoint POST {VISION_API_URL}/chat/completions
    (VISION_API_KEY optional, VISION_MODEL as model, image as a base64 data URL,
    timeout 120s) and the model's description/transcription is used as the text.
    If the env vars are missing OR the call fails for any reason, the image is
    OCR'd locally with pytesseract instead. mime_type stays "image/*" and the
    source_uri (the file path for path input) is preserved either way.
    """
    import os

    if isinstance(path_or_bytes, str):
        resolved_uri = path_or_bytes
    else:
        resolved_uri = source_uri

    if os.environ.get("VISION_API_URL") and os.environ.get("VISION_MODEL"):
        try:
            text = _extract_image_vision(path_or_bytes)
            return ExtractionResult(text=text, mime_type="image/*", source_uri=resolved_uri)
        except Exception as exc:  # any failure -> OCR fallback
            import sys

            print(f"[extract_image] vision call failed ({type(exc).__name__}: {_sanitize(exc)}); "
                  "falling back to OCR", file=sys.stderr)
    return _extract_image_ocr(path_or_bytes, resolved_uri)


YOUTUBE_MIME_TYPE = "text/x-youtube-transcript"


def _assert_youtube_url(url: str) -> None:
    """Validate a YouTube URL: public http(s) (SSRF guard) AND YouTube-only host.

    Reuses _assert_public_url (scheme + resolved-IP checks) and additionally
    requires the hostname to be youtube.com or youtu.be (or a subdomain of
    either), keeping ingestion scope tight.
    """
    from urllib.parse import urlsplit

    _assert_public_url(url)
    host = (urlsplit(url).hostname or "").lower()
    is_youtube = (
        host == "youtube.com" or host.endswith(".youtube.com")
        or host == "youtu.be" or host.endswith(".youtu.be")
    )
    if not is_youtube:
        raise ValueError(f"refusing non-YouTube URL (host {host!r}); only youtube.com / youtu.be are supported")


def _subtitle_to_text(raw: str) -> str:
    """Convert WebVTT / SRT / json3 subtitle content to plain caption text.

    Drops cue headers, timestamp lines, SRT counters and inline tags, and
    collapses consecutive duplicate lines (auto-captions overlap and repeat).
    """
    import json as _json
    import re

    if raw.lstrip().startswith("{"):
        # json3: {"events": [{"segs": [{"utf8": "..."}]}]}
        try:
            data = _json.loads(raw)
            parts = []
            for event in data.get("events", []):
                for seg in event.get("segs") or []:
                    t = (seg.get("utf8") or "").strip()
                    if t and t != "\n":
                        parts.append(t.replace("\n", " "))
            return "\n".join(parts)
        except (ValueError, AttributeError):
            return ""

    out: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("WEBVTT") or s.startswith(("Kind:", "Language:")):
            continue
        if "-->" in s:  # VTT / SRT cue timestamp line
            continue
        if s.isdigit():  # SRT numeric cue counter
            continue
        s = re.sub(r"<[^>]+>", "", s).strip()  # VTT inline tags <00:00:01.000><c>...
        if s:
            out.append(s)
    deduped: list[str] = []
    for s in out:
        if not deduped or deduped[-1] != s:
            deduped.append(s)
    return "\n".join(deduped)


def _yt_dlp_runner(url: str, workdir: str) -> dict:
    """Real yt-dlp runner: download captions (+ metadata only, no media) into workdir.

    Returns {"subtitle_path": str | None, "info": dict}. yt_dlp is imported
    lazily so the rest of the pipeline never pays for it.
    """
    import glob
    import os

    import yt_dlp

    outtmpl = os.path.join(workdir, "%(id)s.%(ext)s")
    ydl_opts = {
        "skip_download": True,  # metadata + subtitles only, never the media file
        "writesubtitles": True,
        "writeautomaticsub": True,
        # Try English first ('en', then any 'en.*' variant), then any
        # available caption language.
        "subtitleslangs": ["en", "en.*", "all"],
        "subtitlesformat": "vtt/srt/json3/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
    if info is None:
        return {"subtitle_path": None, "info": {}}

    # Pick the best subtitle file written: prefer English, then any vtt/srt/json3.
    candidates = sorted(glob.glob(os.path.join(workdir, "*.vtt"))
                        + glob.glob(os.path.join(workdir, "*.srt"))
                        + glob.glob(os.path.join(workdir, "*.json3")))
    subtitle_path = None
    for cand in candidates:
        base = os.path.basename(cand)
        if ".en" in base:
            subtitle_path = cand
            break
    if subtitle_path is None and candidates:
        subtitle_path = candidates[0]
    return {"subtitle_path": subtitle_path, "info": info}


def extract_youtube(url: str, runner=None) -> ExtractionResult:
    """Extract a YouTube video's caption transcript + metadata.

    runner: injectable for tests — callable(url, workdir) ->
        {"subtitle_path": str | None, "info": dict}. Defaults to the real
        yt-dlp call (_yt_dlp_runner). Caption languages are tried in the
        order 'en', 'en.*', then any available.

    The text ALWAYS includes the video title and description (even when
    captions exist), plus the caption transcript when available — a
    transcript is NEVER fabricated.
    """
    import os
    import tempfile

    # Phase 5 audit trail: inbound third-party fetch (no gate) before fetching.
    _audit_log_external_fetch(url)

    _assert_youtube_url(url)
    if runner is None:
        runner = _yt_dlp_runner

    with tempfile.TemporaryDirectory(prefix="sb-yt-") as workdir:
        outcome = runner(url, workdir) or {}
        subtitle_path = outcome.get("subtitle_path")
        info = outcome.get("info") or {}
        captions = ""
        if subtitle_path:
            try:
                with open(subtitle_path, encoding="utf-8", errors="replace") as fh:
                    captions = _subtitle_to_text(fh.read())
            except OSError:
                captions = ""

    # Always include the metadata (title + description + URL); append the
    # caption transcript when one was found. Never fabricate a transcript.
    title = str(info.get("title") or "").strip()
    description = str(info.get("description") or "").strip()
    parts = [p for p in (title, captions.strip(), description, url) if p]
    return ExtractionResult(text="\n\n".join(parts), mime_type=YOUTUBE_MIME_TYPE, source_uri=url)


# Audio file extensions the transcriber can decode (via ffmpeg). Anything else
# is rejected with a clean ValueError instead of being fed to the decoder.
_AUDIO_EXTS = {
    "wav", "mp3", "m4a", "mp4", "ogg", "oga", "opus", "flac", "wma", "aac", "webm",
}

# Whisper model size for local transcription ("small" or "base" per Phase 2 spec).
_WHISPER_MODEL_SIZE = "base"

# Loaded once per process and reused (bridge serve mode ingests many files).
_whisper_model_cache = None


def _get_whisper_model():
    """Load and cache the Whisper model, preferring CUDA only when it works.

    Device detection alone is not enough: a machine can have an NVIDIA driver
    (so ctranslate2 reports CUDA devices) but lack the CUDA runtime libraries,
    which only surfaces on the first forward pass. A one-second probe on
    silence verifies the device; on failure we fall back to CPU + int8 once.
    """
    global _whisper_model_cache
    if _whisper_model_cache is not None:
        return _whisper_model_cache

    import numpy as np
    from faster_whisper import WhisperModel

    try:
        import ctranslate2

        device = "cuda" if ctranslate2.get_cuda_device_count() > 0 else "cpu"
    except Exception:
        device = "cpu"
    while True:
        compute_type = "int8" if device == "cpu" else "int8_float16"
        try:
            model = WhisperModel(_WHISPER_MODEL_SIZE, device=device, compute_type=compute_type)
            segments, _info = model.transcribe(np.zeros(16000, dtype=np.float32), beam_size=1)
            list(segments)  # force the forward pass — this is what fails on broken CUDA
        except Exception:
            if device == "cuda":
                device = "cpu"  # CUDA detected but unusable — fall back once
                continue
            raise
        break
    _whisper_model_cache = model
    return model


def extract_audio(path_or_bytes: str | bytes, source_uri: str | None = None) -> ExtractionResult:
    """Transcribe an audio file to text with faster-whisper (fully local).

    Accepts a filesystem path or raw bytes (bytes use source_uri for the
    extension and the reported source_uri). faster_whisper is imported lazily
    inside this function so importing pipeline.extractors stays cheap when the
    dependency (or the model weights) is not installed.
    """
    import os

    if isinstance(path_or_bytes, str):
        path = path_or_bytes
        uri = path
        ext = os.path.splitext(path)[1].lstrip(".").lower()
    else:
        path = None
        uri = source_uri
        ext = os.path.splitext(source_uri or "")[1].lstrip(".").lower()
    if ext not in _AUDIO_EXTS:
        raise ValueError(
            f"unsupported audio extension {ext!r} (expected one of {sorted(_AUDIO_EXTS)})"
        )
    if path is not None and not os.path.isfile(path):
        raise FileNotFoundError(f"audio file not found: {path!r}")

    model = _get_whisper_model()
    if path is not None:
        segments, _info = model.transcribe(path, beam_size=5)
    else:
        segments, _info = model.transcribe(io.BytesIO(path_or_bytes), beam_size=5)
    text = " ".join(seg.text.strip() for seg in segments if seg.text.strip())
    return ExtractionResult(text=text, mime_type=f"audio/{ext}", source_uri=uri)


def extract(source_type: str, payload: str | bytes, source_uri: str | None = None) -> ExtractionResult:
    """Dispatch to the right extractor based on source_type.

    source_type: 'text' | 'url' | 'pdf' | 'image' | 'youtube' | 'audio'
    Unknown types: caller should provide a detailed description
    as plain text with source_type='text' and the original location in source_uri.
    """
    dispatch = {
        "text": lambda: extract_text(payload.decode() if isinstance(payload, bytes) else payload, source_uri),
        "url": lambda: extract_url(payload.decode() if isinstance(payload, bytes) else payload),
        "pdf": lambda: extract_pdf(payload),
        "image": lambda: extract_image(payload, source_uri),
        "youtube": lambda: extract_youtube(payload.decode() if isinstance(payload, bytes) else payload),
        "audio": lambda: extract_audio(payload, source_uri),
    }
    if source_type not in dispatch:
        raise ValueError(f"Unsupported source_type: {source_type!r}. Use 'text' with a description for unsupported formats.")
    return dispatch[source_type]()
