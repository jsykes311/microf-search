"""Address geocoding for /api/accounts/nearest with guarded handling of the Nominatim response.

The endpoint used to call `nr.json()` on whatever Nominatim returned. When the upstream answered with something that
was not JSON (an HTML error or block page, an empty body, a proxy page), that raised JSONDecodeError and the request
failed with an unhandled 500. `geocode_address` checks the response before using it and raises `GeocodeError` for every
upstream failure, so the endpoint can answer with a controlled error instead.

Nothing here starts the app, talks to ActiveCampaign, or logs the address a person searched for: diagnostics carry the
HTTP status, content type, sizes and a short snippet of the upstream body with the query and any markup removed.
"""
from __future__ import annotations

import math
import os
import re
from typing import Optional

import httpx

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Nominatim's usage policy asks for a User-Agent that identifies the application; the default python-httpx one is
# commonly rejected. Set NOMINATIM_USER_AGENT to add a contact address.
DEFAULT_USER_AGENT = "microf-search-dealer-locator/1.0 (+https://github.com/jsykes311/microf-search)"
MAX_QUERY_CHARS = 200
SNIPPET_CHARS = 120
USER_MESSAGE = "Address lookup is temporarily unavailable. Please try again in a minute, or search by ZIP code."

_TAGS = re.compile(r"<[^>]*>")
_SPACE = re.compile(r"\s+")


class GeocodeError(Exception):
    """The address geocoder could not give a usable answer. `kind` says why; `diagnostics` is safe to log."""

    def __init__(self, kind: str, http_status: int = 502, diagnostics: Optional[dict] = None):
        super().__init__(kind)
        self.kind = kind
        self.http_status = http_status
        self.diagnostics = diagnostics or {}
        self.user_message = USER_MESSAGE

    def log_line(self) -> str:
        parts = " ".join(f"{k}={v!r}" if isinstance(v, str) else f"{k}={v}" for k, v in self.diagnostics.items())
        return f"[geocode] nominatim failed kind={self.kind} {parts}".rstrip()


def _valid_coord(value, low: float, high: float) -> Optional[float]:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) and low <= x <= high else None


def redact(text: str, query: str) -> str:
    """A short, single-line, markup-free piece of an upstream body with the user's query removed."""
    text = _TAGS.sub(" ", text or "")
    for piece in {query.strip(), *[p for p in re.split(r"[\s,]+", query) if len(p) >= 4]}:
        if piece:
            text = re.sub(re.escape(piece), "[query]", text, flags=re.I)
    return _SPACE.sub(" ", text).strip()[:SNIPPET_CHARS]


def describe_response(resp: httpx.Response, query: str) -> dict:
    """What is safe to log about an upstream response: no URL, no query, no cookies or other headers."""
    try:
        body = resp.text
    except Exception:
        body = ""
    info = {"http": resp.status_code,
            "content_type": (resp.headers.get("content-type") or "")[:60],
            "bytes": len(resp.content or b""),
            "snippet": redact(body, query)}
    if resp.headers.get("retry-after"):
        info["retry_after"] = resp.headers["retry-after"][:20]
    return info


async def geocode_address(client: httpx.AsyncClient, address: str, url: str = NOMINATIM_URL,
                          user_agent: Optional[str] = None) -> Optional[dict]:
    """{"lat", "lon", "label"} for the best match, None when Nominatim finds nothing, GeocodeError when it fails."""
    query = (address or "").strip()[:MAX_QUERY_CHARS]
    headers = {"Accept-Language": "en", "User-Agent": user_agent or os.environ.get("NOMINATIM_USER_AGENT") or DEFAULT_USER_AGENT}
    base = {"query_chars": len(query)}
    try:
        resp = await client.get(url, params={"q": query, "countrycodes": "us", "limit": 1, "format": "json"}, headers=headers)
    except httpx.TimeoutException as exc:
        raise GeocodeError("timeout", 504, {**base, "error": type(exc).__name__}) from None
    except httpx.HTTPError as exc:
        raise GeocodeError("network", 502, {**base, "error": type(exc).__name__}) from None

    diag = {**base, **describe_response(resp, query)}
    if resp.status_code == 429:
        raise GeocodeError("rate_limited", 503, diag)
    if resp.status_code != 200:
        raise GeocodeError("upstream_status", 502, diag)
    try:
        results = resp.json()
    except ValueError:                      # JSONDecodeError and UnicodeDecodeError both subclass ValueError
        raise GeocodeError("invalid_json", 502, diag) from None
    if not isinstance(results, list):
        raise GeocodeError("bad_shape", 502, diag)
    if not results:
        return None
    first = results[0]
    lat = _valid_coord(first.get("lat"), -90, 90) if isinstance(first, dict) else None
    lon = _valid_coord(first.get("lon"), -180, 180) if isinstance(first, dict) else None
    if lat is None or lon is None:
        raise GeocodeError("bad_shape", 502, diag)
    label = str(first.get("display_name") or query).split(",")[0].strip() or query
    return {"lat": lat, "lon": lon, "label": label}


def lookup_zip(geo, address: str) -> Optional[dict]:
    """Offline ZIP lookup through pgeocode. None unless the input starts with 5 digits that pgeocode knows.
    (pgeocode answers an unknown ZIP with NaN, which used to be accepted as a location.)"""
    zip_clean = (address or "").strip()[:5]
    if not zip_clean.isdigit():
        return None
    try:
        r = geo.query_postal_code(zip_clean)
        lat, lon = _valid_coord(r["latitude"], -90, 90), _valid_coord(r["longitude"], -180, 180)
        if lat is None or lon is None:
            return None
        return {"lat": lat, "lon": lon, "label": f"{r.get('place_name', zip_clean)}, {r.get('state_code', '')}"}
    except Exception:
        return None
