"""
Reusable async ActiveCampaign API client with concurrent pagination.
Usage:
    from ac_client import fetch_all_pages, ac_get, ac_post, ac_delete

    # Fetch all accountCustomFieldData records concurrently
    records = await fetch_all_pages("accountCustomFieldData", key="accountCustomFieldData")

    # Simple GET
    data = await ac_get("accounts/123")

    # POST / DELETE
    data = await ac_post("customObjects/records/<schema>", body={...})
    status = await ac_delete("customObjects/records/<schema>/<id>")
"""
import asyncio
import httpx
import os
import random
import time
from dotenv import load_dotenv

load_dotenv()

BASE_URL = (os.getenv("AC_BASE_URL") or os.getenv("PROD_URL", "https://microf.api-us1.com")).rstrip("/")
API_KEY  = os.getenv("AC_API_KEY") or os.getenv("PROD_KEY", "")

_HEADERS = {"Api-Token": API_KEY}
_CONCURRENCY = 4    # shared AC ceiling; background jobs use this same gate
_PAGE_SIZE   = 100
_MIN_REQUEST_INTERVAL = float(os.getenv("AC_MIN_REQUEST_INTERVAL", "0.20"))
_MAX_ATTEMPTS = 5
_request_gate = asyncio.Semaphore(_CONCURRENCY)
_pace_lock = asyncio.Lock()
_next_request_at = 0.0


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=BASE_URL,
        headers=_HEADERS,
        timeout=60,
        limits=httpx.Limits(max_connections=_CONCURRENCY + 5),
    )


async def request_json(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    body: dict | None = None,
    retry_writes: bool = False,
) -> tuple[int, dict]:
    """Make one rate-limited AC request with bounded, Retry-After-aware retries.

    GET and DELETE are safe to retry. A caller must explicitly opt a write into
    retries after establishing that its payload is idempotent.
    """
    global _next_request_at
    method = method.upper()
    attempts = _MAX_ATTEMPTS if method in {"GET", "DELETE"} or retry_writes else 1

    for attempt in range(attempts):
        async with _request_gate:
            async with _pace_lock:
                now = time.monotonic()
                if now < _next_request_at:
                    await asyncio.sleep(_next_request_at - now)
                _next_request_at = time.monotonic() + _MIN_REQUEST_INTERVAL

            async with _client() as c:
                response = await c.request(
                    method,
                    f"/api/3/{path.lstrip('/')}",
                    params=params or {},
                    json=body,
                )

        retryable = response.status_code == 429 or response.status_code in {502, 503, 504, 520}
        if not retryable or attempt == attempts - 1:
            response.raise_for_status()
            try:
                data = response.json()
            except Exception:
                data = {}
            return response.status_code, data

        retry_after = response.headers.get("Retry-After", "").strip()
        try:
            delay = min(30.0, max(0.0, float(retry_after)))
        except ValueError:
            delay = 0.0
        if delay <= 0:
            delay = min(30.0, (2 ** attempt) + random.uniform(0, 1.0))
        await asyncio.sleep(delay)

    raise RuntimeError("ActiveCampaign request retry loop exited unexpectedly")


async def ac_get(path: str, params: dict = None) -> dict:
    _, data = await request_json("GET", path, params=params)
    return data


async def ac_post(path: str, body: dict) -> tuple[int, dict]:
    async with _client() as c:
        r = await c.post(f"/api/3/{path}", json=body)
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data


async def ac_put(path: str, body: dict) -> tuple[int, dict]:
    async with _client() as c:
        r = await c.put(f"/api/3/{path}", json=body)
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data


async def ac_patch(path: str, body: dict) -> tuple[int, dict]:
    async with _client() as c:
        r = await c.patch(f"/api/3/{path}", json=body)
        try:
            data = r.json()
        except Exception:
            data = {}
        return r.status_code, data


async def ac_delete(path: str) -> int:
    status, _ = await request_json("DELETE", path)
    return status


async def fetch_all_pages(
    endpoint: str,
    key: str,
    params: dict = None,
    workers: int = _CONCURRENCY,
    page_size: int = _PAGE_SIZE,
    sequential: bool = False,
) -> list:
    """
    Fetch all pages of an endpoint.

    sequential=False (default): concurrent — fast but can produce duplicates/gaps
      on large or frequently-updated endpoints due to AC pagination shifting.
    sequential=True: pages one at a time, stops when a page yields no new IDs.
      Safe against stale meta.total; deduplicates by 'id' field automatically.
      Use for SLPs, accounts, contacts — smaller sets where correctness > speed.
    """
    base_params = dict(params or {})
    base_params["limit"] = page_size

    if sequential:
        import math
        seen: set = set()
        records: list = []

        # First, get the total count so we know how many pages to fetch
        for attempt in range(3):
            try:
                _, first = await request_json(
                    "GET", endpoint, params={**base_params, "offset": 0}
                )
                total = int(first.get("meta", {}).get("total", 0))
                for rec in first.get(key, []):
                    if rec.get("id") not in seen:
                        seen.add(rec.get("id"))
                        records.append(rec)
                break
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(1)

        if not total:
            return records

        # Fetch remaining pages — iterate all offsets up to total
        # AC's pagination is non-deterministic, so we must visit every offset
        # rather than stopping on partial pages.
        num_pages = math.ceil(total / page_size)
        for page_num in range(1, num_pages + 1):
            offset = page_num * page_size
            for attempt in range(3):
                try:
                    _, page_data = await request_json(
                        "GET", endpoint, params={**base_params, "offset": offset}
                    )
                    page = page_data.get(key, [])
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)
            for rec in page:
                if rec.get("id") not in seen:
                    seen.add(rec.get("id"))
                    records.append(rec)
            if not page:
                break  # truly empty — done
        return records

    # ── Concurrent path ────────────────────────────────────────────────────────
    # Fetch page 0 to get total
    _, first = await request_json("GET", endpoint, params={**base_params, "offset": 0})

    total = int(first.get("meta", {}).get("total", 0))
    records = list(first.get(key, []))

    if total <= page_size:
        return records

    offsets = range(page_size, total, page_size)
    sem = asyncio.Semaphore(workers)
    pages: dict[int, list] = {}

    async def fetch_page(offset: int):
        async with sem:
            for attempt in range(3):
                try:
                    _, page_data = await request_json(
                        "GET", endpoint, params={**base_params, "offset": offset}
                    )
                    pages[offset] = page_data.get(key, [])
                    return
                except Exception:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(1)

    await asyncio.gather(*[fetch_page(o) for o in offsets])

    for o in sorted(pages):
        records.extend(pages[o])

    return records


async def fetch_all_slps(schema_id: str, max_passes: int = 8) -> list:
    """Fetch all SLP custom object records, deduped by ID.

    AC's offset-based pagination for custom objects is non-deterministic —
    different pages appear at different offsets on each request, so a single
    pass misses ~28% of records, 3 passes misses ~1%, and 5-6 passes
    converges to 100%.  We keep running passes until one finds nothing new
    (convergence), capped at max_passes for safety.
    """
    seen: set = set()
    deduped: list = []

    async def one_pass() -> int:
        raw = await fetch_all_pages(
            f"customObjects/records/{schema_id}",
            key="records",
            sequential=False,
        )
        new_count = 0
        for rec in raw:
            rid = rec.get("id")
            if rid and rid not in seen:
                seen.add(rid)
                deduped.append(rec)
                new_count += 1
        return new_count

    for i in range(max_passes):
        new = await one_pass()
        if new == 0:   # converged — every record already seen
            break

    return deduped
