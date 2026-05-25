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
from dotenv import load_dotenv

load_dotenv()

BASE_URL = (os.getenv("AC_BASE_URL") or os.getenv("PROD_URL", "https://microf.api-us1.com")).rstrip("/")
API_KEY  = os.getenv("AC_API_KEY") or os.getenv("PROD_KEY", "")

_HEADERS = {"Api-Token": API_KEY}
_CONCURRENCY = 15   # parallel page fetches
_PAGE_SIZE   = 100


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=BASE_URL,
        headers=_HEADERS,
        timeout=60,
        limits=httpx.Limits(max_connections=_CONCURRENCY + 5),
    )


async def ac_get(path: str, params: dict = None) -> dict:
    async with _client() as c:
        r = await c.get(f"/api/3/{path}", params=params or {})
        r.raise_for_status()
        return r.json()


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
    async with _client() as c:
        r = await c.delete(f"/api/3/{path}")
        return r.status_code


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
        async with _client() as c:
            for attempt in range(3):
                try:
                    r = await c.get(f"/api/3/{endpoint}", params={**base_params, "offset": 0})
                    r.raise_for_status()
                    first = r.json()
                    total = int(first.get("meta", {}).get("total", 0))
                    for rec in first.get(key, []):
                        if rec.get("id") not in seen:
                            seen.add(rec.get("id"))
                            records.append(rec)
                    break
                except Exception:
                    if attempt == 2:
                        total = 0
                    await asyncio.sleep(1)

        if not total:
            return records

        # Fetch remaining pages — iterate all offsets up to total
        # AC's pagination is non-deterministic, so we must visit every offset
        # rather than stopping on partial pages.
        num_pages = math.ceil(total / page_size)
        for page_num in range(1, num_pages + 1):
            offset = page_num * page_size
            async with _client() as c:
                for attempt in range(3):
                    try:
                        r = await c.get(
                            f"/api/3/{endpoint}",
                            params={**base_params, "offset": offset},
                        )
                        r.raise_for_status()
                        page = r.json().get(key, [])
                        break
                    except Exception:
                        if attempt == 2:
                            page = []
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
    async with _client() as client:
        r = await client.get(f"/api/3/{endpoint}", params={**base_params, "offset": 0})
        r.raise_for_status()
        first = r.json()

    total = int(first.get("meta", {}).get("total", 0))
    records = list(first.get(key, []))

    if total <= page_size:
        return records

    offsets = range(page_size, total, page_size)
    sem = asyncio.Semaphore(workers)
    pages: dict[int, list] = {}

    async def fetch_page(offset: int):
        async with sem:
            async with _client() as c:
                for attempt in range(3):
                    try:
                        r = await c.get(
                            f"/api/3/{endpoint}",
                            params={**base_params, "offset": offset},
                        )
                        r.raise_for_status()
                        pages[offset] = r.json().get(key, [])
                        return
                    except Exception:
                        if attempt == 2:
                            pages[offset] = []
                        await asyncio.sleep(1)

    await asyncio.gather(*[fetch_page(o) for o in offsets])

    for o in sorted(pages):
        records.extend(pages[o])

    return records


async def fetch_all_slps(schema_id: str) -> list:
    """Fetch all SLP custom object records, deduped by ID.
    Uses concurrent pagination (fast) + dedup — AC's offset pagination returns
    duplicates at different offsets, so sequential dedup misses some records
    while concurrent fetch captures them all before deduplication."""
    raw = await fetch_all_pages(
        f"customObjects/records/{schema_id}",
        key="records",
        sequential=False,
    )
    seen: set = set()
    deduped: list = []
    for rec in raw:
        rid = rec.get("id")
        if rid and rid not in seen:
            seen.add(rid)
            deduped.append(rec)
    return deduped
