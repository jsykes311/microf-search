"""Tests for guarded Nominatim handling. Every upstream is mocked; nothing reaches the network or starts the app."""
import ast
import asyncio
import json
import math
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import address_geocode as ag

MAIN = Path(__file__).resolve().parents[1] / "main.py"
ADDRESS = "12 Fakeville Lane, Springfield"


def run(coro):
    return asyncio.run(coro)


def client_for(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def geocode(handler, address=ADDRESS):
    async with client_for(handler) as c:
        return await ag.geocode_address(c, address)


def reply(status=200, body="", ctype="application/json; charset=utf-8", headers=None):
    h = {"content-type": ctype, **(headers or {})}
    return lambda request: httpx.Response(status, content=body if isinstance(body, bytes) else body.encode(), headers=h)


GOOD = json.dumps([{"lat": "39.78", "lon": "-89.65", "display_name": "Springfield, Sangamon County, Illinois, USA"}])


class ValidResults(unittest.TestCase):
    def test_valid_result(self):
        hit = run(geocode(reply(body=GOOD)))
        self.assertEqual(hit, {"lat": 39.78, "lon": -89.65, "label": "Springfield"})

    def test_request_identifies_the_app_and_limits_the_query(self):
        seen = {}

        def handler(request):
            seen["ua"] = request.headers["user-agent"]
            seen["q"] = request.url.params["q"]
            return httpx.Response(200, content=GOOD.encode(), headers={"content-type": "application/json"})
        run(geocode(handler, "x" * 500))
        self.assertIn("microf-search", seen["ua"])
        self.assertEqual(len(seen["q"]), ag.MAX_QUERY_CHARS)

    def test_user_agent_can_be_set_from_the_environment(self):
        seen = {}

        def handler(request):
            seen["ua"] = request.headers["user-agent"]
            return httpx.Response(200, content=GOOD.encode())
        with patch.dict("os.environ", {"NOMINATIM_USER_AGENT": "custom-agent/9"}):
            run(geocode(handler))
        self.assertEqual(seen["ua"], "custom-agent/9")

    def test_missing_display_name_falls_back_to_the_first_part_of_the_query(self):
        body = json.dumps([{"lat": 1, "lon": 2}])      # same as before: the label is cut at the first comma
        self.assertEqual(run(geocode(reply(body=body))), {"lat": 1.0, "lon": 2.0, "label": "12 Fakeville Lane"})


class EmptyResults(unittest.TestCase):
    def test_empty_list_is_not_an_error(self):
        self.assertIsNone(run(geocode(reply(body="[]"))))


class InvalidBodies(unittest.TestCase):
    def kind(self, handler):
        with self.assertRaises(ag.GeocodeError) as cm:
            run(geocode(handler))
        return cm.exception

    def test_html_with_200(self):
        e = self.kind(reply(body="<html><body>Access blocked for 12 Fakeville Lane</body></html>", ctype="text/html"))
        self.assertEqual((e.kind, e.http_status), ("invalid_json", 502))

    def test_empty_body(self):
        self.assertEqual(self.kind(reply(body="")).kind, "invalid_json")

    def test_truncated_json(self):
        self.assertEqual(self.kind(reply(body='[{"lat": "39.7')).kind, "invalid_json")

    def test_non_utf8_bytes(self):
        self.assertEqual(self.kind(reply(body=b"\xff\xfe\x00bad", ctype="application/octet-stream")).kind, "invalid_json")

    def test_json_object_instead_of_list(self):
        self.assertEqual(self.kind(reply(body='{"error": "rate limit"}')).kind, "bad_shape")

    def test_item_not_an_object(self):
        self.assertEqual(self.kind(reply(body='["x"]')).kind, "bad_shape")

    def test_missing_or_nonnumeric_or_out_of_range_coordinates(self):
        for item in ({"display_name": "A"}, {"lat": "abc", "lon": "1"}, {"lat": "91", "lon": "0"},
                     {"lat": "0", "lon": "-181"}, {"lat": "NaN", "lon": "1"}, {"lat": None, "lon": None}):
            with self.subTest(item=item):
                self.assertEqual(self.kind(reply(body=json.dumps([item]))).kind, "bad_shape")


class UpstreamFailures(unittest.TestCase):
    def kind(self, handler):
        with self.assertRaises(ag.GeocodeError) as cm:
            run(geocode(handler))
        return cm.exception

    def test_error_statuses(self):
        for status in (400, 403, 404, 500, 502, 503, 504):
            with self.subTest(status=status):
                e = self.kind(reply(status, "<html>nope</html>", "text/html"))
                self.assertEqual((e.kind, e.http_status, e.diagnostics["http"]), ("upstream_status", 502, status))

    def test_valid_looking_json_with_an_error_status_is_still_an_error(self):
        self.assertEqual(self.kind(reply(500, GOOD)).kind, "upstream_status")

    def test_rate_limited(self):
        e = self.kind(reply(429, "slow down", "text/plain", {"retry-after": "30"}))
        self.assertEqual((e.kind, e.http_status, e.diagnostics["retry_after"]), ("rate_limited", 503, "30"))

    def test_timeout(self):
        def handler(request):
            raise httpx.ReadTimeout("slow", request=request)
        e = self.kind(handler)
        self.assertEqual((e.kind, e.http_status), ("timeout", 504))

    def test_connection_failure(self):
        def handler(request):
            raise httpx.ConnectError("refused", request=request)
        e = self.kind(handler)
        self.assertEqual((e.kind, e.http_status), ("network", 502))


class Diagnostics(unittest.TestCase):
    def error(self, body, address=ADDRESS, ctype="text/html", status=200):
        async def go():
            async with client_for(reply(status, body, ctype)) as c:
                await ag.geocode_address(c, address)
        with self.assertRaises(ag.GeocodeError) as cm:
            run(go())
        return cm.exception

    def test_log_line_never_contains_the_searched_address(self):
        e = self.error("<html><h1>Blocked</h1><p>Request for 12 Fakeville Lane, Springfield denied (Fakeville)</p></html>")
        line = e.log_line()
        for secret in ("Fakeville", "12 Fakeville", "Springfield"):
            self.assertNotIn(secret, line)
        self.assertIn("kind=invalid_json", line)
        self.assertIn("http=200", line)
        self.assertIn("content_type='text/html'", line)
        self.assertIn(f"query_chars={len(ADDRESS)}", line)
        self.assertIn("[query]", line)
        self.assertNotIn("<h1>", line)

    def test_snippet_is_short_and_single_line(self):
        e = self.error("line one\n\n   line two\t" + "z" * 5000)
        self.assertLessEqual(len(e.diagnostics["snippet"]), ag.SNIPPET_CHARS)
        self.assertNotIn("\n", e.diagnostics["snippet"])

    def test_log_carries_no_url_or_headers(self):
        e = self.error("x", status=403)
        line = e.log_line()
        for leaked in ("nominatim.openstreetmap.org", "q=", "user-agent", "set-cookie", "authorization"):
            self.assertNotIn(leaked, line.lower())

    def test_user_message_does_not_expose_upstream_details(self):
        e = self.error("<html>secret internal page</html>")
        self.assertNotIn("secret", e.user_message)
        self.assertNotIn("html", e.user_message.lower())


class FakeGeo:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def query_postal_code(self, z):
        self.calls.append(z)
        return self.rows.get(z, {"latitude": float("nan"), "longitude": float("nan")})


class ZipRegression(unittest.TestCase):
    ROWS = {"62701": {"latitude": 39.8, "longitude": -89.64, "place_name": "Springfield", "state_code": "IL"},
            "30301": {"latitude": 33.75, "longitude": -84.39}}

    def test_zip_resolves_offline_with_the_same_label(self):
        hit = ag.lookup_zip(FakeGeo(self.ROWS), "62701")
        self.assertEqual(hit, {"lat": 39.8, "lon": -89.64, "label": "Springfield, IL"})

    def test_zip_plus_four_and_padding(self):
        self.assertEqual(ag.lookup_zip(FakeGeo(self.ROWS), "  62701-1234 ")["label"], "Springfield, IL")

    def test_missing_place_columns_keep_the_old_label(self):
        self.assertEqual(ag.lookup_zip(FakeGeo(self.ROWS), "30301")["label"], "30301, ")

    def test_unknown_zip_is_not_treated_as_a_location(self):
        self.assertIsNone(ag.lookup_zip(FakeGeo(self.ROWS), "00000"))

    def test_text_that_is_not_a_zip_skips_the_lookup(self):
        geo = FakeGeo(self.ROWS)
        self.assertIsNone(ag.lookup_zip(geo, "Springfield IL"))
        self.assertEqual(geo.calls, [])

    def test_lookup_errors_are_swallowed(self):
        class Boom:
            def query_postal_code(self, z):
                raise RuntimeError("data missing")
        self.assertIsNone(ag.lookup_zip(Boom(), "62701"))


def load_endpoint():
    """accounts_nearest from main.py, compiled in isolation (importing main would start the app's background work)."""
    tree = ast.parse(MAIN.read_text())
    fn = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "accounts_nearest")
    fn.decorator_list = []
    ns = {"asyncio": asyncio, "_address_geocode": ag, "JSONResponse": __import__("fastapi.responses", fromlist=["x"]).JSONResponse,
          "_location_index_ts": 0, "_location_index_building": True, "_location_index": {}}
    exec(compile(ast.Module([fn], []), str(MAIN), "exec"), ns)
    return ns


class EndpointWiring(unittest.TestCase):
    """The real accounts_nearest with pgeocode faked and the Nominatim transport mocked."""

    def call(self, address, handler, rows=None):
        ns = load_endpoint()
        fake = types.ModuleType("pgeocode")
        fake.Nominatim = lambda country: FakeGeo(rows or {})
        real = httpx.AsyncClient
        hits = []

        def counting(request):
            hits.append(request)
            return handler(request)
        with patch.dict(sys.modules, {"pgeocode": fake}), \
                patch("httpx.AsyncClient", lambda **kw: real(transport=httpx.MockTransport(counting))):
            out = run(ns["accounts_nearest"](address=address, limit=10))
        return out, hits

    def test_zip_never_calls_nominatim(self):
        out, hits = self.call("62701", reply(500, "boom"), TestZip.ROWS)
        self.assertEqual(hits, [])
        self.assertEqual((out["lat"], out["label"], out["warming_up"]), (39.8, "Springfield, IL", True))

    def test_address_fallback_with_a_good_answer(self):
        out, hits = self.call(ADDRESS, reply(body=GOOD))
        self.assertEqual(len(hits), 1)
        self.assertEqual((out["lat"], out["lon"], out["label"]), (39.78, -89.65, "Springfield"))

    def test_nothing_found_keeps_the_old_message(self):
        out, _ = self.call("Nowhereville", reply(body="[]"))
        self.assertEqual(out, {"error": "Could not geocode: Nowhereville"})

    def test_html_instead_of_json_is_a_controlled_error(self):
        out, _ = self.call(ADDRESS, reply(body="<html>blocked</html>", ctype="text/html"))
        self.assertEqual(out.status_code, 502)
        body = json.loads(out.body)
        self.assertEqual(body["code"], "invalid_json")
        self.assertIn("error", body)          # the pages read data.error
        self.assertNotIn("blocked", body["error"])

    def test_upstream_error_and_timeout_are_controlled_errors(self):
        out, _ = self.call(ADDRESS, reply(503, "down", "text/plain"))
        self.assertEqual((out.status_code, json.loads(out.body)["code"]), (502, "upstream_status"))

        def timeout(request):
            raise httpx.ConnectTimeout("t", request=request)
        out, _ = self.call(ADDRESS, timeout)
        self.assertEqual((out.status_code, json.loads(out.body)["code"]), (504, "timeout"))

    def test_blank_address_returns_before_any_lookup(self):
        out, hits = self.call("", reply(500, "boom"))
        self.assertEqual((out, hits), ({"accounts": [], "lat": None, "lon": None}, []))


TestZip = ZipRegression

if __name__ == "__main__":
    unittest.main()
