"""Safety tests for PandaDoc sync scoping and last-contacted snapshots."""
import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main
import ac_client


def slp(record_id, account_id, status):
    return {
        "id": record_id,
        "fields": [
            {"id": "dealer-id", "value": "redacted"},
            {"id": "slp-status-detail", "value": status},
        ],
        "relationships": {"account": [account_id]},
    }


class PandaDocSyncSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.originals = {
            name: getattr(main, name)
            for name in ("ac_get", "ac_get_all", "ac_post", "ac_delete")
        }
        main._pandadoc_signed_tag_id = "4010"
        main.PANDADOC_SIGNED_SYNC_DRY_RUN = False
        main._slp_cache_records = [
            slp("slp-a", "acct-a", "Awaiting Dealer Agreement Signature"),
            slp("slp-b", "acct-b", "Activated"),
        ]

    async def asyncTearDown(self):
        for name, value in self.originals.items():
            setattr(main, name, value)
        main._pandadoc_signed_tag_id = None
        main._slp_cache_records = []

    async def test_lookup_and_tag_removal_are_strictly_scoped(self):
        writes = []
        deletes = []
        lookup_params = []

        async def fake_get_all(path, key, params=None):
            if path == "contacts":
                return [{"id": "contact-a"}, {"id": "contact-b"}]
            self.assertEqual(path, "accountContacts")
            lookup_params.append(dict(params or {}))
            cid = params["filters[contact]"]
            aid = "acct-a" if cid == "contact-a" else "acct-b"
            return [{"id": f"link-{cid}", "contact": cid, "account": aid}]

        async def fake_get(path, params=None):
            self.assertEqual(path, f"customObjects/records/{main.SLP_SCHEMA_ID}/slp-a")
            return {"record": slp("slp-a", "acct-a", "Awaiting Dealer Agreement Signature")}

        async def fake_post(path, body):
            writes.append((path, body))
            return {"record": body["record"]}

        async def fake_delete(path):
            deletes.append(path)
            return 200

        main.ac_get_all = fake_get_all
        main.ac_get = fake_get
        main.ac_post = fake_post
        main.ac_delete = fake_delete

        output = io.StringIO()
        with redirect_stdout(output):
            await main._sync_pandadoc_signed()

        self.assertEqual(
            lookup_params,
            [{"filters[contact]": "contact-a"}, {"filters[contact]": "contact-b"}],
        )
        self.assertEqual(len(writes), 1)
        self.assertEqual(
            deletes,
            ["contacts/contact-a/tags/4010"],
        )
        self.assertIn("unique_slps_transitioned=1", output.getvalue())
        self.assertIn("wrong_status=1", output.getvalue())

    async def test_unscoped_relationship_response_aborts_without_writes(self):
        writes = []

        async def fake_get_all(path, key, params=None):
            if path == "contacts":
                return [{"id": str(i)} for i in range(6)]
            requested = params["filters[contact]"]
            return [{"id": "bad", "contact": f"not-{requested}", "account": "acct-a"}]

        async def fake_post(path, body):
            writes.append((path, body))

        main.ac_get_all = fake_get_all
        main.ac_post = fake_post

        output = io.StringIO()
        with redirect_stdout(output):
            await main._sync_pandadoc_signed()

        self.assertFalse(writes)
        self.assertIn("aborting run after 5 consecutive lookup failures", output.getvalue())
        self.assertIn("aborted=1", output.getvalue())


class LastContactedSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_get_all = main.ac_get_all
        main._lc_cache = {"healthy-account": {"date": "2026-11-01", "type": "Note"}}
        main._lc_cache_ts = 123.0
        main._lc_cache_refreshing = False
        main._lc_cache_last_error = ""
        main._lc_cache_source_status = {}

    async def asyncTearDown(self):
        main.ac_get_all = self.original_get_all
        main._lc_cache = {}
        main._lc_cache_ts = 0.0
        main._lc_cache_refreshing = False
        main._lc_cache_last_error = ""
        main._lc_cache_source_status = {}

    async def test_required_source_failure_keeps_last_healthy_snapshot(self):
        async def fake_get_all(path, key, params=None):
            if path.startswith("customObjects/"):
                return []
            if path == "notes" and params.get("filters[reltype]") == "CustomerAccount":
                raise RuntimeError("simulated notes failure")
            if path == "accountContacts":
                return []
            if path == "notes":
                return []
            raise AssertionError((path, params))

        main.ac_get_all = fake_get_all
        before = dict(main._lc_cache)
        before_ts = main._lc_cache_ts

        await main._refresh_lc_cache()

        self.assertEqual(main._lc_cache, before)
        self.assertEqual(main._lc_cache_ts, before_ts)
        self.assertEqual(main._lc_cache_last_error, "account_notes")
        self.assertFalse(main._lc_cache_refreshing)


class ActiveCampaignBackoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_retries_429_then_succeeds_within_bound(self):
        class Response:
            def __init__(self, status, data, headers=None):
                self.status_code = status
                self._data = data
                self.headers = headers or {}

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP {self.status_code}")

            def json(self):
                return self._data

        responses = [
            Response(429, {}, {"Retry-After": "0.001"}),
            Response(200, {"contacts": []}),
        ]

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def request(self, *args, **kwargs):
                return responses.pop(0)

        original_client = ac_client._client
        original_interval = ac_client._MIN_REQUEST_INTERVAL
        ac_client._client = lambda: FakeClient()
        ac_client._MIN_REQUEST_INTERVAL = 0
        ac_client._next_request_at = 0
        try:
            status, data = await ac_client.request_json("GET", "contacts")
        finally:
            ac_client._client = original_client
            ac_client._MIN_REQUEST_INTERVAL = original_interval

        self.assertEqual(status, 200)
        self.assertEqual(data, {"contacts": []})
        self.assertFalse(responses)


if __name__ == "__main__":
    unittest.main()
