"""Account merge tests run against an in-memory fake of the ActiveCampaign API."""
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import account_merge as am


class FakeAC:
    """Just enough of AC: paging, duplicate account-contact links rejected, SLP upsert by id."""

    def __init__(self):
        self.accounts = {"1": {"id": "1", "name": "Source"}, "2": {"id": "2", "name": "Target"}}
        self.links = {}      # id -> {id, account, contact, jobTitle}
        self.contacts = {}
        self.deals = {}
        self.slps = {}
        self.notes = {}
        self.calls = []
        self.fail_put_deal = set()
        self._n = 1000

    def link(self, account, contact, job=""):
        self._n += 1
        self.links[str(self._n)] = {"id": str(self._n), "account": str(account), "contact": str(contact), "jobTitle": job}
        self.contacts.setdefault(str(contact), {"id": str(contact), "firstName": f"C{contact}", "lastName": "", "email": f"c{contact}@x.com"})

    @staticmethod
    def _page(rows, params):
        off, lim = int(params.get("offset", 0)), int(params.get("limit", 100))
        return rows[off:off + lim]

    async def get(self, path, params=None):
        params = params or {}
        self.calls.append(("GET", path))
        if path.startswith("accounts/") and path.endswith("/accountContacts"):
            acct = path.split("/")[1]
            return {"accountContacts": self._page([l for l in self.links.values() if l["account"] == acct], params)}
        if path.endswith("/accountCustomFieldData"):
            return {"customerAccountCustomFieldData": []}
        if path.startswith("accounts/"):
            return {"account": self.accounts[path.split("/")[1]]}
        if path.startswith("contacts/"):
            return {"contact": self.contacts[path.split("/")[1]]}
        if path == "deals":
            return {"deals": self._page([d for d in self.deals.values() if d["account"] == params["filters[account]"]], params)}
        if path == "notes":
            return {"notes": self._page([n for n in self.notes.values() if n["relid"] == params["rel_id"]], params)}
        if path.startswith("customObjects/records/") and path.count("/") == 3:
            return {"record": self.slps[path.split("/")[3]]}
        if path.startswith("customObjects/records/"):
            acct = params["filters[relationships.account][eq]"]
            return {"records": self._page([r for r in self.slps.values() if acct in r["relationships"]["account"]], params)}
        raise AssertionError(path)

    async def post(self, path, body):
        self.calls.append(("POST", path))
        if path == "accountContacts":
            c = body["accountContact"]
            if any(l["contact"] == str(c["contact"]) and l["account"] == str(c["account"]) for l in self.links.values()):
                raise Exception("HTTP 422 contact already exists on account")
            self.link(c["account"], c["contact"])
            return {}
        if path.startswith("customObjects/records/"):
            r = body["record"]
            self.slps[r["id"]] = {"id": r["id"], "fields": r["fields"], "relationships": {"account": [str(a) for a in r["relationships"]["account"]]}}
            return {"record": self.slps[r["id"]]}
        if path == "notes":
            self._n += 1
            n = body["note"]
            self.notes[str(self._n)] = {"id": str(self._n), "note": n["note"], "relid": str(n["relid"])}
            return {}
        raise AssertionError(path)

    async def put(self, path, body):
        self.calls.append(("PUT", path))
        if path.startswith("accountContacts/"):
            self.links[path.split("/")[1]].update({"account": str(body["accountContact"]["account"]),
                                                    "jobTitle": body["accountContact"].get("jobTitle", "")})
            return {}
        if path.startswith("deals/"):
            did = path.split("/")[1]
            if did in self.fail_put_deal:
                raise Exception("HTTP 500")
            self.deals[did]["account"] = str(body["deal"]["account"])
            return {}
        raise AssertionError(path)

    async def delete(self, path):
        self.calls.append(("DELETE", path))
        kind, ident = path.split("/")
        store = {"accountContacts": self.links, "notes": self.notes, "accounts": self.accounts}[kind]
        store.pop(ident, None)
        return 200


def run(coro):
    return asyncio.run(coro)


def slp(sid, acct):
    return {"id": sid, "fields": [{"id": "dealer-id", "value": "123"}], "relationships": {"account": [acct]}}


class MergeTests(unittest.TestCase):
    def setUp(self):
        self.ac = FakeAC()
        self.dir = Path(tempfile.mkdtemp())
        self.ac.link("1", "10", "Owner")
        self.ac.link("1", "11")
        self.ac.link("2", "11")          # already on the target
        self.ac.link("2", "12")
        self.ac.deals["50"] = {"id": "50", "title": "LTO", "account": "1"}
        self.ac.slps["s1"] = slp("s1", "1")
        self.ac.slps["s2"] = slp("s2", "2")

    def merge(self, **kw):
        kw.setdefault("contact_ids", ["10", "11"])
        kw.setdefault("deal_ids", ["50"])
        kw.setdefault("slp_ids", ["s1"])
        return run(am.merge_accounts(self.ac, "1", "2", directory=self.dir, **kw))

    def test_contact_already_on_target_is_not_reinserted(self):
        res = self.merge()
        self.assertTrue(res["ok"], res["errors"])
        self.assertNotIn(("POST", "accountContacts"), self.ac.calls)
        on_target = sorted(l["contact"] for l in self.ac.links.values() if l["account"] == "2")
        self.assertEqual(on_target, ["10", "11", "12"])
        self.assertEqual([l for l in self.ac.links.values() if l["account"] == "1"], [])
        self.assertEqual(next(r for r in res["contacts"] if r["id"] == "11")["detail"], "already on target")

    def test_job_title_survives_move(self):
        self.merge()
        link = next(l for l in self.ac.links.values() if l["contact"] == "10")
        self.assertEqual((link["account"], link["jobTitle"]), ("2", "Owner"))

    def test_deals_and_slps_move(self):
        self.merge()
        self.assertEqual(self.ac.deals["50"]["account"], "2")
        self.assertEqual(self.ac.slps["s1"]["relationships"]["account"], ["2"])
        self.assertEqual(self.ac.slps["s1"]["fields"], [{"id": "dealer-id", "value": "123"}])
        self.assertEqual(len(self.ac.slps), 2)   # upserted, not duplicated

    def test_backup_written_before_any_change_and_has_both_sides(self):
        res = self.merge()
        data = json.loads((self.dir / res["backup"]).read_text())
        self.assertEqual({d["id"] for d in data["source"]["deals"]}, {"50"})
        self.assertEqual({c["id"] for c in data["source"]["contacts"]}, {"10", "11"})
        self.assertEqual({c["id"] for c in data["target"]["contacts"]}, {"11", "12"})
        self.assertEqual([r["id"] for r in data["source"]["slps"]], ["s1"])
        first_write = next(i for i, c in enumerate(self.ac.calls) if c[0] != "GET")
        self.assertTrue(first_write > 0)

    def test_backup_failure_changes_nothing(self):
        blocker = self.dir / "file"
        blocker.write_text("x")
        with self.assertRaises(OSError):
            run(am.merge_accounts(self.ac, "1", "2", slp_ids=["s1"], contact_ids=["10"], deal_ids=["50"], directory=blocker / "sub"))
        self.assertEqual([c for c in self.ac.calls if c[0] != "GET"], [])

    def test_delete_source_only_when_empty(self):
        res = self.merge(delete_source=True)
        self.assertTrue(res["deleted_source"])
        self.assertNotIn("1", self.ac.accounts)

    def test_delete_refused_when_something_left_behind(self):
        res = self.merge(contact_ids=["10"], delete_source=True)   # contact 11 left on source
        self.assertFalse(res["deleted_source"])
        self.assertIn("1", self.ac.accounts)
        self.assertTrue(any("still has" in e for e in res["errors"]))

    def test_delete_refused_after_a_failed_step(self):
        self.ac.fail_put_deal = {"50"}
        res = self.merge(delete_source=True)
        self.assertFalse(res["ok"])
        self.assertFalse(res["deleted_source"])
        self.assertIn("1", self.ac.accounts)

    def test_rerun_after_partial_failure_is_safe(self):
        self.ac.fail_put_deal = {"50"}
        self.merge()
        self.ac.fail_put_deal = set()
        res = self.merge(delete_source=True)
        self.assertTrue(res["ok"], res["errors"])
        self.assertTrue(res["deleted_source"])

    def test_more_than_one_page_of_contacts(self):
        for c in range(200, 340):
            self.ac.link("1", str(c))
        ids = [l["contact"] for l in self.ac.links.values() if l["account"] == "1"]
        res = self.merge(contact_ids=ids, delete_source=True)
        self.assertTrue(res["ok"], res["errors"])
        self.assertTrue(res["deleted_source"])

    def test_notes_all_move_even_across_pages(self):
        for i in range(60):
            self.ac.notes[f"n{i}"] = {"id": f"n{i}", "note": f"note {i}", "relid": "1"}
        res = self.merge(move_notes_too=True, delete_source=True)
        self.assertEqual(res["notes"], {"moved": 60, "errors": 0})
        self.assertEqual(len([n for n in self.ac.notes.values() if n["relid"] == "2"]), 60)
        self.assertEqual([n for n in self.ac.notes.values() if n["relid"] == "1"], [])

    def test_same_account_rejected(self):
        with self.assertRaises(ValueError):
            run(am.merge_accounts(self.ac, "1", "1", directory=self.dir))


class BackupFileTests(unittest.TestCase):
    def test_read_backup_rejects_paths(self):
        for bad in ("../../etc/passwd", "account_merge_1_to_2.json", "x.json", ""):
            with self.assertRaises(FileNotFoundError):
                am.read_backup(bad)

    def test_read_backup_roundtrip(self):
        d = Path(tempfile.mkdtemp())
        name, _ = am.write_backup("1", "2", {"a": 1}, {"b": 2}, {}, d)
        self.assertEqual(json.loads(am.read_backup(name, d))["source"], {"a": 1})


if __name__ == "__main__":
    unittest.main()
