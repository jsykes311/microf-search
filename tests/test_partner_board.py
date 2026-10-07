"""Leaderboard tests use made-up production data; nothing touches ActiveCampaign."""
import sys
import unittest
from datetime import date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.testclient import TestClient
import partner_board as pb


def row(name, did, partner, apps, rpas, **kw):
    return {"dealer": name, "dealer_id": did, "account_id": f"A{did}", "strategic_partner": partner,
            "apps": apps, "approved": kw.get("approved", apps), "pending": 0, "rpas": rpas, "nia": 0, "revenue": 0.0}


def data():
    return {"periods": {
        "July 2026": {"production": [row("Acme", "1", "Alpha", 10, 4), row("Bolt", "2", "Alpha", 5, 1),
                                     row("Cobalt", "3", "Beta", 20, 3), row("Dyna", "4", "Gamma", 2, 0)],
                      "period_type": "monthly", "production_uploaded_at": "2026-08-01T10:00:00"},
        "August 2026": {"production": [row("Acme", "1", "Alpha", 6, 2), row("Bolt", "2", "Alpha", 0, 0),
                                       row("Cobalt", "3", "Beta", 12, 5), row("Dyna", "4", "Gamma", 2, 0),
                                       row("Echo", "5", "Delta", 9, 5)],
                        "period_type": "monthly", "production_uploaded_at": "2026-09-01T10:00:00"},
        # stored quarter rows drop the partner (as the real dump does) and must not double count
        "Q3 2026": {"production": [{"dealer": "Acme", "dealer_id": "1", "apps": 16, "rpas": 6}], "period_type": "quarterly"},
        "Q1 2026": {"production": [{"dealer": "Old", "dealer_id": "9", "apps": 7, "rpas": 2}], "period_type": "quarterly"},
    }}


class BoardTests(unittest.TestCase):
    def test_month_totals_and_per_contractor_subtotals(self):
        b = pb.build_board(data(), "m:July 2026")
        by = {p["partner"]: p for p in b["partners"]}
        self.assertEqual((by["Alpha"]["apps"], by["Alpha"]["rpas"]), (15, 5))
        self.assertEqual([(c["dealer"], c["apps"], c["rpas"]) for c in by["Alpha"]["contractor_rows"]],
                         [("Acme", 10, 4), ("Bolt", 5, 1)])
        self.assertEqual(b["totals"], {"partners": 3, "contractors": 4, "active_contractors": 4, "apps": 37, "rpas": 8})

    def test_ranking_by_rpas_then_apps_and_movement(self):
        b = pb.build_board(data(), "m:August 2026")
        self.assertEqual([(p["rank"], p["partner"]) for p in b["partners"]],
                         [(1, "Beta"), (2, "Delta"), (3, "Alpha"), (4, "Gamma")])   # Beta beats Delta on apps (12 v 9)
        self.assertEqual(b["prior_label"], "July 2026")
        by = {p["partner"]: p for p in b["partners"]}
        # July was Alpha 1, Beta 2, Gamma 3
        self.assertEqual((by["Beta"]["prior_rank"], by["Beta"]["rank_change"]), (2, 1))
        self.assertEqual((by["Alpha"]["prior_rank"], by["Alpha"]["rank_change"]), (1, -2))
        self.assertEqual(by["Gamma"]["rank_change"], -1)
        self.assertTrue(by["Delta"]["is_new"])
        self.assertIsNone(by["Delta"]["rank_change"])

    def test_ties_share_a_rank(self):
        d = {"periods": {"May 2026": {"production": [row("a", "1", "P1", 5, 2), row("b", "2", "P2", 5, 2),
                                                      row("c", "3", "P3", 1, 0)]}}}
        b = pb.build_board(d, "m:May 2026")
        self.assertEqual([p["rank"] for p in b["partners"]], [1, 1, 3])

    def test_ytd_adds_months_once_and_ignores_stored_quarters(self):
        b = pb.build_board(data(), "ytd")
        by = {p["partner"]: p for p in b["partners"]}
        self.assertEqual((by["Alpha"]["apps"], by["Alpha"]["rpas"]), (21, 7))     # 10+5+6+0, 4+1+2+0
        self.assertEqual(b["totals"]["rpas"], 7 + 8 + 0 + 5)
        self.assertNotIn("Old", [c["dealer"] for p in b["partners"] for c in p["contractor_rows"]])

    def test_partial_periods_are_flagged(self):
        self.assertTrue(pb.build_board(data(), "q:Q3 2026")["period"]["partial"])          # July + August only
        self.assertEqual(pb.build_board(data(), "q:Q3 2026")["period"]["months_expected"], 3)
        self.assertFalse(pb.build_board(data(), "m:July 2026")["period"]["partial"])
        ytd = pb.build_board(data(), "ytd")["period"]
        self.assertEqual((len(ytd["months"]), ytd["months_expected"], ytd["partial"]), (2, 8, True))   # Jul+Aug of Jan-Aug
        self.assertTrue(pb.build_board(data(), "ttm")["period"]["partial"])

    def test_quarter_built_from_months_keeps_partner(self):
        b = pb.build_board(data(), "q:Q3 2026")
        self.assertEqual({p["partner"] for p in b["partners"]}, {"Alpha", "Beta", "Gamma", "Delta"})
        self.assertEqual(b["totals"]["rpas"], 20)

    def test_stored_quarter_used_only_without_months_and_needs_lookup_for_partner(self):
        b = pb.build_board(data(), "q:Q1 2026", {"9": "Omega"})
        self.assertEqual([(p["partner"], p["rpas"]) for p in b["partners"]], [("Omega", 2)])
        b2 = pb.build_board(data(), "q:Q1 2026", {})
        self.assertEqual(b2["partners"], [])
        self.assertEqual(b2["unassigned"]["contractors"], 1)

    def test_current_partner_tag_wins_over_saved_row(self):
        b = pb.build_board(data(), "m:July 2026", {"3": "Alpha"})
        by = {p["partner"]: p for p in b["partners"]}
        self.assertEqual(by["Alpha"]["contractors"], 3)
        self.assertNotIn("Beta", by)

    def test_messy_numbers_do_not_crash(self):
        d = {"periods": {"June 2026": {"production": [row("a", "1", "P", "1,200", "x"), {"dealer": "b", "dealer_id": "2", "strategic_partner": "P", "apps": None, "rpas": 2.6}, "junk", {}]}}}
        b = pb.build_board(d, "m:June 2026")
        self.assertEqual((b["partners"][0]["apps"], b["partners"][0]["rpas"]), (1200, 3))

    def test_multiple_partners_on_one_contractor_credit_each(self):
        d = {"periods": {"June 2026": {"production": [row("a", "1", "P1, P2", 4, 1)]}}}
        b = pb.build_board(d, "m:June 2026")
        self.assertEqual({p["partner"] for p in b["partners"]}, {"P1", "P2"})

    def test_default_period_is_current_month_else_latest(self):
        _, default = pb.period_options(data(), today=date(2026, 8, 20))
        self.assertEqual(default, "m:August 2026")
        _, default = pb.period_options(data(), today=date(2026, 10, 2))
        self.assertEqual(default, "m:August 2026")

    def test_empty_and_unknown_period(self):
        self.assertFalse(pb.build_board({"periods": {}}, "")["has_data"])
        b = pb.build_board(data(), "m:January 2020")
        self.assertEqual(b["partners"], [])
        self.assertEqual(pb.build_board(data(), "bogus")["partners"], [])


class ApiTests(unittest.TestCase):
    def app(self, allowed=True):
        app = FastAPI()

        def require_admin(request: Request):
            if not allowed:
                raise HTTPException(403, "Admin only")
            return "admin"
        pb.install_partner_board(app, require_admin, data, lambda: ({"1": "Alpha"}, 7))
        return TestClient(app)

    def test_api_and_page(self):
        c = self.app()
        r = c.get("/api/partner-board?period=m:July%202026")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["coverage"]["partners_tagged_in_ac"], 7)
        self.assertEqual(c.get("/partner-leaderboard").status_code, 200)

    def test_requires_admin(self):
        c = self.app(allowed=False)
        self.assertEqual(c.get("/api/partner-board").status_code, 403)
        self.assertEqual(c.get("/partner-leaderboard").status_code, 403)


if __name__ == "__main__":
    unittest.main()
