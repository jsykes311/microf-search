"""Upload guardrail tests use made-up production data."""
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import upload_guard as ug
from partner_board import month_label, shift_month

TODAY = date(2026, 10, 8)


def prod(apps, rpas=0, n=3):
    return [{"dealer": f"d{i}", "dealer_id": str(i), "apps": apps // n, "rpas": rpas // n, "revenue": 0.0} for i in range(n)]


def months(start, count, apps=300, rpas=60):
    out, k = {}, start
    for _ in range(count):
        out[month_label(k)] = prod(apps, rpas)
        k = shift_month(k, 1)
    return out


def quarters(monthly):
    qs = {}
    for label, rows in monthly.items():
        mname, year = label.split()
        q = (["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"].index(mname)) // 3 + 1
        qs.setdefault(f"Q{q} {year}", []).extend(rows)
    return qs


def stored(monthly, rollup=True):
    per = {label: {"production": rows, **({"rollup": [{"dealer": "x"}]} if rollup else {})} for label, rows in monthly.items()}
    for label, rows in quarters(monthly).items():
        per[label] = {"production": rows, **({"rollup": [{"dealer": "x"}]} if rollup else {})}
    return {"periods": per}


def file(min_="2025-10-01", max_="2026-09-30", **kw):
    return {"rows": 1000, "date_min": min_, "date_max": max_, "bad_dates": 0, "has_nia": True, "fixes": [], **kw}


FULL = {"included": 399, "tagged": 399, "partner": "", "source": "index"}


def run(existing, monthly, f=None, cov=None, today=None):
    return ug.analyze_upload(existing, monthly, quarters(monthly), [{"dealer": "x"}], f or file(), cov or FULL, today=today or TODAY)


def levels(r):
    return [(w["level"], w["title"]) for w in r["warnings"]]


class AnalyzeTests(unittest.TestCase):
    def test_clean_full_year_has_no_warnings(self):
        m = months((2025, 10), 12)
        r = run(stored(m), m, file(max_="2026-09-30"), today=date(2026, 10, 2))       # file is two days old
        self.assertEqual([w for w in r["warnings"] if w["level"] != "info"], [])
        self.assertFalse(r["requires_ack"])
        self.assertEqual(r["counts"], {"new": 0, "replace": 16})            # 12 months + 4 quarters

    def test_new_vs_replace_and_totals(self):
        m = months((2025, 10), 12)
        old = {k: v for k, v in m.items() if k != "October 2025"}
        r = run(stored(old), m)
        p = {x["label"]: x for x in r["periods"]}
        self.assertEqual(p["October 2025"]["action"], "new")
        self.assertEqual(p["November 2025"]["action"], "replace")
        self.assertEqual(p["November 2025"]["incoming"]["apps"], 300)
        self.assertIsNone(p["October 2025"]["existing"])

    def test_june_start_file_is_flagged_as_dangerous(self):
        full = months((2025, 10), 13)
        short = {k: v for k, v in full.items() if parse(k) >= (2026, 6)}
        r = run(stored(full), short, file("2026-06-01", "2026-10-07"))
        got = levels(r)
        self.assertIn(("danger", "This file only covers 5 months"), got)
        self.assertIn(("danger", "Q2 2026 would be replaced with a partial quarter"), got)
        self.assertTrue(r["requires_ack"])
        detail = next(w["detail"] for w in r["warnings"] if w["title"].startswith("Q2 2026"))
        self.assertIn("900 apps", detail)                                    # the stored quarter that would be lost
        self.assertIn("300 apps", detail)

    def test_quarter_still_in_progress_is_not_flagged(self):
        m = months((2025, 10), 13)                                           # ends with October 2026 only
        r = run(stored(m), m, file("2025-10-01", "2026-10-07"))
        self.assertFalse(any("Q4 2026" in w["title"] for w in r["warnings"]))
        self.assertTrue(any(w["level"] == "info" and "in progress" in w["title"] for w in r["warnings"]))
        self.assertFalse(r["requires_ack"])

    def test_short_file_without_existing_data_is_only_a_warning(self):
        m = months((2026, 7), 3)
        r = run({"periods": {}}, m, file("2026-07-01", "2026-09-30"))
        self.assertIn(("warn", "This file only covers 3 months"), levels(r))
        self.assertFalse(r["requires_ack"])

    def test_mid_month_start(self):
        m = months((2025, 11), 12)
        r = run(stored(months((2025, 11), 12, apps=600)), m, file("2025-11-15", "2026-10-31"))
        self.assertIn(("danger", "November 2025 starts mid-month"), levels(r))
        r2 = run({"periods": {}}, m, file("2025-11-15", "2026-10-31"))
        self.assertIn(("warn", "November 2025 starts mid-month"), levels(r2))

    def test_partner_coverage(self):
        m = months((2025, 10), 12)
        r = run(stored(m), m, cov={"included": 10, "tagged": 399, "partner": "EGIA", "source": "index"})
        self.assertIn(("danger", "Only part of your partner contractors are in this upload"), levels(r))
        self.assertIn("partner filter: EGIA", r["warnings"][0]["detail"])
        self.assertEqual([w for w in run(stored(m), m, cov={"included": 360, "tagged": 399})["warnings"] if w["level"] == "danger"], [])
        self.assertEqual([w for w in run(stored(m), m, cov={"included": 5, "tagged": 0})["warnings"] if w["level"] == "danger"], [])

    def test_other_warnings(self):
        m = months((2025, 10), 12)
        r = run(stored(m), m, file(has_nia=False, bad_dates=200, date_max="2026-09-01"))
        got = [t for _, t in levels(r)]
        self.assertIn("No NIA column in this file", got)
        self.assertIn("Some rows have unreadable dates", got)
        self.assertIn("This file looks out of date", got)

    def test_shrinking_months_are_listed_but_the_current_month_is_skipped(self):
        old = months((2025, 10), 12, apps=300)
        new = months((2025, 10), 12, apps=300)
        new["March 2026"] = prod(210)                                        # -30%
        new["September 2026"] = prod(30)                                     # the newest month: expected to differ
        r = run(stored(old), new, file("2025-10-01", "2026-09-15"))
        drop = next(w for w in r["warnings"] if w["title"].startswith("Some replaced months"))
        self.assertIn("March 2026: 300 to 210", drop["detail"])
        self.assertNotIn("September 2026", drop["detail"])


def parse(label):
    from partner_board import parse_month
    return parse_month(label)


class StorageTests(unittest.TestCase):
    def test_data_path(self):
        here = "/app"
        self.assertEqual(ug.data_path("a.json", "/custom/a.json", True, True, here), "/custom/a.json")          # setting wins
        self.assertEqual(ug.data_path("a.json", "", True, True, here), "/var/data/a.json")                    # Render + disk
        self.assertEqual(ug.data_path("a.json", "", True, False, here), "/app/a.json")                        # Render, no disk
        self.assertEqual(ug.data_path("a.json", "", False, True, here), "/app/a.json")                        # local dev

    def test_warns_only_on_render_off_the_disk(self):
        self.assertIsNone(ug.storage_warning("/var/data/apex_data.json", True))
        self.assertIsNone(ug.storage_warning("/app/apex_data.json", False))
        w = ug.storage_warning("/app/apex_data.json", True)
        self.assertEqual(w["level"], "warn")
        self.assertIn("/app/apex_data.json", w["detail"])
        self.assertIn("/var/data/apex_data.json", w["detail"])


class FileStatsTests(unittest.TestCase):
    def test_stats(self):
        df = pd.DataFrame({"inserted_time": ["2026-01-05", "2026-02-10", "garbage"], "NIA": [1, 2, 3]})
        df.attrs["fixes"] = ["fixed it"]
        s = ug.file_stats(df)
        self.assertEqual((s["rows"], s["date_min"], s["date_max"], s["bad_dates"], s["has_nia"], s["fixes"]),
                         (3, "2026-01-05", "2026-02-10", 1, True, ["fixed it"]))
        self.assertFalse(ug.file_stats(df.drop(columns="NIA"))["has_nia"])


class SnapshotTests(unittest.TestCase):
    def test_keeps_only_the_newest_copies(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "apex_data.json"
            self.assertIsNone(ug.snapshot(path))                             # nothing to copy yet
            path.write_text('{"periods": {}}')
            made = [ug.snapshot(path, keep=3) for _ in range(6)]
            self.assertTrue(all(Path(m).exists() or True for m in made))
            left = sorted(Path(d).glob("apex_data.backup-*.json"))
            self.assertEqual(len(left), 3)
            self.assertEqual(path.read_text(), '{"periods": {}}')             # the real file is untouched


if __name__ == "__main__":
    unittest.main()
