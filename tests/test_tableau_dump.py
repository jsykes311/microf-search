"""Un-pivoting tests use made-up rows."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
from tableau_dump import depivot_measures

MEASURES = ["Approved Amount", "CCR", "NIA ", "Total Job Price"]   # Tableau leaves a trailing space on "NIA "


def pivoted(apps, with_id=True):
    rows = []
    for i, (did, status, nia) in enumerate(apps, 1):
        for m in MEASURES:
            r = {"Dealer Id": did, "inserted_time": "2026-05-0%d" % i, "Primary App": 1, "App Sub Status": status,
                 "Measure Names": m, "Measure Values": {"NIA ": nia}.get(m, 100.0 * i)}
            if with_id:
                r["Application Id"] = 1000 + i
            rows.append(r)
    return pd.DataFrame(rows)


class DepivotTests(unittest.TestCase):
    def test_not_pivoted_is_returned_untouched(self):
        df = pd.DataFrame({"Dealer Id": [1, 2], "inserted_time": ["2026-05-01", "2026-05-02"], "NIA": [5, 6]})
        out, info = depivot_measures(df)
        self.assertIsNone(info)
        self.assertIs(out, df)

    def test_one_row_per_application_with_measure_columns(self):
        out, info = depivot_measures(pivoted([(11, "FUNDED", 50.0), (12, "DECLINED", 0.0), (13, "FUNDED", 75.0)]))
        self.assertEqual((info["rows_before"], info["rows_after"]), (12, 3))
        self.assertEqual(len(out), 3)
        self.assertIn("NIA", out.columns)                  # the trailing space is stripped
        self.assertNotIn("Measure Names", out.columns)
        self.assertEqual(out["NIA"].tolist(), [50.0, 0.0, 75.0])
        self.assertEqual(out["Dealer Id"].tolist(), [11, 12, 13])    # original order kept
        self.assertEqual(int((out["App Sub Status"] == "FUNDED").sum()), 2)

    def test_works_without_an_application_id(self):
        out, info = depivot_measures(pivoted([(11, "FUNDED", 50.0), (12, "FUNDED", 60.0)], with_id=False))
        self.assertEqual(len(out), 2)
        self.assertEqual(out["NIA"].tolist(), [50.0, 60.0])

    def test_identical_applications_are_not_merged(self):
        one = pivoted([(11, "FUNDED", 50.0)], with_id=False)
        twice = pd.concat([one, one], ignore_index=True)           # two real, indistinguishable applications
        out, _ = depivot_measures(twice)
        self.assertEqual(len(out), 2)

    def test_existing_column_is_never_overwritten(self):
        df = pivoted([(11, "FUNDED", 50.0)])
        df["CCR"] = "keep me"
        out, info = depivot_measures(df)
        self.assertEqual(out["CCR"].tolist(), ["keep me"])
        self.assertNotIn("CCR", info["measures"])

    def test_missing_measure_value_is_blank_not_dropped(self):
        df = pivoted([(11, "FUNDED", 50.0), (12, "FUNDED", 60.0)])
        df = df[~((df["Application Id"] == 1002) & (df["Measure Names"] == "CCR"))]
        out, _ = depivot_measures(df)
        self.assertEqual(len(out), 2)
        self.assertTrue(pd.isna(out.loc[1, "CCR"]))


if __name__ == "__main__":
    unittest.main()
