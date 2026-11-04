"""SLP status rename: 'Contractor Activated' -> 'Activated' (helpers compiled in isolation from main.py)."""
import ast
import re
import unittest
from pathlib import Path

SRC = (Path(__file__).resolve().parents[1] / "main.py").read_text()


def load_helpers() -> dict:
    tree, wanted, parts = ast.parse(SRC), {"_ACTIVATED_STATUSES", "_is_activated", "_status_matches"}, []
    for node in tree.body:
        name = node.name if isinstance(node, ast.FunctionDef) else (
            node.targets[0].id if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name) else None)
        if name in wanted:
            parts.append(ast.get_source_segment(SRC, node))
    ns: dict = {}
    exec("\n".join(parts), ns)
    return ns


class ActivatedStatus(unittest.TestCase):
    def setUp(self):
        self.h = load_helpers()

    def test_both_spellings_are_activated(self):
        is_act = self.h["_is_activated"]
        self.assertTrue(is_act("Activated"))
        self.assertTrue(is_act("Contractor Activated"))
        self.assertTrue(is_act("  Activated "))

    def test_other_statuses_are_not_activated(self):
        is_act = self.h["_is_activated"]
        for s in ("Deactivated", "Deactivated - Fraud", "Pre-activation", "Approved", "", None):
            self.assertFalse(is_act(s), s)

    def test_status_filter_treats_spellings_as_one(self):
        m = self.h["_status_matches"]
        self.assertTrue(m("Contractor Activated", "Activated"))
        self.assertTrue(m("Activated", "Contractor Activated"))
        self.assertTrue(m("Declined", "Declined"))
        self.assertFalse(m("Deactivated", "Activated"))
        self.assertFalse(m("Declined", "Deactivated - Fraud"))

    def test_no_raw_comparison_left_in_main(self):
        left = [ln.strip() for ln in SRC.splitlines() if re.search(r'[!=]= "Contractor Activated"', ln)]
        self.assertEqual(left, [], "use _is_activated() instead of comparing to the old spelling")


if __name__ == "__main__":
    unittest.main()
