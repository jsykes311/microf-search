"""Mocked regression tests for license-tag writes; no app or network is started."""
import sys
import asyncio
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main


def association(contact="10", tag="20"):
    return {"contactTag": {"id": "99", "contact": contact, "tag": tag},
            "meta": {}, "links": {}}


class LicenseTagWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_tag_is_not_posted(self):
        with patch.object(main, "ac_get_all", AsyncMock(return_value=[{"id": "1", "tag": "20"}])), \
             patch.object(main, "ac_post", AsyncMock()) as post:
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_ALREADY_PRESENT)
        post.assert_not_awaited()

    async def test_successful_three_key_dictionary_is_counted(self):
        with patch.object(main, "ac_get_all", AsyncMock(return_value=[])), \
             patch.object(main, "ac_post", AsyncMock(return_value=association())):
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_APPLIED)

    async def test_failed_precheck_does_not_post(self):
        with patch.object(main, "ac_get_all", AsyncMock(side_effect=TimeoutError)), \
             patch.object(main, "ac_post", AsyncMock()) as post:
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_PRECHECK_FAILED)
        post.assert_not_awaited()

    async def test_uncertain_write_reconciles_when_tag_is_present(self):
        reads = AsyncMock(side_effect=[[], [{"id": "1", "tag": "20"}]])
        with patch.object(main, "ac_get_all", reads), \
             patch.object(main, "ac_post", AsyncMock(side_effect=TimeoutError)) as post:
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_APPLIED_RECONCILED)
        post.assert_awaited_once()

    async def test_uncertain_write_confirmed_absent_is_failed_without_retry(self):
        with patch.object(main, "ac_get_all", AsyncMock(side_effect=[[], []])), \
             patch.object(main, "ac_post", AsyncMock(side_effect=TimeoutError)) as post:
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_WRITE_FAILED)
        post.assert_awaited_once()

    async def test_uncertain_write_and_failed_verification_stays_uncertain(self):
        reads = AsyncMock(side_effect=[[], TimeoutError()])
        with patch.object(main, "ac_get_all", reads), \
             patch.object(main, "ac_post", AsyncMock(side_effect=TimeoutError)) as post:
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_WRITE_UNCERTAIN)
        post.assert_awaited_once()

    async def test_mismatched_response_is_reconciled_not_counted_as_success(self):
        with patch.object(main, "ac_get_all", AsyncMock(side_effect=[[], []])), \
             patch.object(main, "ac_post", AsyncMock(return_value=association("other", "20"))):
            result = await main._apply_tag_if_missing("10", "20")
        self.assertEqual(result, main._TAG_WRITE_FAILED)


class LicenseTagSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_dedupes_operations_and_counts_every_outcome(self):
        candidates = [
            {"bucket": main.LICENSE_TAG_30, "contact_ids": ["1", "2", "2"]},
            {"bucket": main.LICENSE_TAG_30, "contact_ids": ["2", "3"]},
        ]
        outcomes = {"1": main._TAG_ALREADY_PRESENT, "2": main._TAG_APPLIED,
                    "3": main._TAG_WRITE_UNCERTAIN}

        async def apply(contact_id, _tag_id):
            return outcomes[contact_id]

        with patch.object(main, "_license_expiration_candidates", AsyncMock(return_value=candidates)), \
             patch.object(main, "_get_tag_id", AsyncMock(side_effect=lambda name: name)), \
             patch.object(main, "_apply_tag_if_missing", AsyncMock(side_effect=apply)) as apply_mock:
            result = await main._run_license_expiration_tagging()

        summary = result["summary"][main.LICENSE_TAG_30]
        self.assertEqual(summary["matched_accounts"], 2)
        self.assertEqual(summary["contacts_considered"], 3)
        self.assertEqual(summary[main._TAG_ALREADY_PRESENT], 1)
        self.assertEqual(summary[main._TAG_APPLIED], 1)
        self.assertEqual(summary[main._TAG_WRITE_UNCERTAIN], 1)
        self.assertEqual(apply_mock.await_count, 3)

    async def test_second_run_is_skipped_while_first_is_active(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_run():
            started.set()
            await release.wait()
            return {"status": "completed"}

        main._license_tagging_task = None
        main._license_tagging_state = {"run_id": "active", "phase": "tagging",
                                       "operations_completed": 7}
        with patch.object(main, "_run_license_expiration_tagging_once", AsyncMock(side_effect=slow_run)) as once:
            first = asyncio.create_task(main._run_license_expiration_tagging())
            await started.wait()
            duplicate = await main._run_license_expiration_tagging()
            release.set()
            completed = await first

        self.assertEqual(completed["status"], "completed")
        self.assertEqual(duplicate, {"status": "already_running", "run_id": "active",
                                     "phase": "tagging", "operations_completed": 7})
        self.assertEqual(once.await_count, 1)
        self.assertIsNone(main._license_tagging_task)


class LicenseCandidateScopeTests(unittest.IsolatedAsyncioTestCase):
    def _license(self, account="77"):
        expiration = (date.today() + timedelta(days=30)).isoformat()
        return {"id": "lic-1", "fields": [{"id": "expiration-date", "value": expiration}],
                "relationships": {"account": [account]}}

    def _slp(self, account="77"):
        return {"id": "slp-1", "fields": [
            {"id": "channel", "value": main._LICENSE_DEACT_CHANNEL},
            {"id": "slp-status-detail", "value": "Activated"},
        ], "relationships": {"account": [account]}}

    async def test_fetches_contacts_only_for_qualifying_accounts(self):
        calls = []

        async def get_all(path, key, params=None):
            calls.append((path, params or {}))
            if path.startswith("customObjects/records/"):
                return [self._license()]
            self.assertEqual(path, "accountContacts")
            self.assertEqual((params or {}).get("filters[account]"), "77")
            return [{"id": "1", "account": "77", "contact": "10"},
                    {"id": "2", "account": "77", "contact": "10"},
                    {"id": "3", "account": "77", "contact": "11"}]

        with patch.object(main, "ac_get_all", AsyncMock(side_effect=get_all)), \
             patch.object(main, "get_slp_cache", AsyncMock(return_value=[self._slp()])):
            rows = await main._license_expiration_candidates()

        self.assertEqual(rows[0]["contact_ids"], ["10", "11"])
        self.assertEqual(len([c for c in calls if c[0] == "accountContacts"]), 1)

    async def test_rejects_unscoped_account_contact_results(self):
        async def get_all(path, key, params=None):
            if path.startswith("customObjects/records/"):
                return [self._license()]
            return [{"id": "1", "account": "other-client", "contact": "10"}]

        with patch.object(main, "ac_get_all", AsyncMock(side_effect=get_all)), \
             patch.object(main, "get_slp_cache", AsyncMock(return_value=[self._slp()])):
            with self.assertRaisesRegex(RuntimeError, "unscoped"):
                await main._license_expiration_candidates()


if __name__ == "__main__":
    unittest.main()
