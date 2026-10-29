"""Concurrency tests for the Team Activity stale-while-revalidate cache."""
import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main


class TeamActivityCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_refresh = main._refresh_ta_cache
        main._ta_cache = {}
        main._ta_cache_ts = 0.0
        main._ta_refresh_task = None

    async def asyncTearDown(self):
        if main._ta_refresh_task and not main._ta_refresh_task.done():
            main._ta_refresh_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await main._ta_refresh_task
        main._refresh_ta_cache = self.original_refresh
        main._ta_cache = {}
        main._ta_cache_ts = 0.0
        main._ta_refresh_task = None

    async def test_concurrent_cold_requests_share_one_refresh(self):
        calls = 0
        release = asyncio.Event()

        async def refresh():
            nonlocal calls
            calls += 1
            await release.wait()
            main._ta_cache = {"snapshot": "fresh"}
            main._ta_cache_ts = main._time.time()

        main._refresh_ta_cache = refresh
        requests = [asyncio.create_task(main._get_ta_cache()) for _ in range(5)]
        for _ in range(3):
            await asyncio.sleep(0)
            if calls:
                break
        self.assertEqual(calls, 1)
        release.set()
        self.assertEqual(await asyncio.gather(*requests), [{"snapshot": "fresh"}] * 5)

    async def test_expired_cache_is_served_while_single_refresh_runs(self):
        calls = 0
        release = asyncio.Event()
        stale = {"snapshot": "stale"}
        main._ta_cache = stale
        main._ta_cache_ts = main._time.time() - main._TA_CACHE_TTL - 1

        async def refresh():
            nonlocal calls
            calls += 1
            await release.wait()
            main._ta_cache = {"snapshot": "fresh"}
            main._ta_cache_ts = main._time.time()

        main._refresh_ta_cache = refresh
        results = await asyncio.gather(*(main._get_ta_cache() for _ in range(5)))
        self.assertEqual(results, [stale] * 5)
        await asyncio.sleep(0)
        self.assertEqual(calls, 1)
        release.set()
        await main._ta_refresh_task
        self.assertEqual(main._ta_cache, {"snapshot": "fresh"})


if __name__ == "__main__":
    unittest.main()
