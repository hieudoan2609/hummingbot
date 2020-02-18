#!/usr/bin/env python

from os.path import join, realpath
import sys
import asyncio
import logging
import unittest
from typing import (
    Dict,
    Optional
)

from hummingbot.market.dreamx.dreamx_order_book_tracker import DREAMXOrderBookTracker
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_tracker import OrderBookTrackerDataSourceType
from hummingbot.core.utils.async_utils import safe_ensure_future

sys.path.insert(0, realpath(join(__file__, "../../../")))

TEST_PAIR_1 = "ETH_ONE"
TEST_PAIRS = [TEST_PAIR_1]


class DREAMXOrderBookTrackerUnitTest(unittest.TestCase):
    order_book_tracker: Optional[DREAMXOrderBookTracker] = None

    @classmethod
    def setUpClass(cls):
        cls.ev_loop: asyncio.BaseEventLoop = asyncio.get_event_loop()
        cls.order_book_tracker: DREAMXOrderBookTracker = DREAMXOrderBookTracker(
            data_source_type=OrderBookTrackerDataSourceType.EXCHANGE_API,
            trading_pairs=TEST_PAIRS
        )
        cls.order_book_tracker_task: asyncio.Task = safe_ensure_future(cls.order_book_tracker.start())
        cls.ev_loop.run_until_complete(cls.wait_til_tracker_ready())

    @classmethod
    async def wait_til_tracker_ready(cls):
        while True:
            if len(cls.order_book_tracker.order_books) > 0:
                print("Initialized real-time order books.")
                return
            await asyncio.sleep(1000)

    def test_tracker_integrity(self):
        # Wait 30 seconds to process some diffs.
        self.ev_loop.run_until_complete(asyncio.sleep(30.0))
        order_books: Dict[str, OrderBook] = self.order_book_tracker.order_books
        eth_one_book: OrderBook = order_books[TEST_PAIR_1]
        print("eth_one_book")
        print(eth_one_book.snapshot)
        self.assertGreaterEqual(eth_one_book.get_price_for_volume(True, 10), eth_one_book.get_price(True))
        self.assertLessEqual(eth_one_book.get_price_for_volume(False, 10), eth_one_book.get_price(False))


def main():
    logging.basicConfig(level=logging.INFO)
    unittest.main()


if __name__ == "__main__":
    main()
