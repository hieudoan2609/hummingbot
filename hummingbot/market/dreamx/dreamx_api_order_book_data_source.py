#!/usr/bin/env python

import aiohttp
import asyncio
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.core.utils import async_ttl_cache
from hummingbot.market.dreamx.dreamx_active_order_tracker import DREAMXActiveOrderTracker
from hummingbot.market.dreamxx.dreamxx_order_book import DREAMXOrderBook
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_tracker_entry import OrderBookTrackerEntry
from hummingbot.market.dreamx.dreamx_order_book_message import DREAMXOrderBookMessage
from hummingbot.market.dreamx.dreamx_order_book_tracker_entry import DREAMXOrderBookTrackerEntry

DREAMX_REST_URL = "https://api-testnet.dreamx.market"
DREAMX_WS_URL = "wss://api-testnet.dreamx.market/cable"


class DREAMXAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _iaobds_logger: Optional[logging.Logger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._iaobds_logger is None:
            cls._iaobds_logger = logging.getLogger(__name__)
        return cls._iaobds_logger

    def __init__(self, dreamx_api_key: str, trading_pairs: Optional[List[str]] = None):
        super().__init__()
        self._dreamx_api_key = dreamx_api_key
        self._trading_pairs: Optional[List[str]] = trading_pairs
        self._get_tracking_pair_done_event: asyncio.Event = asyncio.Event()

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_all_token_info(cls) -> Dict[str, Dict[str, Any]]:
        """
        Returns all token information
        """
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{DREAMX_REST_URL}/tokens") as response:
                response: aiohttp.ClientResponse = response
                if response.status != 200:
                    raise IOError(f"Error fetching token info. HTTP status is {response.status}.")
                data: Dict[str, Dict[str, Any]] = await response.json()
                return data

    @classmethod
    @async_ttl_cache(ttl=60 * 30, maxsize=1)
    async def get_active_exchange_markets(cls) -> pd.DataFrame:
        async with aiohttp.ClientSession() as client:
            async with client.get(f"{DREAMX_REST_URL}/tickers") as response:
                response: aiohttp.ClientResponse = response
                parsed_response: Dict[str, Dict[str, str]] = await response.json()
                data: List[Dict[str, Any]] = []
                for item in parsed_response["records"]:
                    trading_pair = item["market_symbol"]
                    base_volume = item["base_volume"]
                    quote_volume = item["quote_volume"]
                    base_asset, quote_asset = trading_pair.split("_")
                    data.append({
                        "market": trading_pair,
                        "base_asset": base_asset,
                        "quote_asset": quote_asset,
                        "base_volume": base_volume,
                        "quote_volume": quote_volume
                    })
                all_markets: pd.DataFrame = pd.DataFrame.from_records(data=data, index="market")
                return all_markets.sort_values("base_volume", ascending=False)

    async def get_trading_pairs(self) -> List[str]:
        if self._trading_pairs is None:
            active_markets: pd.DataFrame = await self.get_active_exchange_markets()
            trading_pairs: List[str] = active_markets.index.tolist()
            self._trading_pairs = trading_pairs
        else:
            trading_pairs: List[str] = self._trading_pairs
        return trading_pairs

    async def get_snapshot(self, client: aiohttp.ClientSession, trading_pair: str) -> Dict[str, Any]:
        orders: List[Dict[str, Any]] = []
        try:
            async with client.get(f"{DREAMX_REST_URL}/order_books/ETH_ONE?per_page=1000") as response:
                response: aiohttp.ClientResponse = response
                response = await response.json()
                bids = response["bid"]["records"]
                asks = response["ask"]["records"]
                orders = bids + asks
                return {"orders": orders}

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().network(
                f"Error getting snapshot for {trading_pair}.",
                exc_info=True,
                app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection."
            )
            await asyncio.sleep(5.0)

    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        # Get the currently active markets
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            retval: Dict[str, DREAMXOrderBookTrackerEntry] = {}
            number_of_pairs: int = len(trading_pairs)
            token_info: Dict[str, Dict[str, Any]] = await self.get_all_token_info()
            for index, trading_pair in enumerate(trading_pairs):
                try:
                    snapshot: Dict[str, any] = await self.get_snapshot(client, trading_pair)
                    snapshot_msg: DREAMXOrderBookMessage = DREAMXOrderBook.snapshot_message_from_exchange(
                        msg=snapshot,
                        timestamp=None,
                        metadata={"market": trading_pair}
                    )
                    base_asset_str, quote_asset_str = trading_pair.split("_")
                    base_asset: Dict[str, Any] = token_info[base_asset_str]
                    quote_asset: Dict[str, Any] = token_info[quote_asset_str]
                    dreamx_active_order_tracker: DREAMXActiveOrderTracker = DREAMXActiveOrderTracker(base_asset=base_asset,
                                                                                                     quote_asset=quote_asset)
                    bids, asks = dreamx_active_order_tracker.convert_snapshot_message_to_order_book_row(snapshot_msg)
                    snapshot_timestamp: float = dreamx_active_order_tracker.latest_snapshot_timestamp
                    dreamx_order_book: OrderBook = self.order_book_create_function()
                    dreamx_order_book.apply_snapshot(bids, asks, snapshot_timestamp)
                    retval[trading_pair] = DREAMXOrderBookTrackerEntry(
                        trading_pair,
                        snapshot_timestamp,
                        dreamx_order_book,
                        dreamx_active_order_tracker
                    )

                    self.logger().info(f"Initialized order book for {trading_pair}. "
                                       f"{index+1}/{number_of_pairs} completed.")
                    await asyncio.sleep(1.0)
                except Exception:
                    self.logger().error(f"Error initializing order book for {trading_pair}.", exc_info=True)
                    await asyncio.sleep(5)

            self._get_tracking_pair_done_event.set()
            return retval

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def _send_subscribe(self,
                              ws: websockets.WebSocketClientProtocol,
                              channel: str):
        subscribe_payload: str = ujson.dumps({
            "channel": channel
        })
        subscribe_request: Dict[str, Any] = {
            "command": "subscribe",
            "identifier": subscribe_payload
        }
        await ws.send(ujson.dumps(subscribe_request))

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        # Trade messages are received from the order book web socket
        pass

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with websockets.connect(DREAMX_WS_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    await self._send_subscribe(ws, "MarketOrdersChannel")
                    await self._send_subscribe(ws, "MarketTradesChannel")
                    async for raw_message in self._inner_messages(ws):
                        decoded: Dict[str, Any] = ujson.loads(raw_message)
                        message = decoded.get("message")
                        identifier = decoded.get("identifier")
                        if not message or not identifier:
                            # ignore ActionCable's generic messages
                            continue
                        diff_messages: List[Dict[str, Any]] = []
                        event: str = message["channel"]
                        # LEFT HERE
                        if event == "MarketOrders":
                            orders: List[str, Any] = message["payload"]
                            market: str = orders[0]["market_symbol"]
                            diff_messages = [{**o, "event": event, "market": market} for o in orders]
                        elif event == "MarketTrades":
                            trades: List[str, Any] = message["payload"]
                            diff_messages = [{**t, "event": event} for t in trades]
                        else:
                            # ignore message if event is not recognized
                            continue
                        for diff_message in diff_messages:
                            ob_message: DREAMXOrderBookMessage = DREAMXOrderBook.diff_message_from_exchange(diff_message)
                            output.put_nowait(ob_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Error getting order book diff messages.",
                    exc_info=True,
                    app_warning_msg=f"Error getting order book diff messages. Check network connection."
                )
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        await self._get_tracking_pair_done_event.wait()
        while True:
            try:
                trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: DREAMXOrderBookMessage = DREAMXOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                {"market": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair} at {snapshot_timestamp}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().network(
                                f"Error getting snapshot for {trading_pair}.",
                                exc_info=True,
                                app_warning_msg=f"Error getting snapshot for {trading_pair}. Check network connection."
                            )
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    f"Unexpected error listening for order book snapshot.",
                    exc_info=True,
                    app_warning_msg=f"Unexpected error listening for order book snapshot. Check network connection."
                )
                await asyncio.sleep(5.0)
