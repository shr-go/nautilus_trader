# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio

import msgspec

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.data import BinanceCommonDataClient
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesEnumParser
from nautilus_trader.adapters.binance.futures.http.market import BinanceFuturesMarketHttpAPI
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesForceOrderMsg
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesMarkPriceAllMsg
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesMarkPriceData
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesMarkPriceMsg
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate
from nautilus_trader.adapters.binance.futures.types import BinanceLiquidation
from nautilus_trader.adapters.binance.futures.types import BinanceOpenInterest
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import Liquidation
from nautilus_trader.model.data import MarkPriceUpdate
from nautilus_trader.model.data import OpenInterest
from nautilus_trader.model.data import OrderBookDelta
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.identifiers import InstrumentId


class BinanceFuturesDataClient(BinanceCommonDataClient):
    """
    Provides a data client for the Binance Futures exchange.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : BinanceHttpClient
        The Binance HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : InstrumentProvider
        The instrument provider.
    base_url_ws : str
        The base URL for the WebSocket client.
    config : BinanceDataClientConfig
        The configuration for the client.
    account_type : BinanceAccountType, default 'USDT_FUTURES'
        The account type for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: BinanceHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InstrumentProvider,
        base_url_ws: str,
        config: BinanceDataClientConfig,
        account_type: BinanceAccountType = BinanceAccountType.USDT_FUTURES,
        name: str | None = None,
        base_url_ws_public: str | None = None,
    ) -> None:
        PyCondition.is_true(
            account_type.is_futures,
            "account_type was not USDT_FUTURES or COIN_FUTURES",
        )

        # Futures HTTP API
        self._futures_http_market = BinanceFuturesMarketHttpAPI(client, account_type)

        # Futures enum parser
        self._futures_enum_parser = BinanceFuturesEnumParser()

        # Instantiate common base class
        super().__init__(
            loop=loop,
            client=client,
            market=self._futures_http_market,
            enum_parser=self._futures_enum_parser,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            account_type=account_type,
            base_url_ws=base_url_ws,
            name=name,
            config=config,
            base_url_ws_public=base_url_ws_public,
        )

        # Register additional futures websocket handlers
        self._ws_handlers["@markPrice"] = self._handle_mark_price
        self._ws_handlers["!markPrice@arr"] = self._handle_mark_price_all
        self._ws_handlers["@forceOrder"] = self._handle_force_order

        # Websocket msgspec decoders
        self._decoder_futures_mark_price_msg = msgspec.json.Decoder(BinanceFuturesMarkPriceMsg)
        self._decoder_futures_mark_price_all_msg = msgspec.json.Decoder(
            BinanceFuturesMarkPriceAllMsg,
        )
        self._decoder_futures_force_order_msg = msgspec.json.Decoder(BinanceFuturesForceOrderMsg)

        # Open-interest REST polling state
        self._oi_poll_tasks: dict[InstrumentId, asyncio.Task] = {}
        self._oi_poll_default_secs: int = 5

    # -- WEBSOCKET HANDLERS ---------------------------------------------------------------------------------

    def _handle_book_partial_update(self, raw: bytes) -> None:
        msg = self._decoder_order_book_msg.decode(raw)
        instrument_id: InstrumentId = self._get_cached_instrument_id(msg.data.s)
        book_snapshot: OrderBookDeltas = msg.data.parse_to_order_book_deltas(
            instrument_id=instrument_id,
            ts_init=self._clock.timestamp_ns(),
            snapshot=True,
        )
        # Check if book buffer active
        book_buffer: list[OrderBookDelta | OrderBookDeltas] | None = self._book_buffer.get(
            instrument_id,
        )

        if book_buffer is not None:
            book_buffer.append(book_snapshot)
        else:
            self._handle_data(book_snapshot)

    def _handle_mark_price_data(self, data: BinanceFuturesMarkPriceData) -> None:
        instrument_id: InstrumentId = self._get_cached_instrument_id(data.s)
        data = data.parse_to_binance_futures_mark_price_update(
            instrument_id=instrument_id,
            ts_init=self._clock.timestamp_ns(),
        )
        data_type = DataType(
            BinanceFuturesMarkPriceUpdate,
            metadata={"instrument_id": instrument_id},
        )
        generic = CustomData(data_type=data_type, data=data)

        self._handle_data(generic)
        self._handle_data(
            MarkPriceUpdate(
                data.instrument_id,
                data.mark,
                data.ts_event,
                data.ts_init,
            ),
        )

    def _handle_mark_price(self, raw: bytes) -> None:
        msg = self._decoder_futures_mark_price_msg.decode(raw)
        self._handle_mark_price_data(msg.data)

    def _handle_mark_price_all(self, raw: bytes) -> None:
        msg = self._decoder_futures_mark_price_all_msg.decode(raw)
        for data in msg.data:
            self._handle_mark_price_data(data)

    def _handle_force_order(self, raw: bytes) -> None:
        msg = self._decoder_futures_force_order_msg.decode(raw)
        instrument_id: InstrumentId = self._get_cached_instrument_id(msg.data.o.s)
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(
                f"Cannot parse Binance forceOrder: instrument not in cache for {instrument_id}",
            )
            return

        ts_init = self._clock.timestamp_ns()

        binance_liq = msg.data.parse_to_binance_liquidation(
            instrument_id=instrument_id,
            price_precision=instrument.price_precision,
            size_precision=instrument.size_precision,
            ts_init=ts_init,
        )
        liq = msg.data.parse_to_liquidation(
            instrument_id=instrument_id,
            price_precision=instrument.price_precision,
            size_precision=instrument.size_precision,
            ts_init=ts_init,
        )

        data_type = DataType(
            BinanceLiquidation,
            metadata={"instrument_id": instrument_id},
        )
        self._handle_data(CustomData(data_type=data_type, data=binance_liq))
        self._handle_data(liq)

    # -- OPEN INTEREST POLLING ------------------------------------------------------------------

    def _start_open_interest_polling(
        self,
        instrument_id: InstrumentId,
        interval_secs: int | None = None,
    ) -> None:
        if instrument_id in self._oi_poll_tasks:
            return
        secs = interval_secs if interval_secs is not None else self._oi_poll_default_secs
        if secs < 5:
            secs = 5  # Do not poll REST faster than 5s by default

        task = self._loop.create_task(
            self._open_interest_poll_loop(instrument_id, secs),
            name=f"oi-poll-{instrument_id}",
        )
        self._oi_poll_tasks[instrument_id] = task

    def _stop_open_interest_polling(self, instrument_id: InstrumentId) -> None:
        task = self._oi_poll_tasks.pop(instrument_id, None)
        if task is not None and not task.done():
            task.cancel()

    async def _open_interest_poll_loop(
        self,
        instrument_id: InstrumentId,
        interval_secs: int,
    ) -> None:
        instrument = self._cache.instrument(instrument_id)
        if instrument is None:
            self._log.warning(
                f"Cannot start OI polling: instrument not in cache for {instrument_id}",
            )
            return

        backoff = 1.0
        symbol = instrument_id.symbol.value
        while True:
            try:
                response = await self._futures_http_market.query_open_interest(symbol)
                ts_init = self._clock.timestamp_ns()
                binance_oi = response.parse_to_binance_open_interest(
                    instrument_id=instrument_id,
                    size_precision=instrument.size_precision,
                    poll_interval_secs=interval_secs,
                    ts_init=ts_init,
                )
                oi = response.parse_to_open_interest(
                    instrument_id=instrument_id,
                    size_precision=instrument.size_precision,
                    ts_init=ts_init,
                )
                data_type = DataType(
                    BinanceOpenInterest,
                    metadata={"instrument_id": instrument_id},
                )
                self._handle_data(CustomData(data_type=data_type, data=binance_oi))
                self._handle_data(oi)

                backoff = 1.0
                await asyncio.sleep(interval_secs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.warning(
                    f"OI polling error for {instrument_id}: {e}; backing off {backoff:.1f}s",
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)
