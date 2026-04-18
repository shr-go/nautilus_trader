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

        # Open-interest REST polling state.
        #
        # Poll tasks and subscriber tracking are keyed by
        # (instrument_id, interval_secs) so two subscribers that request the
        # same instrument with DIFFERENT intervals each get their own cadence
        # and their own `poll_interval_secs` field on emitted
        # `BinanceOpenInterest` samples. Keying by instrument alone would let
        # the second subscriber silently share the first's interval.
        self._oi_poll_tasks: dict[tuple[InstrumentId, int], asyncio.Task] = {}
        self._oi_poll_default_secs: int = 5
        # Track every `DataType` currently subscribed per (instrument_id,
        # interval_secs) so the poller can emit on exactly those topics —
        # a subscription with `interval_secs` in its metadata must get samples
        # on the same topic (and no other interval's emissions).
        self._oi_subscribed_data_types: dict[tuple[InstrumentId, int], list[DataType]] = {}
        # Same pattern for forceOrder (liquidation) subscriptions: we track
        # subscriber metadata so canonical + venue-specific topics both match.
        self._liq_subscribed_data_types: dict[InstrumentId, list[DataType]] = {}

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

        # Emit one CustomData per tracked subscription so topics match
        # whatever metadata the subscriber used. Payload dispatches by
        # `DataType.type`. For canonical `Liquidation` with the bare
        # `{"instrument_id": ...}` metadata we skip the emit — the engine's
        # `_handle_liquidation` already publishes on that default custom-data
        # topic and a second emit here would deliver the event twice.
        default_metadata = {"instrument_id": instrument_id}
        subscribed_types = self._liq_subscribed_data_types.get(instrument_id) or []
        emitted_any = False
        for dt in subscribed_types:
            if dt.type is Liquidation and dt.metadata == default_metadata:
                # Engine covers this exact topic — skip to avoid duplicate.
                emitted_any = True
                continue
            emitted_any = True
            if dt.type is BinanceLiquidation:
                self._handle_data(CustomData(data_type=dt, data=binance_liq))
            elif dt.type is Liquidation:
                self._handle_data(CustomData(data_type=dt, data=liq))
            else:  # pragma: no cover (defensive)
                self._log.warning(
                    f"Unexpected liquidation subscription data_type {dt}; skipping emit",
                )

        if not emitted_any:
            # No subscription recorded yet — fall back to the default
            # venue-specific custom-data topic so the feed is still observable.
            self._handle_data(
                CustomData(
                    data_type=DataType(
                        BinanceLiquidation,
                        metadata=default_metadata,
                    ),
                    data=binance_liq,
                ),
            )

        # Always route the canonical object through the DataEngine so the
        # `data.liquidations.{venue}.{symbol}` topic and the default canonical
        # custom-data topic (`data.Liquidation.instrument_id=...`) also fire.
        self._handle_data(liq)

    # -- LIQUIDATION SUBSCRIPTION TRACKING ------------------------------------------------------

    def _register_liquidation_subscription(
        self,
        instrument_id: InstrumentId,
        data_type: DataType,
    ) -> None:
        subscribed = self._liq_subscribed_data_types.setdefault(instrument_id, [])
        if data_type not in subscribed:
            subscribed.append(data_type)

    def _deregister_liquidation_subscription(
        self,
        instrument_id: InstrumentId,
        data_type: DataType | None,
    ) -> bool:
        """Returns True if any subscription remains, False if all are gone."""
        if data_type is None:
            self._liq_subscribed_data_types.pop(instrument_id, None)
            return False
        subscribed = self._liq_subscribed_data_types.get(instrument_id)
        if subscribed is None:
            return False
        if data_type in subscribed:
            subscribed.remove(data_type)
        if not subscribed:
            self._liq_subscribed_data_types.pop(instrument_id, None)
            return False
        return True

    # -- OPEN INTEREST POLLING ------------------------------------------------------------------

    def _resolve_oi_interval_secs(self, interval_secs: int | None) -> int:
        secs = interval_secs if interval_secs is not None else self._oi_poll_default_secs
        if secs < 5:
            secs = 5  # Do not poll REST faster than 5s by default
        return secs

    def _start_open_interest_polling(
        self,
        instrument_id: InstrumentId,
        interval_secs: int | None = None,
        data_type: DataType | None = None,
    ) -> None:
        secs = self._resolve_oi_interval_secs(interval_secs)
        key = (instrument_id, secs)

        # Record every subscribed DataType for THIS (instrument, interval)
        # bucket so each interval's poll loop only emits on the subscribers
        # that actually asked for that cadence.
        if data_type is not None:
            subscribed = self._oi_subscribed_data_types.setdefault(key, [])
            if data_type not in subscribed:
                subscribed.append(data_type)

        # Evict the bucket's existing task if it's finished OR cancel has
        # already been requested on it (disconnect / stop / shutdown call
        # `cancel()` but the task may not yet have reached `done()`).
        # Short-circuiting on a cancelled-but-not-yet-done task would let
        # the pending cancellation race the resubscribe — the subsequent
        # `_clear` done-callback would pop the entry and leave the bucket
        # with no active poller.
        existing = self._oi_poll_tasks.get(key)
        if existing is not None:
            # `Task.cancelling()` is available since Python 3.11; we target
            # 3.12+. Non-zero means cancel has been requested (and the task
            # is either about to exit or already cancelled).
            has_cancel_pending = bool(existing.cancelling()) if hasattr(
                existing, "cancelling"
            ) else False
            if existing.done() or has_cancel_pending:
                self._oi_poll_tasks.pop(key, None)
                if not existing.done():
                    existing.cancel()
            else:
                return

        # Register via the LiveDataClient task registry so disconnect/shutdown
        # cancels the loop together with the rest of the client's pending tasks.
        task = self.create_task(
            self._open_interest_poll_loop(instrument_id, secs),
            log_msg=f"oi-poll-{instrument_id}-{secs}s",
        )
        self._oi_poll_tasks[key] = task

        # Drop the task from the dict when it finishes (cancelled, errored, or
        # otherwise) so later calls can restart it without tripping the
        # short-circuit above.
        def _clear(
            finished_task: asyncio.Task,
            _key: tuple[InstrumentId, int] = key,
        ) -> None:
            if self._oi_poll_tasks.get(_key) is finished_task:
                self._oi_poll_tasks.pop(_key, None)

        task.add_done_callback(_clear)

    def _stop_open_interest_polling(
        self,
        instrument_id: InstrumentId,
        data_type: DataType | None = None,
        interval_secs: int | None = None,
    ) -> None:
        # Derive the (instrument_id, interval) bucket this subscription lives
        # in. If an explicit interval wasn't provided, recover it from the
        # data_type metadata (or fall back to default). When neither is
        # available (e.g. disconnect-all), tear down every bucket.
        if data_type is not None and interval_secs is None:
            interval_secs = data_type.metadata.get("interval_secs")

        if data_type is None and interval_secs is None:
            self._teardown_all_oi_buckets(instrument_id)
            return

        secs = self._resolve_oi_interval_secs(interval_secs)
        key = (instrument_id, secs)

        # Only tear down this bucket when its LAST subscriber is gone.
        if data_type is not None:
            subscribed = self._oi_subscribed_data_types.get(key)
            if subscribed is not None and data_type in subscribed:
                subscribed.remove(data_type)
                if subscribed:
                    return
                self._oi_subscribed_data_types.pop(key, None)
        else:
            self._oi_subscribed_data_types.pop(key, None)

        task = self._oi_poll_tasks.pop(key, None)
        if task is not None and not task.done():
            task.cancel()

    def _teardown_all_oi_buckets(self, instrument_id: InstrumentId) -> None:
        for key in list(self._oi_subscribed_data_types.keys()):
            if key[0] == instrument_id:
                self._oi_subscribed_data_types.pop(key, None)
        for key in list(self._oi_poll_tasks.keys()):
            if key[0] != instrument_id:
                continue
            task = self._oi_poll_tasks.pop(key, None)
            if task is not None and not task.done():
                task.cancel()

    async def _wait_for_instrument_in_cache(
        self,
        instrument_id: InstrumentId,
        poll_interval_secs: float = 0.5,
        max_wait_secs: float = 30.0,
    ):
        """
        Wait for the instrument to land in the cache before the poll loop
        issues any REST request.

        Subscribing before `_connect()` has finished loading instruments is
        a real race: the subscribe command can route to the adapter
        synchronously, but instruments are populated asynchronously as the
        HTTP request completes. Exiting on the first miss leaves the
        bucket's subscription bookkeeping in place with no task polling
        it, so the feed stays silent until an explicit unsub/resub cycle.

        Returns the instrument once available, or `None` if the task was
        cancelled (disconnect/shutdown) before it showed up. Gives up and
        returns `None` after `max_wait_secs` with a warning — that keeps
        the loop from leaking forever if the instrument id is genuinely
        invalid.
        """
        instrument = self._cache.instrument(instrument_id)
        if instrument is not None:
            return instrument

        waited = 0.0
        while waited < max_wait_secs:
            await asyncio.sleep(poll_interval_secs)
            waited += poll_interval_secs
            instrument = self._cache.instrument(instrument_id)
            if instrument is not None:
                return instrument

        self._log.warning(
            f"OI polling gave up waiting for instrument {instrument_id} "
            f"to appear in cache after {max_wait_secs:.1f}s",
        )
        return None

    async def _open_interest_poll_loop(
        self,
        instrument_id: InstrumentId,
        interval_secs: int,
    ) -> None:
        # Subscriptions can be issued before `_connect()` finishes populating
        # the instrument cache. Spin (with backoff) until the instrument
        # shows up or the task is cancelled — exiting immediately would
        # strand the subscription's bookkeeping with no active poller and no
        # automatic retry.
        instrument = await self._wait_for_instrument_in_cache(instrument_id)
        if instrument is None:
            # Cancelled while waiting (disconnect/shutdown).
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
                # Emit one CustomData per tracked subscription in THIS bucket
                # so the topic matches whatever metadata the subscriber used
                # (with or without `interval_secs`). Payload dispatches by
                # `DataType.type`. Skip the canonical default — engine's
                # `_handle_open_interest` already publishes on that topic.
                default_metadata = {"instrument_id": instrument_id}
                key = (instrument_id, interval_secs)
                subscribed_types = self._oi_subscribed_data_types.get(key) or []
                emitted_any = False
                for dt in subscribed_types:
                    if dt.type is OpenInterest and dt.metadata == default_metadata:
                        emitted_any = True
                        continue
                    emitted_any = True
                    if dt.type is BinanceOpenInterest:
                        self._handle_data(CustomData(data_type=dt, data=binance_oi))
                    elif dt.type is OpenInterest:
                        self._handle_data(CustomData(data_type=dt, data=oi))
                    else:  # pragma: no cover (defensive)
                        self._log.warning(
                            f"Unexpected OI subscription data_type {dt}; skipping emit",
                        )

                if not emitted_any:
                    # Default topics when no subscription has been recorded yet
                    # (covers the non-dispatch emit path, e.g. tests).
                    self._handle_data(
                        CustomData(
                            data_type=DataType(
                                BinanceOpenInterest,
                                metadata=default_metadata,
                            ),
                            data=binance_oi,
                        ),
                    )

                # Always route the canonical object through the DataEngine so
                # the canonical `data.open_interest.{venue}.{symbol}` topic
                # and the canonical default custom-data topic both fire.
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
