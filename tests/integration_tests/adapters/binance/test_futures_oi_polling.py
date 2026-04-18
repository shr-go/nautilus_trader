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

"""Lifecycle tests for the Binance Futures open-interest REST poller."""

import asyncio

import pytest

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.config import BinanceDataClientConfig
from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.binance.futures.data import BinanceFuturesDataClient
from nautilus_trader.adapters.binance.futures.types import BinanceOpenInterest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.model.data import CustomData as _CustomData  # noqa: F401 (used indirectly)
from nautilus_trader.model.data import OpenInterest as CanonicalOpenInterest
from nautilus_trader.model.objects import Quantity
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.model.data import DataType
from nautilus_trader.test_kit.mocks.cache_database import MockCacheDatabase
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


async def _never_ending_poll_loop(*_args, **_kwargs) -> None:
    """Replaces `_open_interest_poll_loop` so the task parks until cancelled."""
    await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_oi_poll_task_restarts_after_cancel(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    # Arrange
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = Cache(database=MockCacheDatabase())

    data_client = BinanceLiveDataClientFactory.create(
        loop=event_loop,
        name="BINANCE",
        config=BinanceDataClientConfig(
            api_key="SOME_BINANCE_API_KEY",  # noqa: S106
            api_secret="SOME_BINANCE_API_SECRET",  # noqa: S106
            account_type=BinanceAccountType.USDT_FUTURES,
        ),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )
    assert isinstance(data_client, BinanceFuturesDataClient)

    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _never_ending_poll_loop)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id

    # Act 1: start polling
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    first = data_client._oi_poll_tasks.get(instrument_id)
    assert first is not None
    assert not first.done()

    # Act 2: cancel (simulating disconnect) and let the done-callback fire
    data_client._stop_open_interest_polling(instrument_id)
    await asyncio.sleep(0)  # allow done-callback to execute
    assert instrument_id not in data_client._oi_poll_tasks

    # Act 3: re-subscribe — polling must spin up a fresh task
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    second = data_client._oi_poll_tasks.get(instrument_id)
    assert second is not None
    assert second is not first
    assert not second.done()

    # Cleanup
    data_client._stop_open_interest_polling(instrument_id)
    await asyncio.sleep(0)
    assert instrument_id not in data_client._oi_poll_tasks


@pytest.mark.asyncio
async def test_oi_poll_tracks_subscribed_data_types(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """_start_open_interest_polling records every subscribed DataType so the
    emit topic matches whatever metadata the subscriber used — notably
    `interval_secs`."""
    # Arrange
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = Cache(database=MockCacheDatabase())

    data_client = BinanceLiveDataClientFactory.create(
        loop=event_loop,
        name="BINANCE",
        config=BinanceDataClientConfig(
            api_key="SOME_BINANCE_API_KEY",  # noqa: S106
            api_secret="SOME_BINANCE_API_SECRET",  # noqa: S106
            account_type=BinanceAccountType.USDT_FUTURES,
        ),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )
    assert isinstance(data_client, BinanceFuturesDataClient)

    # Use a parked poll loop so the real REST call is never made
    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _never_ending_poll_loop)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id

    # Case A: subscribe with interval_secs → tracked data type carries it
    dt_with_interval = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": instrument_id, "interval_secs": 10},
    )
    data_client._start_open_interest_polling(
        instrument_id,
        interval_secs=10,
        data_type=dt_with_interval,
    )
    tracked = data_client._oi_subscribed_data_types.get(instrument_id)
    assert tracked == [dt_with_interval]

    # Case B: a second subscriber with different metadata is also tracked
    dt_without_interval = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": instrument_id},
    )
    data_client._start_open_interest_polling(
        instrument_id,
        interval_secs=10,
        data_type=dt_without_interval,
    )
    assert data_client._oi_subscribed_data_types[instrument_id] == [
        dt_with_interval,
        dt_without_interval,
    ]

    # Removing one subscriber keeps the other active
    data_client._stop_open_interest_polling(
        instrument_id,
        data_type=dt_with_interval,
    )
    assert data_client._oi_subscribed_data_types[instrument_id] == [dt_without_interval]

    # Removing the last subscriber tears down entirely
    data_client._stop_open_interest_polling(
        instrument_id,
        data_type=dt_without_interval,
    )
    assert instrument_id not in data_client._oi_subscribed_data_types

    # Let the done-callback fire before asserting the task dict is clean
    await asyncio.sleep(0)
    assert instrument_id not in data_client._oi_poll_tasks


def _run_single_oi_emit(
    data_client,
    instrument,
):
    """Helper: drive exactly one OI poll cycle using tracked subscriptions."""
    from nautilus_trader.adapters.binance.futures.schemas.market import (
        BinanceFuturesOpenInterestResponse,
    )

    response = BinanceFuturesOpenInterestResponse(
        symbol="BTCUSDT",
        openInterest="1234.567",
        time=1_700_000_000_000,
    )
    instrument_id = instrument.id
    ts_init = 1
    binance_oi = response.parse_to_binance_open_interest(
        instrument_id=instrument_id,
        size_precision=instrument.size_precision,
        poll_interval_secs=10,
        ts_init=ts_init,
    )
    canonical_oi = response.parse_to_open_interest(
        instrument_id=instrument_id,
        size_precision=instrument.size_precision,
        ts_init=ts_init,
    )

    default_metadata = {"instrument_id": instrument_id}
    subscribed_types = data_client._oi_subscribed_data_types.get(instrument_id) or []
    for dt in subscribed_types:
        if dt.type is CanonicalOpenInterest and dt.metadata == default_metadata:
            continue  # engine covers this via _handle_open_interest's default emit
        if dt.type is BinanceOpenInterest:
            data_client._handle_data(_CustomData(data_type=dt, data=binance_oi))
        elif dt.type is CanonicalOpenInterest:
            data_client._handle_data(_CustomData(data_type=dt, data=canonical_oi))
    data_client._handle_data(canonical_oi)


@pytest.mark.asyncio
async def test_canonical_open_interest_subscription_with_interval_secs_receives_events(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """A subscriber that uses DataType(OpenInterest, {"instrument_id": ...,
    "interval_secs": 10}) must actually receive emitted samples on that exact
    custom-data topic (not on the bare {"instrument_id": ...} topic)."""
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = Cache(database=MockCacheDatabase())

    data_client = BinanceLiveDataClientFactory.create(
        loop=event_loop,
        name="BINANCE",
        config=BinanceDataClientConfig(
            api_key="SOME_BINANCE_API_KEY",  # noqa: S106
            api_secret="SOME_BINANCE_API_SECRET",  # noqa: S106
            account_type=BinanceAccountType.USDT_FUTURES,
        ),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )

    emitted: list = []
    monkeypatch.setattr(data_client, "_handle_data", lambda d: emitted.append(d))

    btc = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(btc)
    instrument = cache.instrument(btc.id)
    assert instrument is not None

    canonical_dt = DataType(
        CanonicalOpenInterest,
        metadata={"instrument_id": btc.id, "interval_secs": 10},
    )
    data_client._start_open_interest_polling(
        btc.id,
        interval_secs=10,
        data_type=canonical_dt,
    )

    _run_single_oi_emit(data_client, instrument)

    custom_emits = [e for e in emitted if isinstance(e, _CustomData)]
    matching = [
        e for e in custom_emits
        if e.data_type == canonical_dt and isinstance(e.data, CanonicalOpenInterest)
    ]
    assert len(matching) == 1, (
        f"Expected one CustomData with canonical metadata {canonical_dt}, "
        f"got: {[(e.data_type, type(e.data).__name__) for e in custom_emits]}"
    )
    canonical_emits = [e for e in emitted if isinstance(e, CanonicalOpenInterest)]
    assert len(canonical_emits) == 1
    assert canonical_emits[0].value == Quantity.from_str("1234.567")

    data_client._stop_open_interest_polling(btc.id, data_type=canonical_dt)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_canonical_open_interest_bare_subscription_is_not_duplicated(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """When a subscriber uses the BARE default metadata (just instrument_id),
    the engine's `_handle_open_interest` publishes on that default custom-data
    topic. The adapter must NOT also emit a CustomData with the same metadata
    or the subscriber would receive every sample twice."""
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = Cache(database=MockCacheDatabase())

    data_client = BinanceLiveDataClientFactory.create(
        loop=event_loop,
        name="BINANCE",
        config=BinanceDataClientConfig(
            api_key="SOME_BINANCE_API_KEY",  # noqa: S106
            api_secret="SOME_BINANCE_API_SECRET",  # noqa: S106
            account_type=BinanceAccountType.USDT_FUTURES,
        ),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )

    emitted: list = []
    monkeypatch.setattr(data_client, "_handle_data", lambda d: emitted.append(d))

    btc = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(btc)
    instrument = cache.instrument(btc.id)

    # Bare canonical subscription: metadata has only instrument_id.
    bare_dt = DataType(
        CanonicalOpenInterest,
        metadata={"instrument_id": btc.id},
    )
    data_client._start_open_interest_polling(
        btc.id,
        interval_secs=5,
        data_type=bare_dt,
    )

    _run_single_oi_emit(data_client, instrument)

    # No CustomData with the bare-canonical metadata should have been emitted —
    # the engine handles that default topic itself.
    duplicate_emits = [
        e for e in emitted
        if isinstance(e, _CustomData) and e.data_type == bare_dt
    ]
    assert len(duplicate_emits) == 0, (
        "Adapter must not emit a CustomData on the default canonical topic; "
        "the engine's _handle_open_interest already publishes there."
    )

    # The canonical OpenInterest object is still routed through the engine.
    canonical_emits = [e for e in emitted if isinstance(e, CanonicalOpenInterest)]
    assert len(canonical_emits) == 1

    data_client._stop_open_interest_polling(btc.id, data_type=bare_dt)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_oi_poll_task_clears_entry_when_loop_completes(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """If the poll loop exits on its own (e.g. cancelled via cancel_pending_tasks
    during disconnect), the dict entry must be cleared so a later
    `_start_open_interest_polling` can restart it."""
    # Arrange
    clock = LiveClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = Cache(database=MockCacheDatabase())

    data_client = BinanceLiveDataClientFactory.create(
        loop=event_loop,
        name="BINANCE",
        config=BinanceDataClientConfig(
            api_key="SOME_BINANCE_API_KEY",  # noqa: S106
            api_secret="SOME_BINANCE_API_SECRET",  # noqa: S106
            account_type=BinanceAccountType.USDT_FUTURES,
        ),
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )
    assert isinstance(data_client, BinanceFuturesDataClient)

    async def _immediate_exit(*_args, **_kwargs):
        return

    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _immediate_exit)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id

    # Act: start a poll loop that finishes right away (mimicking natural exit)
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    first = data_client._oi_poll_tasks.get(instrument_id)
    assert first is not None

    # Yield so the task completes and the done-callback runs
    await first
    await asyncio.sleep(0)

    # Assert: the stale entry is gone
    assert instrument_id not in data_client._oi_poll_tasks

    # Re-start must succeed (new task)
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    second = data_client._oi_poll_tasks.get(instrument_id)
    assert second is not None
    assert second is not first
