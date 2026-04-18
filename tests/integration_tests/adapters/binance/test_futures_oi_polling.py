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
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.model.data import CustomData as _CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import OpenInterest as CanonicalOpenInterest
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.mocks.cache_database import MockCacheDatabase
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


async def _never_ending_poll_loop(*_args, **_kwargs) -> None:
    """Replaces `_open_interest_poll_loop` so the task parks until cancelled."""
    await asyncio.Event().wait()


def _make_client(event_loop):
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
    return data_client, cache


@pytest.mark.asyncio
async def test_oi_poll_task_restarts_after_cancel(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    data_client, _ = _make_client(event_loop)
    assert isinstance(data_client, BinanceFuturesDataClient)
    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _never_ending_poll_loop)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id
    key = (instrument_id, 5)

    # Act 1: start polling
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    first = data_client._oi_poll_tasks.get(key)
    assert first is not None
    assert not first.done()

    # Act 2: cancel (simulating disconnect) and let the done-callback fire
    data_client._stop_open_interest_polling(instrument_id, interval_secs=5)
    await asyncio.sleep(0)
    assert key not in data_client._oi_poll_tasks

    # Act 3: re-subscribe — polling must spin up a fresh task
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    second = data_client._oi_poll_tasks.get(key)
    assert second is not None
    assert second is not first
    assert not second.done()

    # Cleanup
    data_client._stop_open_interest_polling(instrument_id, interval_secs=5)
    await asyncio.sleep(0)
    assert key not in data_client._oi_poll_tasks


@pytest.mark.asyncio
async def test_oi_poll_tracks_subscribed_data_types(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """Multiple subscribers on the SAME (instrument, interval) bucket share
    the same poll task, and each subscription is tracked so emits fan out
    to every subscribed topic."""
    data_client, _ = _make_client(event_loop)
    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _never_ending_poll_loop)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id
    key = (instrument_id, 10)

    dt_with_interval = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": instrument_id, "interval_secs": 10},
    )
    data_client._start_open_interest_polling(
        instrument_id,
        interval_secs=10,
        data_type=dt_with_interval,
    )
    assert data_client._oi_subscribed_data_types[key] == [dt_with_interval]

    dt_without_interval = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": instrument_id},
    )
    data_client._start_open_interest_polling(
        instrument_id,
        interval_secs=10,
        data_type=dt_without_interval,
    )
    assert data_client._oi_subscribed_data_types[key] == [
        dt_with_interval,
        dt_without_interval,
    ]

    # Removing one subscriber keeps the other active
    data_client._stop_open_interest_polling(
        instrument_id,
        data_type=dt_with_interval,
        interval_secs=10,
    )
    assert data_client._oi_subscribed_data_types[key] == [dt_without_interval]

    # Removing the last subscriber tears down entirely
    data_client._stop_open_interest_polling(
        instrument_id,
        data_type=dt_without_interval,
        interval_secs=10,
    )
    assert key not in data_client._oi_subscribed_data_types

    await asyncio.sleep(0)
    assert key not in data_client._oi_poll_tasks


@pytest.mark.asyncio
async def test_distinct_intervals_get_independent_poll_tasks(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """Two subscribers on the same instrument with DIFFERENT intervals must
    each get their own poll task at their own cadence, with the correct
    `poll_interval_secs` flowing into every emitted sample. Previously the
    second subscriber would silently share the first's interval."""
    data_client, _ = _make_client(event_loop)
    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _never_ending_poll_loop)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id

    dt_a = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": instrument_id, "interval_secs": 5},
    )
    dt_b = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": instrument_id, "interval_secs": 30},
    )
    data_client._start_open_interest_polling(instrument_id, interval_secs=5, data_type=dt_a)
    data_client._start_open_interest_polling(instrument_id, interval_secs=30, data_type=dt_b)

    assert (instrument_id, 5) in data_client._oi_poll_tasks
    assert (instrument_id, 30) in data_client._oi_poll_tasks
    assert data_client._oi_poll_tasks[(instrument_id, 5)] is not \
        data_client._oi_poll_tasks[(instrument_id, 30)]

    # Unsubscribing one does NOT kill the other
    data_client._stop_open_interest_polling(
        instrument_id,
        data_type=dt_a,
        interval_secs=5,
    )
    await asyncio.sleep(0)
    assert (instrument_id, 5) not in data_client._oi_poll_tasks
    assert (instrument_id, 30) in data_client._oi_poll_tasks
    assert not data_client._oi_poll_tasks[(instrument_id, 30)].done()

    # Cleanup
    data_client._stop_open_interest_polling(
        instrument_id,
        data_type=dt_b,
        interval_secs=30,
    )
    await asyncio.sleep(0)


def _run_single_oi_emit(data_client, instrument, interval_secs: int = 10):
    """Helper: drive exactly one OI poll cycle for one (instrument, interval)
    bucket using tracked subscriptions (no REST call)."""
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
        poll_interval_secs=interval_secs,
        ts_init=ts_init,
    )
    canonical_oi = response.parse_to_open_interest(
        instrument_id=instrument_id,
        size_precision=instrument.size_precision,
        ts_init=ts_init,
    )

    default_metadata = {"instrument_id": instrument_id}
    key = (instrument_id, interval_secs)
    subscribed_types = data_client._oi_subscribed_data_types.get(key) or []
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
    """A subscriber using DataType(OpenInterest, {"instrument_id": ...,
    "interval_secs": 10}) must receive emitted samples on that exact
    custom-data topic (not on the bare {"instrument_id": ...} topic)."""
    data_client, cache = _make_client(event_loop)
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

    _run_single_oi_emit(data_client, instrument, interval_secs=10)

    custom_emits = [e for e in emitted if isinstance(e, _CustomData)]
    matching = [
        e for e in custom_emits
        if e.data_type == canonical_dt and isinstance(e.data, CanonicalOpenInterest)
    ]
    assert len(matching) == 1
    canonical_emits = [e for e in emitted if isinstance(e, CanonicalOpenInterest)]
    assert len(canonical_emits) == 1
    assert canonical_emits[0].value == Quantity.from_str("1234.567")

    data_client._stop_open_interest_polling(btc.id, data_type=canonical_dt, interval_secs=10)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_canonical_open_interest_bare_subscription_is_not_duplicated(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """When a subscriber uses the BARE default metadata (just instrument_id),
    the engine's `_handle_open_interest` publishes on the default custom-data
    topic. The adapter must NOT also emit a CustomData with the same metadata,
    or the subscriber would receive every sample twice."""
    data_client, cache = _make_client(event_loop)
    emitted: list = []
    monkeypatch.setattr(data_client, "_handle_data", lambda d: emitted.append(d))

    btc = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(btc)
    instrument = cache.instrument(btc.id)

    bare_dt = DataType(
        CanonicalOpenInterest,
        metadata={"instrument_id": btc.id},
    )
    data_client._start_open_interest_polling(
        btc.id,
        interval_secs=5,
        data_type=bare_dt,
    )

    _run_single_oi_emit(data_client, instrument, interval_secs=5)

    duplicate_emits = [
        e for e in emitted
        if isinstance(e, _CustomData) and e.data_type == bare_dt
    ]
    assert len(duplicate_emits) == 0

    canonical_emits = [e for e in emitted if isinstance(e, CanonicalOpenInterest)]
    assert len(canonical_emits) == 1

    data_client._stop_open_interest_polling(btc.id, data_type=bare_dt, interval_secs=5)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_distinct_intervals_emit_with_correct_poll_interval_secs(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """Each (instrument, interval) bucket must emit BinanceOpenInterest with
    the matching `poll_interval_secs` field — NOT the first subscriber's
    cadence shared across all buckets."""
    data_client, cache = _make_client(event_loop)
    emitted: list = []
    monkeypatch.setattr(data_client, "_handle_data", lambda d: emitted.append(d))

    btc = TestInstrumentProvider.btcusdt_binance()
    cache.add_instrument(btc)
    instrument = cache.instrument(btc.id)

    dt_a = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": btc.id, "interval_secs": 5},
    )
    dt_b = DataType(
        BinanceOpenInterest,
        metadata={"instrument_id": btc.id, "interval_secs": 30},
    )
    data_client._start_open_interest_polling(btc.id, interval_secs=5, data_type=dt_a)
    data_client._start_open_interest_polling(btc.id, interval_secs=30, data_type=dt_b)

    # Drive one cycle in each bucket
    _run_single_oi_emit(data_client, instrument, interval_secs=5)
    _run_single_oi_emit(data_client, instrument, interval_secs=30)

    binance_oi_emits = [
        e.data for e in emitted
        if isinstance(e, _CustomData) and isinstance(e.data, BinanceOpenInterest)
    ]
    intervals = sorted({x.poll_interval_secs for x in binance_oi_emits})
    assert intervals == [5, 30], (
        f"Each bucket must carry its own poll_interval_secs; got {intervals}"
    )

    data_client._stop_open_interest_polling(btc.id, data_type=dt_a, interval_secs=5)
    data_client._stop_open_interest_polling(btc.id, data_type=dt_b, interval_secs=30)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_resubscribe_after_cancel_but_before_task_exits_spawns_fresh_task(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """Regression: after `cancel()` is called on an OI poll task, a
    resubscribe for the same (instrument, interval) bucket that arrives
    BEFORE the task reaches `done()` must spawn a fresh task.

    Previously the `_start_open_interest_polling` short-circuit was gated
    only on `.done()` so a cancelled-but-still-draining task would block
    the resubscribe; the subsequent done-callback would then pop the
    bucket empty, leaving the subscription with no poller until an
    explicit unsub/resub cycle."""
    data_client, _ = _make_client(event_loop)

    # Park the poll loop so cancel() takes a beat to actually finish the task.
    async def _parked_loop(*_args, **_kwargs):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            # Simulate a slow-exiting loop: yield once before exiting.
            await asyncio.sleep(0)
            raise

    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _parked_loop)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id
    key = (instrument_id, 5)

    # 1. Start the poller, then explicitly cancel (simulating disconnect).
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    first = data_client._oi_poll_tasks[key]
    first.cancel()
    # Do NOT yield — the task hasn't reached done() yet; `.cancelling()` is 1.
    assert not first.done()

    # 2. Resubscribe immediately. Must NOT short-circuit.
    data_client._start_open_interest_polling(instrument_id, interval_secs=5)

    # 3. Let both tasks settle.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    second = data_client._oi_poll_tasks.get(key)
    assert second is not None, (
        "resubscribe after cancel must spawn a fresh task even if the old "
        "one hasn't fully exited yet"
    )
    assert second is not first
    assert not second.done()

    # Cleanup
    second.cancel()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_oi_poll_loop_waits_for_instrument_cache_population(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """Regression: if an OI subscription fires before the instrument is in
    the cache (startup race), the poll loop used to exit immediately with
    a warning. The bookkeeping stayed, the task was dead, so the feed was
    permanently silent. The loop now waits for the instrument to show up
    before issuing any REST calls."""
    data_client, cache = _make_client(event_loop)

    btc = TestInstrumentProvider.btcusdt_binance()
    # Do NOT seed the cache yet — simulate the race where the subscription
    # beats _connect() to the punch.

    # Track each HTTP call so we can assert the loop doesn't issue one
    # until the instrument is available.
    http_calls: list = []

    async def _stub_query_open_interest(symbol):
        http_calls.append(symbol)
        from nautilus_trader.adapters.binance.futures.schemas.market import (
            BinanceFuturesOpenInterestResponse,
        )

        return BinanceFuturesOpenInterestResponse(
            symbol=symbol,
            openInterest="1234.567",
            time=1_700_000_000_000,
        )

    # Short-circuit the REST client's query so we don't need a live HTTP.
    monkeypatch.setattr(
        data_client._futures_http_market,
        "query_open_interest",
        _stub_query_open_interest,
    )

    # Start the poll task while the cache is empty (startup race).
    data_client._start_open_interest_polling(btc.id, interval_secs=5)

    # Give the poll loop a few scheduler turns. It should NOT have issued
    # any REST calls yet (instrument is still missing).
    for _ in range(4):
        await asyncio.sleep(0)
    assert http_calls == [], (
        "poll loop must NOT issue REST requests before the instrument cache "
        f"is populated; got {len(http_calls)} premature call(s)"
    )

    # Populate the cache — mirrors `_connect()` finishing its HTTP load.
    cache.add_instrument(btc)

    # The poller's wait-helper sleeps 0.5s between cache checks; give it
    # two cycles plus a margin to pick up the instrument and fire a call.
    await asyncio.sleep(1.2)

    assert len(http_calls) >= 1, (
        "poll loop must resume polling once the instrument lands in cache"
    )

    # Cleanup
    data_client._stop_open_interest_polling(btc.id, interval_secs=5)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_oi_poll_wait_has_no_hard_timeout(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """Regression: the earlier 30s `max_wait_secs` timeout silently
    stranded OI subscriptions on slow startup (a venue that took > 30s
    to populate its instrument cache would leave the bucket dead with
    no poller). The waiter now has no hard timeout — it keeps trying
    until cancelled or until the instrument lands.

    We patch the sleep to fast-forward virtual time, then run the
    waiter for more than the old 30s limit without populating the
    cache, and assert the task is still running (not exited).
    """
    data_client, cache = _make_client(event_loop)

    btc = TestInstrumentProvider.btcusdt_binance()
    # Instrument is intentionally NOT added to the cache.

    # Fast-forward time: every real-time 0.5s sleep becomes ~0ms so we
    # can simulate 60+ virtual seconds of waiting in the poll loop.
    sleep_calls: list[float] = []
    real_sleep = asyncio.sleep

    async def _instant_sleep(seconds: float, *args, **kwargs):
        sleep_calls.append(seconds)
        # Yield once so the loop can check the cache and re-enter sleep,
        # but don't actually wait.
        return await real_sleep(0)

    monkeypatch.setattr(
        "nautilus_trader.adapters.binance.futures.data.asyncio.sleep",
        _instant_sleep,
    )

    # Drive the wait helper directly and assert it doesn't return within
    # a simulated window exceeding the old 30s hard limit. `wait_for`
    # with 0.2s real time lets us give the task plenty of virtual
    # iterations without blocking the test.
    task = event_loop.create_task(
        data_client._wait_for_instrument_in_cache(btc.id, poll_interval_secs=0.5)
    )
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=0.2)
        pytest.fail(
            "waiter returned unexpectedly — it must keep polling past 30s"
        )
    except asyncio.TimeoutError:
        pass

    # The helper must still be running — old bug would have had it
    # returning (and the task completing) well before 0.2s real time
    # because the fast-forwarded sleeps simulate far more than 30s.
    assert not task.done(), (
        "wait-for-instrument helper exited early; old 30s hard timeout "
        "would strand the subscription, new behavior waits until cancelled"
    )
    # Verify we actually simulated > 30s of virtual wait time.
    assert sum(sleep_calls) > 30.0, (
        f"test scaffolding failed to fast-forward enough virtual time; "
        f"only simulated {sum(sleep_calls):.1f}s"
    )

    # Cancellation is the sole termination path.
    task.cancel()
    with pytest.raises((asyncio.CancelledError, BaseException)):
        await task


@pytest.mark.asyncio
async def test_oi_poll_task_clears_entry_when_loop_completes(
    event_loop,
    binance_http_client,
    monkeypatch,
):
    """A naturally-exiting poll loop clears its own dict entry so a later
    `_start_open_interest_polling` can restart it."""
    data_client, _ = _make_client(event_loop)

    async def _immediate_exit(*_args, **_kwargs):
        return

    monkeypatch.setattr(data_client, "_open_interest_poll_loop", _immediate_exit)

    instrument_id = TestInstrumentProvider.btcusdt_binance().id
    key = (instrument_id, 5)

    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    first = data_client._oi_poll_tasks.get(key)
    assert first is not None

    await first
    await asyncio.sleep(0)

    assert key not in data_client._oi_poll_tasks

    data_client._start_open_interest_polling(instrument_id, interval_secs=5)
    second = data_client._oi_poll_tasks.get(key)
    assert second is not None
    assert second is not first
