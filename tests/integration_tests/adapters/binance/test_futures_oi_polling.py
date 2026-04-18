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
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
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
