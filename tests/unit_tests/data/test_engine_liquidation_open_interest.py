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

"""End-to-end DataEngine dispatch tests for canonical Liquidation / OpenInterest.

These tests drive the ACTUAL shipping path: they construct a DataEngine with
a MessageBus, subscribe a handler on a topic, call
`DataEngine._handle_data(event)`, and verify the subscriber received the
event. They cover the regression the previous Codex review flagged — that
historical replay and native `subscribe_data(...)` subscribers (i.e. paths
that don't go through the adapter's tracked-subscription emit) must still
deliver canonical Liquidation / OpenInterest events.
"""

import pytest

from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import TestClock
from nautilus_trader.data.engine import DataEngine
from nautilus_trader.data.engine import DataEngineConfig
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import Liquidation
from nautilus_trader.model.data import OpenInterest
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


BTCUSDT = TestInstrumentProvider.btcusdt_binance()


@pytest.fixture
def data_engine_fixture():
    clock = TestClock()
    msgbus = MessageBus(trader_id=TestIdStubs.trader_id(), clock=clock)
    cache = TestComponentStubs.cache()
    cache.add_instrument(BTCUSDT)
    config = DataEngineConfig(debug=True)
    engine = DataEngine(msgbus=msgbus, cache=cache, clock=clock, config=config)
    return engine, msgbus, clock


def _sample_open_interest() -> OpenInterest:
    return OpenInterest(
        instrument_id=BTCUSDT.id,
        value=Quantity.from_str("12345.678"),
        ts_event=1,
        ts_init=2,
    )


def _sample_liquidation() -> Liquidation:
    return Liquidation(
        instrument_id=BTCUSDT.id,
        side=OrderSide.SELL,
        quantity=Quantity.from_str("0.500"),
        price=Price.from_str("50000.10"),
        order_status=OrderStatus.FILLED,
        ts_event=1,
        ts_init=2,
    )


def test_handle_open_interest_publishes_on_canonical_topic(data_engine_fixture):
    """A subscriber on the dedicated `data.open_interest.{venue}.{symbol}`
    topic must receive events emitted via `_handle_open_interest`."""
    engine, msgbus, _ = data_engine_fixture
    received: list = []
    topic = f"data.open_interest.{BTCUSDT.id.venue}.{BTCUSDT.id.symbol}"
    msgbus.subscribe(topic=topic, handler=received.append)

    oi = _sample_open_interest()
    engine._handle_data(oi)

    assert received == [oi]


def test_handle_open_interest_publishes_on_default_custom_data_topic(data_engine_fixture):
    """A `subscribe_data(DataType(OpenInterest, {"instrument_id": X}))` style
    subscriber registers on the default custom-data topic. The engine MUST
    publish there too so native subscribers (and historical replay) get
    events even when no adapter is emitting CustomData."""
    engine, msgbus, _ = data_engine_fixture
    received: list = []
    default_dt = DataType(OpenInterest, metadata={"instrument_id": BTCUSDT.id})
    topic = f"data.{default_dt.topic}"
    msgbus.subscribe(topic=topic, handler=received.append)

    oi = _sample_open_interest()
    engine._handle_data(oi)

    assert received == [oi], (
        "subscribe_data(DataType(OpenInterest, {'instrument_id': X})) "
        "must receive events from engine._handle_data(oi); otherwise historical "
        "replay and non-adapter emissions silently drop events."
    )


def test_handle_liquidation_publishes_on_canonical_topic(data_engine_fixture):
    engine, msgbus, _ = data_engine_fixture
    received: list = []
    topic = f"data.liquidations.{BTCUSDT.id.venue}.{BTCUSDT.id.symbol}"
    msgbus.subscribe(topic=topic, handler=received.append)

    liq = _sample_liquidation()
    engine._handle_data(liq)

    assert received == [liq]


def test_handle_liquidation_publishes_on_default_custom_data_topic(data_engine_fixture):
    engine, msgbus, _ = data_engine_fixture
    received: list = []
    default_dt = DataType(Liquidation, metadata={"instrument_id": BTCUSDT.id})
    topic = f"data.{default_dt.topic}"
    msgbus.subscribe(topic=topic, handler=received.append)

    liq = _sample_liquidation()
    engine._handle_data(liq)

    assert received == [liq]


def test_handle_open_interest_historical_routes_to_historical_topics(data_engine_fixture):
    """Historical replay (via `_handle_data(..., historical=True)`) must fire
    on the `historical.` prefix — both the canonical and default custom-data
    variants."""
    engine, msgbus, _ = data_engine_fixture
    received_canonical: list = []
    received_custom: list = []

    canonical_topic = f"historical.data.open_interest.{BTCUSDT.id.venue}.{BTCUSDT.id.symbol}"
    default_dt = DataType(OpenInterest, metadata={"instrument_id": BTCUSDT.id})
    custom_topic = f"historical.data.{default_dt.topic}"
    msgbus.subscribe(topic=canonical_topic, handler=received_canonical.append)
    msgbus.subscribe(topic=custom_topic, handler=received_custom.append)

    oi = _sample_open_interest()
    engine._handle_data(oi, historical=True)

    assert received_canonical == [oi]
    assert received_custom == [oi]


def test_handle_liquidation_historical_routes_to_historical_topics(data_engine_fixture):
    engine, msgbus, _ = data_engine_fixture
    received_canonical: list = []
    received_custom: list = []

    canonical_topic = f"historical.data.liquidations.{BTCUSDT.id.venue}.{BTCUSDT.id.symbol}"
    default_dt = DataType(Liquidation, metadata={"instrument_id": BTCUSDT.id})
    custom_topic = f"historical.data.{default_dt.topic}"
    msgbus.subscribe(topic=canonical_topic, handler=received_canonical.append)
    msgbus.subscribe(topic=custom_topic, handler=received_custom.append)

    liq = _sample_liquidation()
    engine._handle_data(liq, historical=True)

    assert received_canonical == [liq]
    assert received_custom == [liq]


def test_handle_open_interest_live_subscribers_do_not_receive_historical(data_engine_fixture):
    """Live and historical topics must be disjoint: a subscriber listening
    only on the live topic must NOT receive historical samples."""
    engine, msgbus, _ = data_engine_fixture
    live_received: list = []
    default_dt = DataType(OpenInterest, metadata={"instrument_id": BTCUSDT.id})
    live_topic = f"data.{default_dt.topic}"
    msgbus.subscribe(topic=live_topic, handler=live_received.append)

    oi = _sample_open_interest()
    engine._handle_data(oi, historical=True)

    assert live_received == []


def test_handle_open_interest_delivers_exactly_once_on_default_topic(data_engine_fixture):
    """Sanity check: one event in produces exactly one delivery on each
    subscribed topic (no accidental double-emit from the engine)."""
    engine, msgbus, _ = data_engine_fixture
    canonical_received: list = []
    custom_received: list = []
    msgbus.subscribe(
        topic=f"data.open_interest.{BTCUSDT.id.venue}.{BTCUSDT.id.symbol}",
        handler=canonical_received.append,
    )
    default_dt = DataType(OpenInterest, metadata={"instrument_id": BTCUSDT.id})
    msgbus.subscribe(topic=f"data.{default_dt.topic}", handler=custom_received.append)

    oi = _sample_open_interest()
    engine._handle_data(oi)

    assert len(canonical_received) == 1
    assert len(custom_received) == 1
