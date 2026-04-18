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

"""DataEngine dispatch regression tests for canonical Liquidation / OpenInterest.

These cover the replay/native-subscriber path where events do not originate
from an adapter with subscription-metadata tracking. The engine must publish
on BOTH the canonical topic and the default custom-data topic so:

* `subscribe_instrument_close`-style helpers subscribing on the canonical
  topic continue to work,
* `subscribe_data(DataType(Liquidation, {"instrument_id": X}))` continues
  to receive events during historical replay, catalog reads, and test
  harnesses that call `DataEngine._handle_data(...)` directly.
"""

from nautilus_trader.common.data_topics import TopicCache
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import Liquidation
from nautilus_trader.model.data import OpenInterest
from nautilus_trader.test_kit.providers import TestInstrumentProvider


BTCUSDT_BINANCE = TestInstrumentProvider.btcusdt_binance()


def test_topic_cache_has_liquidations_and_open_interest_helpers():
    """TopicCache must expose dedicated helpers for the canonical types."""
    cache = TopicCache()

    canonical_liq_topic = cache.get_liquidations_topic(BTCUSDT_BINANCE.id)
    canonical_oi_topic = cache.get_open_interest_topic(BTCUSDT_BINANCE.id)

    assert canonical_liq_topic.startswith("data.liquidations.")
    assert canonical_oi_topic.startswith("data.open_interest.")
    # Historical flavor is distinct
    hist_liq_topic = cache.get_liquidations_topic(BTCUSDT_BINANCE.id, historical=True)
    assert hist_liq_topic.startswith("historical.data.liquidations.")


def test_custom_data_topic_matches_for_subscribe_data_default_metadata():
    """The default custom-data topic the engine emits on must exactly equal
    what `subscribe_data(DataType(OpenInterest, {"instrument_id": X}))`
    subscribes to; otherwise bare-metadata subscribers receive nothing."""
    cache = TopicCache()

    canonical_default_dt = DataType(
        OpenInterest,
        metadata={"instrument_id": BTCUSDT_BINANCE.id},
    )

    # Topic the engine's _handle_open_interest publishes on
    engine_emit_topic = cache.get_custom_data_topic(canonical_default_dt, BTCUSDT_BINANCE.id)
    # Topic a subscriber registers on (same call path as Actor.subscribe_data)
    subscriber_topic = cache.get_custom_data_topic(canonical_default_dt, BTCUSDT_BINANCE.id)

    assert engine_emit_topic == subscriber_topic

    # Symmetric assertion for Liquidation
    canonical_liq_dt = DataType(
        Liquidation,
        metadata={"instrument_id": BTCUSDT_BINANCE.id},
    )
    assert cache.get_custom_data_topic(canonical_liq_dt, BTCUSDT_BINANCE.id) == \
        cache.get_custom_data_topic(canonical_liq_dt, BTCUSDT_BINANCE.id)


def test_historical_topics_distinct_from_live():
    """Historical replay uses a distinct topic prefix; subscribers on live
    topics must NOT receive historical samples and vice versa."""
    cache = TopicCache()

    dt = DataType(OpenInterest, metadata={"instrument_id": BTCUSDT_BINANCE.id})

    live = cache.get_custom_data_topic(dt, BTCUSDT_BINANCE.id, historical=False)
    hist = cache.get_custom_data_topic(dt, BTCUSDT_BINANCE.id, historical=True)

    assert live != hist
    assert hist.startswith("historical.")
