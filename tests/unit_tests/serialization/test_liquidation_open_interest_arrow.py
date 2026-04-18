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

"""Arrow serialization wiring for canonical `Liquidation` / `OpenInterest`."""

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.data import Liquidation
from nautilus_trader.model.data import OpenInterest
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.serialization.arrow.schema import NAUTILUS_ARROW_SCHEMA
from nautilus_trader.serialization.arrow.serializer import RUST_SERIALIZERS
from nautilus_trader.serialization.arrow.serializer import ArrowSerializer
from nautilus_trader.test_kit.providers import TestInstrumentProvider


BTCUSDT_BINANCE = TestInstrumentProvider.btcusdt_binance()


def test_liquidation_registered_for_arrow_serialization():
    # Types must appear in both the serializer registry AND the schema map,
    # otherwise ParquetDataCatalog.write_data(...) raises TypeError.
    assert Liquidation in RUST_SERIALIZERS
    assert Liquidation in NAUTILUS_ARROW_SCHEMA
    assert hasattr(nautilus_pyo3, "liquidations_to_arrow_record_batch_bytes")


def test_open_interest_registered_for_arrow_serialization():
    assert OpenInterest in RUST_SERIALIZERS
    assert OpenInterest in NAUTILUS_ARROW_SCHEMA
    assert hasattr(nautilus_pyo3, "open_interest_to_arrow_record_batch_bytes")


def test_liquidation_round_trips_through_arrow_table():
    liq = Liquidation(
        instrument_id=BTCUSDT_BINANCE.id,
        side=OrderSide.SELL,
        quantity=Quantity.from_str("0.500"),
        price=Price.from_str("50000.10"),
        order_status=OrderStatus.FILLED,
        ts_event=1,
        ts_init=2,
    )
    table = ArrowSerializer.rust_defined_to_record_batch([liq], data_cls=Liquidation)
    assert table.num_rows == 1


def test_open_interest_round_trips_through_arrow_table():
    oi = OpenInterest(
        instrument_id=BTCUSDT_BINANCE.id,
        value=Quantity.from_str("12345.678"),
        ts_event=1,
        ts_init=2,
    )
    table = ArrowSerializer.rust_defined_to_record_batch([oi], data_cls=OpenInterest)
    assert table.num_rows == 1
