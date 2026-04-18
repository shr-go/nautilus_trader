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

from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.data import Liquidation
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider


BTCUSDT_BINANCE = TestInstrumentProvider.btcusdt_binance()


class TestLiquidation:
    def test_hash_str_and_repr(self):
        liquidation = Liquidation(
            instrument_id=BTCUSDT_BINANCE.id,
            side=OrderSide.SELL,
            quantity=Quantity.from_str("0.500"),
            price=Price.from_str("50000.10"),
            order_status=OrderStatus.FILLED,
            ts_event=1,
            ts_init=2,
        )

        assert isinstance(hash(liquidation), int)
        assert str(liquidation) == "BTCUSDT.BINANCE,SELL,0.500,50000.10,FILLED,1,2"
        assert (
            repr(liquidation)
            == "Liquidation(BTCUSDT.BINANCE,SELL,0.500,50000.10,FILLED,1,2)"
        )

    def test_hash_is_stable(self):
        a = Liquidation(
            instrument_id=BTCUSDT_BINANCE.id,
            side=OrderSide.SELL,
            quantity=Quantity.from_str("0.500"),
            price=Price.from_str("50000.10"),
            order_status=OrderStatus.FILLED,
            ts_event=1,
            ts_init=2,
        )
        b = Liquidation(
            instrument_id=BTCUSDT_BINANCE.id,
            side=OrderSide.SELL,
            quantity=Quantity.from_str("0.500"),
            price=Price.from_str("50000.10"),
            order_status=OrderStatus.FILLED,
            ts_event=1,
            ts_init=2,
        )

        assert hash(a) == hash(b)
        assert a == b

    def test_to_dict_returns_expected(self):
        liquidation = Liquidation(
            instrument_id=BTCUSDT_BINANCE.id,
            side=OrderSide.BUY,
            quantity=Quantity.from_str("1.250"),
            price=Price.from_str("49950.00"),
            order_status=OrderStatus.PARTIALLY_FILLED,
            ts_event=10,
            ts_init=20,
        )

        assert Liquidation.to_dict(liquidation) == {
            "type": "Liquidation",
            "instrument_id": "BTCUSDT.BINANCE",
            "side": "BUY",
            "quantity": "1.250",
            "price": "49950.00",
            "order_status": "PARTIALLY_FILLED",
            "ts_event": 10,
            "ts_init": 20,
        }

    def test_from_dict_roundtrip(self):
        liquidation = Liquidation(
            instrument_id=BTCUSDT_BINANCE.id,
            side=OrderSide.SELL,
            quantity=Quantity.from_str("0.500"),
            price=Price.from_str("50000.10"),
            order_status=OrderStatus.FILLED,
            ts_event=1,
            ts_init=2,
        )
        result = Liquidation.from_dict(Liquidation.to_dict(liquidation))
        assert result == liquidation

    def test_to_pyo3_then_from_pyo3(self):
        liquidation = Liquidation(
            instrument_id=BTCUSDT_BINANCE.id,
            side=OrderSide.SELL,
            quantity=Quantity.from_str("0.500"),
            price=Price.from_str("50000.10"),
            order_status=OrderStatus.FILLED,
            ts_event=1,
            ts_init=2,
        )
        pyo3_obj = liquidation.to_pyo3()
        assert isinstance(pyo3_obj, nautilus_pyo3.Liquidation)

        result = Liquidation.from_pyo3(pyo3_obj)
        assert result == liquidation
