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
from nautilus_trader.model.data import OpenInterest
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider


BTCUSDT_BINANCE = TestInstrumentProvider.btcusdt_binance()


class TestOpenInterest:
    def test_hash_str_and_repr(self):
        sample = OpenInterest(
            instrument_id=BTCUSDT_BINANCE.id,
            value=Quantity.from_str("12345.678"),
            ts_event=1,
            ts_init=2,
        )

        assert isinstance(hash(sample), int)
        assert str(sample) == "BTCUSDT.BINANCE,12345.678,1,2"
        assert repr(sample) == "OpenInterest(BTCUSDT.BINANCE,12345.678,1,2)"

    def test_hash_is_stable(self):
        a = OpenInterest(
            instrument_id=BTCUSDT_BINANCE.id,
            value=Quantity.from_str("1000.000"),
            ts_event=1,
            ts_init=2,
        )
        b = OpenInterest(
            instrument_id=BTCUSDT_BINANCE.id,
            value=Quantity.from_str("1000.000"),
            ts_event=1,
            ts_init=2,
        )

        assert hash(a) == hash(b)
        assert a == b

    def test_to_dict_returns_expected(self):
        sample = OpenInterest(
            instrument_id=BTCUSDT_BINANCE.id,
            value=Quantity.from_str("12345.678"),
            ts_event=10,
            ts_init=20,
        )

        assert OpenInterest.to_dict(sample) == {
            "type": "OpenInterest",
            "instrument_id": "BTCUSDT.BINANCE",
            "value": "12345.678",
            "ts_event": 10,
            "ts_init": 20,
        }

    def test_from_dict_roundtrip(self):
        sample = OpenInterest(
            instrument_id=BTCUSDT_BINANCE.id,
            value=Quantity.from_str("12345.678"),
            ts_event=1,
            ts_init=2,
        )
        result = OpenInterest.from_dict(OpenInterest.to_dict(sample))
        assert result == sample

    def test_to_pyo3_then_from_pyo3(self):
        sample = OpenInterest(
            instrument_id=BTCUSDT_BINANCE.id,
            value=Quantity.from_str("12345.678"),
            ts_event=1,
            ts_init=2,
        )
        pyo3_obj = sample.to_pyo3()
        assert isinstance(pyo3_obj, nautilus_pyo3.OpenInterest)

        result = OpenInterest.from_pyo3(pyo3_obj)
        assert result == sample
