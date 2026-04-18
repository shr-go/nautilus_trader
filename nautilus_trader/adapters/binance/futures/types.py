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

from __future__ import annotations

from decimal import Decimal
from typing import Any

from nautilus_trader.core.data import Data
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import order_side_from_str
from nautilus_trader.model.enums import order_side_to_str
from nautilus_trader.model.enums import order_status_from_str
from nautilus_trader.model.enums import order_status_to_str
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class BinanceFuturesMarkPriceUpdate(Data):
    """
    Represents a Binance Futures mark price and funding rate update.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the update.
    mark : Price
        The mark price for the instrument.
    index : Price
        The index price for the instrument.
    estimated_settle : Price
        The estimated settle price for the instrument
        (only useful in the last hour before the settlement starts).
    funding_rate : Decimal
        The current funding rate for the instrument.
    next_funding_ns : uint64_t
        UNIX timestamp (nanoseconds) when next funding will occur.
    ts_event : uint64_t
        UNIX timestamp (nanoseconds) when the data event occurred.
    ts_init : uint64_t
        UNIX timestamp (nanoseconds) when the data object was initialized.

    References
    ----------
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Mark-Price-Stream

    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        mark: Price,
        index: Price,
        estimated_settle: Price,
        funding_rate: Decimal,
        next_funding_ns: int,
        ts_event: int,
        ts_init: int,
    ):
        self.instrument_id = instrument_id
        self.mark = mark
        self.index = index
        self.estimated_settle = estimated_settle
        self.funding_rate = funding_rate
        self.next_funding_ns = next_funding_ns
        self._ts_event = ts_event
        self._ts_init = ts_init

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"instrument_id={self.instrument_id}, "
            f"mark={self.mark}, "
            f"index={self.index}, "
            f"estimated_settle={self.estimated_settle}, "
            f"funding_rate={self.funding_rate}, "
            f"next_funding_ns={self.next_funding_ns}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )

    @property
    def ts_event(self) -> int:
        """
        UNIX timestamp (nanoseconds) when the data event occurred.

        Returns
        -------
        int

        """
        return self._ts_event

    @property
    def ts_init(self) -> int:
        """
        UNIX timestamp (nanoseconds) when the object was initialized.

        Returns
        -------
        int

        """
        return self._ts_init

    @staticmethod
    def from_dict(values: dict[str, Any]) -> BinanceFuturesMarkPriceUpdate:
        """
        Return a Binance Futures mark price update parsed from the given values.

        Parameters
        ----------
        values : dict[str, Any]
            The values for initialization.

        Returns
        -------
        BinanceFuturesMarkPriceUpdate

        """
        return BinanceFuturesMarkPriceUpdate(
            instrument_id=InstrumentId.from_str(values["instrument_id"]),
            mark=Price.from_str(values["mark"]),
            index=Price.from_str(values["index"]),
            estimated_settle=Price.from_str(values["estimated_settle"]),
            funding_rate=Decimal(values["funding_rate"]),
            next_funding_ns=values["next_funding_ns"],
            ts_event=values["ts_event"],
            ts_init=values["ts_init"],
        )

    @staticmethod
    def to_dict(obj: BinanceFuturesMarkPriceUpdate) -> dict[str, Any]:
        """
        Return a dictionary representation of this object.

        Returns
        -------
        dict[str, Any]

        """
        return {
            "type": type(obj).__name__,
            "instrument_id": str(obj.instrument_id),
            "mark": str(obj.mark),
            "index": str(obj.index),
            "estimated_settle": str(obj.estimated_settle),
            "funding_rate": str(obj.funding_rate),
            "next_funding_ns": obj.next_funding_ns,
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }


class BinanceLiquidation(Data):
    """
    Represents a Binance Futures forced-liquidation (forceOrder) event.

    Preserves venue-specific fields (order type, time-in-force, accumulated
    fill quantity, last-filled quantity, and trade time) that are dropped
    by the Nautilus canonical `Liquidation`.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the liquidation order.
    side : OrderSide
        The side of the liquidation order.
    quantity : Quantity
        The original order quantity.
    price : Price
        The order price.
    avg_price : Price
        The average fill price reported by Binance.
    order_status : OrderStatus
        The order status at the time of the event.
    order_type : str
        The Binance-specific order type string (e.g. "LIMIT").
    time_in_force : str
        The Binance-specific time-in-force string (e.g. "IOC").
    last_filled_qty : Quantity
        The last filled quantity.
    accumulated_qty : Quantity
        The accumulated filled quantity across the order's lifetime.
    trade_time_ms : int
        Venue trade time in milliseconds.
    ts_event : uint64_t
        UNIX timestamp (nanoseconds) when the data event occurred.
    ts_init : uint64_t
        UNIX timestamp (nanoseconds) when the data object was initialized.

    References
    ----------
    https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams

    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
        price: Price,
        avg_price: Price,
        order_status: OrderStatus,
        order_type: str,
        time_in_force: str,
        last_filled_qty: Quantity,
        accumulated_qty: Quantity,
        trade_time_ms: int,
        ts_event: int,
        ts_init: int,
    ):
        self.instrument_id = instrument_id
        self.side = side
        self.quantity = quantity
        self.price = price
        self.avg_price = avg_price
        self.order_status = order_status
        self.order_type = order_type
        self.time_in_force = time_in_force
        self.last_filled_qty = last_filled_qty
        self.accumulated_qty = accumulated_qty
        self.trade_time_ms = trade_time_ms
        self._ts_event = ts_event
        self._ts_init = ts_init

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"instrument_id={self.instrument_id}, "
            f"side={order_side_to_str(self.side)}, "
            f"quantity={self.quantity}, "
            f"price={self.price}, "
            f"avg_price={self.avg_price}, "
            f"order_status={order_status_to_str(self.order_status)}, "
            f"order_type={self.order_type}, "
            f"time_in_force={self.time_in_force}, "
            f"last_filled_qty={self.last_filled_qty}, "
            f"accumulated_qty={self.accumulated_qty}, "
            f"trade_time_ms={self.trade_time_ms}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init

    @staticmethod
    def from_dict(values: dict[str, Any]) -> BinanceLiquidation:
        return BinanceLiquidation(
            instrument_id=InstrumentId.from_str(values["instrument_id"]),
            side=order_side_from_str(values["side"]),
            quantity=Quantity.from_str(values["quantity"]),
            price=Price.from_str(values["price"]),
            avg_price=Price.from_str(values["avg_price"]),
            order_status=order_status_from_str(values["order_status"]),
            order_type=values["order_type"],
            time_in_force=values["time_in_force"],
            last_filled_qty=Quantity.from_str(values["last_filled_qty"]),
            accumulated_qty=Quantity.from_str(values["accumulated_qty"]),
            trade_time_ms=values["trade_time_ms"],
            ts_event=values["ts_event"],
            ts_init=values["ts_init"],
        )

    @staticmethod
    def to_dict(obj: BinanceLiquidation) -> dict[str, Any]:
        return {
            "type": type(obj).__name__,
            "instrument_id": str(obj.instrument_id),
            "side": order_side_to_str(obj.side),
            "quantity": str(obj.quantity),
            "price": str(obj.price),
            "avg_price": str(obj.avg_price),
            "order_status": order_status_to_str(obj.order_status),
            "order_type": obj.order_type,
            "time_in_force": obj.time_in_force,
            "last_filled_qty": str(obj.last_filled_qty),
            "accumulated_qty": str(obj.accumulated_qty),
            "trade_time_ms": obj.trade_time_ms,
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }


class BinanceOpenInterest(Data):
    """
    Represents a Binance Futures open interest sample obtained from REST polling.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the open interest sample.
    value : Quantity
        The open interest value (contract count).
    poll_interval_secs : int
        Polling interval (seconds) used by the Python adapter.
    ts_event : uint64_t
        UNIX timestamp (nanoseconds) when Binance produced the sample.
    ts_init : uint64_t
        UNIX timestamp (nanoseconds) when the data object was initialized.

    References
    ----------
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest

    """

    def __init__(
        self,
        instrument_id: InstrumentId,
        value: Quantity,
        poll_interval_secs: int,
        ts_event: int,
        ts_init: int,
    ):
        self.instrument_id = instrument_id
        self.value = value
        self.poll_interval_secs = poll_interval_secs
        self._ts_event = ts_event
        self._ts_init = ts_init

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"instrument_id={self.instrument_id}, "
            f"value={self.value}, "
            f"poll_interval_secs={self.poll_interval_secs}, "
            f"ts_event={self.ts_event}, "
            f"ts_init={self.ts_init})"
        )

    @property
    def ts_event(self) -> int:
        return self._ts_event

    @property
    def ts_init(self) -> int:
        return self._ts_init

    @staticmethod
    def from_dict(values: dict[str, Any]) -> BinanceOpenInterest:
        return BinanceOpenInterest(
            instrument_id=InstrumentId.from_str(values["instrument_id"]),
            value=Quantity.from_str(values["value"]),
            poll_interval_secs=values["poll_interval_secs"],
            ts_event=values["ts_event"],
            ts_init=values["ts_init"],
        )

    @staticmethod
    def to_dict(obj: BinanceOpenInterest) -> dict[str, Any]:
        return {
            "type": type(obj).__name__,
            "instrument_id": str(obj.instrument_id),
            "value": str(obj.value),
            "poll_interval_secs": obj.poll_interval_secs,
            "ts_event": obj.ts_event,
            "ts_init": obj.ts_init,
        }
