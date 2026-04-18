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

from decimal import Decimal

import msgspec

from nautilus_trader.adapters.binance.common.enums import BinanceOrderStatus
from nautilus_trader.adapters.binance.common.enums import BinanceOrderType
from nautilus_trader.adapters.binance.common.enums import BinanceTimeInForce
from nautilus_trader.adapters.binance.common.schemas.market import BinanceExchangeFilter
from nautilus_trader.adapters.binance.common.schemas.market import BinanceRateLimit
from nautilus_trader.adapters.binance.common.schemas.market import BinanceSymbolFilter
from nautilus_trader.adapters.binance.futures.enums import BinanceFuturesContractStatus
from nautilus_trader.adapters.binance.futures.types import BinanceFuturesMarkPriceUpdate
from nautilus_trader.adapters.binance.futures.types import BinanceLiquidation
from nautilus_trader.adapters.binance.futures.types import BinanceOpenInterest
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.model.data import Liquidation
from nautilus_trader.model.data import OpenInterest
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import CurrencyType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


################################################################################
# HTTP responses
################################################################################


class BinanceFuturesAsset(msgspec.Struct, frozen=True):
    """
    HTTP response 'inner struct' from Binance Futures GET /fapi/v1/exchangeInfo.
    """

    asset: str
    marginAvailable: bool
    autoAssetExchange: str


class BinanceFuturesSymbolInfo(msgspec.Struct, kw_only=True, frozen=True):
    """
    HTTP response 'inner struct' from Binance Futures GET /fapi/v1/exchangeInfo.
    """

    symbol: str
    pair: str
    contractType: str  # Can be '' empty string
    deliveryDate: int
    onboardDate: int
    status: BinanceFuturesContractStatus | None = None
    maintMarginPercent: str
    requiredMarginPercent: str
    baseAsset: str
    quoteAsset: str
    marginAsset: str
    pricePrecision: int
    quantityPrecision: int
    baseAssetPrecision: int
    quotePrecision: int
    underlyingType: str
    underlyingSubType: list[str]
    settlePlan: int | None = None
    triggerProtect: str
    liquidationFee: str
    marketTakeBound: str
    filters: list[BinanceSymbolFilter]
    orderTypes: list[BinanceOrderType]
    timeInForce: list[BinanceTimeInForce]

    def parse_to_base_currency(self):
        return Currency(
            code=self.baseAsset,
            precision=self.baseAssetPrecision,
            iso4217=0,  # Currently unspecified for crypto assets
            name=self.baseAsset,
            currency_type=CurrencyType.CRYPTO,
        )

    def parse_to_quote_currency(self):
        return Currency(
            code=self.quoteAsset,
            precision=self.quotePrecision,
            iso4217=0,  # Currently unspecified for crypto assets
            name=self.quoteAsset,
            currency_type=CurrencyType.CRYPTO,
        )


class BinanceFuturesExchangeInfo(msgspec.Struct, kw_only=True, frozen=True):
    """
    HTTP response from Binance Futures GET /fapi/v1/exchangeInfo.
    """

    timezone: str
    serverTime: int
    rateLimits: list[BinanceRateLimit]
    exchangeFilters: list[BinanceExchangeFilter]
    assets: list[BinanceFuturesAsset] | None = None
    symbols: list[BinanceFuturesSymbolInfo]


class BinanceFuturesMarkFunding(msgspec.Struct, frozen=True):
    """
    HTTP response from Binance Futures GET /fapi/v1/premiumIndex.
    """

    symbol: str
    markPrice: str  # Mark price
    indexPrice: str  # Index price
    estimatedSettlePrice: (
        str  # Estimated Settle Price (only useful in the last hour before the settlement starts)
    )
    lastFundingRate: str  # This is the lasted funding rate
    nextFundingTime: int
    interestRate: str
    time: int


class BinanceFuturesFundRate(msgspec.Struct, frozen=True):
    """
    HTTP response from Binance Futures GET /fapi/v1/fundingRate.
    """

    symbol: str
    fundingRate: str
    fundingTime: str


################################################################################
# WebSocket messages
################################################################################


class BinanceFuturesTradeData(msgspec.Struct, frozen=True):
    """
    WebSocket message 'inner struct' for Binance Futures Trade Streams.

    Fields
    ------
    - e: Event type
    - E: Event time
    - s: Symbol
    - t: Trade ID
    - p: Price
    - q: Quantity
    - b: Buyer order ID
    - a: Seller order ID
    - T: Trade time
    - m: Is the buyer the market maker?

    """

    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    t: int  # Trade ID
    p: str  # Price
    q: str  # Quantity
    T: int  # Trade time
    m: bool  # Is the buyer the market maker?

    def parse_to_trade_tick(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
    ) -> TradeTick:
        """
        Parameters
        ----------
        instrument_id : InstrumentId
            The trade instrument ID.
        ts_init : uint64_t
            UNIX timestamp (nanoseconds) when the data object was initialized.

        Raises
        ------
        ValueError
            If trade tick data are incorrect
        """
        return TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str(self.p),
            size=Quantity.from_str(self.q),
            aggressor_side=AggressorSide.SELLER if self.m else AggressorSide.BUYER,
            trade_id=TradeId(str(self.t)),
            ts_event=millis_to_nanos(self.T),
            ts_init=ts_init,
        )


class BinanceFuturesTradeMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message from Binance Futures Trade Streams.
    """

    stream: str
    data: BinanceFuturesTradeData


class BinanceFuturesMarkPriceData(msgspec.Struct, frozen=True):
    """
    WebSocket message 'inner struct' for Binance Futures Mark Price Update events.
    """

    e: str  # Event type
    E: int  # Event time
    s: str  # Symbol
    p: str  # Mark price
    i: str  # Index price
    P: str  # Estimated Settle Price, only useful in the last hour before the settlement starts
    r: str  # Funding rate
    T: int  # Next funding time

    def parse_to_binance_futures_mark_price_update(
        self,
        instrument_id: InstrumentId,
        ts_init: int,
    ) -> BinanceFuturesMarkPriceUpdate:
        return BinanceFuturesMarkPriceUpdate(
            instrument_id=instrument_id,
            mark=Price.from_str(self.p),
            index=Price.from_str(self.i),
            estimated_settle=Price.from_str(self.P),
            funding_rate=Decimal(self.r),
            next_funding_ns=millis_to_nanos(self.T),
            ts_event=millis_to_nanos(self.E),
            ts_init=ts_init,
        )


class BinanceFuturesMarkPriceMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message from Binance Futures Mark Price Update events.
    """

    stream: str
    data: BinanceFuturesMarkPriceData


class BinanceFuturesMarkPriceAllMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message from Binance Futures All Mark Price Update events.
    """

    stream: str
    data: list[BinanceFuturesMarkPriceData]


_BINANCE_ORDER_STATUS_TO_NAUTILUS: dict[BinanceOrderStatus, OrderStatus] = {
    BinanceOrderStatus.NEW: OrderStatus.ACCEPTED,
    BinanceOrderStatus.PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
    BinanceOrderStatus.FILLED: OrderStatus.FILLED,
    BinanceOrderStatus.CANCELED: OrderStatus.CANCELED,
    BinanceOrderStatus.PENDING_CANCEL: OrderStatus.PENDING_CANCEL,
    BinanceOrderStatus.REJECTED: OrderStatus.REJECTED,
    BinanceOrderStatus.EXPIRED: OrderStatus.EXPIRED,
    BinanceOrderStatus.EXPIRED_IN_MATCH: OrderStatus.CANCELED,
    BinanceOrderStatus.NEW_ADL: OrderStatus.FILLED,
    BinanceOrderStatus.NEW_INSURANCE: OrderStatus.FILLED,
}


def _map_order_status(status: BinanceOrderStatus) -> OrderStatus:
    return _BINANCE_ORDER_STATUS_TO_NAUTILUS.get(status, OrderStatus.ACCEPTED)


class BinanceFuturesForceOrderInner(msgspec.Struct, frozen=True):
    """
    Inner force-order payload nested under `o` in the forceOrder stream.
    """

    s: str  # Symbol
    S: str  # Side (BUY / SELL)
    o: str  # Order type (e.g. LIMIT)
    f: str  # Time in force (e.g. IOC)
    q: str  # Original quantity
    p: str  # Price
    ap: str  # Average price
    X: BinanceOrderStatus  # Order status
    l: str  # Last filled quantity
    z: str  # Accumulated filled quantity
    T: int  # Trade time (ms)


class BinanceFuturesForceOrderData(msgspec.Struct, frozen=True):
    """
    Top-level data struct inside a Binance Futures forceOrder stream message.
    """

    e: str  # Event type (`forceOrder`)
    E: int  # Event time (ms)
    o: BinanceFuturesForceOrderInner

    def parse_to_liquidation(
        self,
        instrument_id: InstrumentId,
        price_precision: int,
        size_precision: int,
        ts_init: int,
    ) -> Liquidation:
        return Liquidation(
            instrument_id=instrument_id,
            side=OrderSide.BUY if self.o.S == "BUY" else OrderSide.SELL,
            quantity=Quantity(float(self.o.q), size_precision),
            price=Price(float(self.o.p), price_precision),
            order_status=_map_order_status(self.o.X),
            ts_event=millis_to_nanos(self.E),
            ts_init=ts_init,
        )

    def parse_to_binance_liquidation(
        self,
        instrument_id: InstrumentId,
        price_precision: int,
        size_precision: int,
        ts_init: int,
    ) -> BinanceLiquidation:
        return BinanceLiquidation(
            instrument_id=instrument_id,
            side=OrderSide.BUY if self.o.S == "BUY" else OrderSide.SELL,
            quantity=Quantity(float(self.o.q), size_precision),
            price=Price(float(self.o.p), price_precision),
            avg_price=Price(float(self.o.ap), price_precision),
            order_status=_map_order_status(self.o.X),
            order_type=self.o.o,
            time_in_force=self.o.f,
            last_filled_qty=Quantity(float(self.o.l), size_precision),
            accumulated_qty=Quantity(float(self.o.z), size_precision),
            trade_time_ms=self.o.T,
            ts_event=millis_to_nanos(self.E),
            ts_init=ts_init,
        )


class BinanceFuturesForceOrderMsg(msgspec.Struct, frozen=True):
    """
    WebSocket message for a Binance Futures per-symbol `forceOrder` event.
    """

    stream: str
    data: BinanceFuturesForceOrderData


class BinanceFuturesOpenInterestResponse(msgspec.Struct, frozen=True):
    """
    REST response shape from `GET /fapi/v1/openInterest`.
    """

    symbol: str
    openInterest: str
    time: int

    def parse_to_open_interest(
        self,
        instrument_id: InstrumentId,
        size_precision: int,
        ts_init: int,
    ) -> OpenInterest:
        return OpenInterest(
            instrument_id=instrument_id,
            value=Quantity(float(self.openInterest), size_precision),
            ts_event=millis_to_nanos(self.time),
            ts_init=ts_init,
        )

    def parse_to_binance_open_interest(
        self,
        instrument_id: InstrumentId,
        size_precision: int,
        poll_interval_secs: int,
        ts_init: int,
    ) -> BinanceOpenInterest:
        return BinanceOpenInterest(
            instrument_id=instrument_id,
            value=Quantity(float(self.openInterest), size_precision),
            poll_interval_secs=poll_interval_secs,
            ts_event=millis_to_nanos(self.time),
            ts_init=ts_init,
        )
