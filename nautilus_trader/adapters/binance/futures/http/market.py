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

import msgspec

from nautilus_trader.adapters.binance.common.enums import BinanceAccountType
from nautilus_trader.adapters.binance.common.enums import BinanceSecurityType
from nautilus_trader.adapters.binance.common.symbol import BinanceSymbol
from nautilus_trader.adapters.binance.futures.schemas.market import BinanceFuturesExchangeInfo
from nautilus_trader.adapters.binance.futures.schemas.market import (
    BinanceFuturesOpenInterestResponse,
)
from nautilus_trader.adapters.binance.http.client import BinanceHttpClient
from nautilus_trader.adapters.binance.http.endpoint import BinanceHttpEndpoint
from nautilus_trader.adapters.binance.http.market import BinanceMarketHttpAPI
from nautilus_trader.core.nautilus_pyo3 import HttpMethod


class BinanceFuturesExchangeInfoHttp(BinanceHttpEndpoint):
    """
    Endpoint of FUTURES exchange trading rules and symbol information.

    `GET /fapi/v1/exchangeInfo`
    `GET /dapi/v1/exchangeInfo`

    References
    ----------
    https://binance-docs.github.io/apidocs/futures/en/#exchange-information
    https://binance-docs.github.io/apidocs/delivery/en/#exchange-information

    """

    def __init__(
        self,
        client: BinanceHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: BinanceSecurityType.NONE,
        }
        url_path = base_endpoint + "exchangeInfo"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder(BinanceFuturesExchangeInfo)

    async def get(self) -> BinanceFuturesExchangeInfo:
        method_type = HttpMethod.GET
        raw = await self._method(method_type, None)
        return self._get_resp_decoder.decode(raw)


class BinanceFuturesOpenInterestHttp(BinanceHttpEndpoint):
    """
    Endpoint for Binance Futures open interest.

    `GET /fapi/v1/openInterest`
    `GET /dapi/v1/openInterest`

    References
    ----------
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest

    """

    class GetParameters(msgspec.Struct, omit_defaults=True, frozen=True):
        symbol: BinanceSymbol

    def __init__(
        self,
        client: BinanceHttpClient,
        base_endpoint: str,
    ):
        methods = {
            HttpMethod.GET: BinanceSecurityType.NONE,
        }
        url_path = base_endpoint + "openInterest"
        super().__init__(
            client,
            methods,
            url_path,
        )
        self._get_resp_decoder = msgspec.json.Decoder(BinanceFuturesOpenInterestResponse)

    async def get(self, symbol: str) -> BinanceFuturesOpenInterestResponse:
        method_type = HttpMethod.GET
        params = self.GetParameters(symbol=BinanceSymbol(symbol))
        raw = await self._method(method_type, params)
        return self._get_resp_decoder.decode(raw)


class BinanceFuturesMarketHttpAPI(BinanceMarketHttpAPI):
    """
    Provides access to the Binance Futures HTTP REST API.

    Parameters
    ----------
    client : BinanceHttpClient
        The Binance REST API client.
    account_type : BinanceAccountType
        The Binance account type, used to select the endpoint.

    """

    def __init__(
        self,
        client: BinanceHttpClient,
        account_type: BinanceAccountType = BinanceAccountType.USDT_FUTURES,
    ):
        super().__init__(
            client=client,
            account_type=account_type,
        )

        if not account_type.is_futures:
            raise RuntimeError(  # pragma: no cover (design-time error)
                f"`BinanceAccountType` not USDT_FUTURES or COIN_FUTURES, was {account_type}",  # pragma: no cover
            )

        self._endpoint_futures_exchange_info = BinanceFuturesExchangeInfoHttp(
            client,
            self.base_endpoint,
        )
        self._endpoint_futures_open_interest = BinanceFuturesOpenInterestHttp(
            client,
            self.base_endpoint,
        )

    async def query_futures_exchange_info(self) -> BinanceFuturesExchangeInfo:
        """
        Retrieve Binance Futures exchange information.
        """
        return await self._endpoint_futures_exchange_info.get()

    async def query_open_interest(self, symbol: str) -> BinanceFuturesOpenInterestResponse:
        """
        Retrieve the current open interest for the given symbol.
        """
        return await self._endpoint_futures_open_interest.get(symbol)
