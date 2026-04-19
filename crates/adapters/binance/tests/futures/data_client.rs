// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Integration tests for the Binance Futures data client.

use std::{collections::HashMap, net::SocketAddr, time::Duration};

use axum::{
    Router,
    extract::ws::{Message, WebSocket},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use nautilus_binance::{
    common::enums::BinanceProductType, config::BinanceDataClientConfig,
    futures::BinanceFuturesDataClient,
};
use nautilus_common::{
    clients::DataClient,
    live::runner::set_data_event_sender,
    messages::{
        DataEvent,
        data::{
            subscribe::{
                SubscribeBookDeltas, SubscribeMarkPrices, SubscribeQuotes, SubscribeTrades,
            },
            unsubscribe::{UnsubscribeQuotes, UnsubscribeTrades},
        },
    },
    testing::wait_until_async,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
};
use nautilus_network::http::HttpClient;
use rstest::rstest;
use serde_json::json;

fn json_response(body: &serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn handle_ws(ws: axum::extract::WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_ws_connection)
}

async fn handle_ws_connection(mut socket: WebSocket) {
    while let Some(Ok(msg)) = socket.recv().await {
        if let Message::Text(text) = msg
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text)
        {
            let method = parsed.get("method").and_then(|m| m.as_str());
            let id = parsed.get("id").and_then(|v| v.as_u64()).unwrap_or(1);

            if method == Some("SUBSCRIBE") {
                let resp = json!({"result": null, "id": id});
                let _ = socket.send(Message::Text(resp.to_string().into())).await;

                if let Some(params) = parsed.get("params").and_then(|p| p.as_array()) {
                    for param in params {
                        if let Some(stream) = param.as_str() {
                            if stream.contains("@aggTrade") {
                                let trade = json!({
                                    "e": "aggTrade",
                                    "E": 1700000000000_i64,
                                    "s": "BTCUSDT",
                                    "a": 1,
                                    "p": "50000.00",
                                    "q": "0.001",
                                    "f": 1,
                                    "l": 1,
                                    "T": 1700000000000_i64,
                                    "m": false
                                });
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                let _ = socket.send(Message::Text(trade.to_string().into())).await;
                            } else if stream.contains("@bookTicker") {
                                let quote = json!({
                                    "e": "bookTicker",
                                    "u": 12345,
                                    "E": 1700000000000_i64,
                                    "T": 1700000000000_i64,
                                    "s": "BTCUSDT",
                                    "b": "50000.00",
                                    "B": "1.000",
                                    "a": "50001.00",
                                    "A": "0.500"
                                });
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                let _ = socket.send(Message::Text(quote.to_string().into())).await;
                            } else if stream.contains("@depth") {
                                let depth_update = json!({
                                    "e": "depthUpdate",
                                    "E": 1700000000000_i64,
                                    "T": 1700000000000_i64,
                                    "s": "BTCUSDT",
                                    "U": 1027024,
                                    "u": 1027025,
                                    "pu": 1027023,
                                    "b": [["50000.00", "1.000"], ["49999.00", "2.000"]],
                                    "a": [["50001.00", "0.500"], ["50002.00", "1.500"]]
                                });
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                let _ = socket
                                    .send(Message::Text(depth_update.to_string().into()))
                                    .await;
                            } else if stream.contains("@markPrice") {
                                let mark_price = json!({
                                    "e": "markPriceUpdate",
                                    "E": 1700000000000_i64,
                                    "s": "BTCUSDT",
                                    "p": "50000.50",
                                    "i": "50000.25",
                                    "P": "50000.75",
                                    "r": "0.00010000",
                                    "T": 1700028800000_i64
                                });
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                let _ = socket
                                    .send(Message::Text(mark_price.to_string().into()))
                                    .await;
                            } else if stream.contains("@forceOrder") {
                                let force_order = json!({
                                    "e": "forceOrder",
                                    "E": 1700000000000_i64,
                                    "o": {
                                        "s": "BTCUSDT",
                                        "S": "SELL",
                                        "o": "LIMIT",
                                        "f": "IOC",
                                        "q": "0.014",
                                        "p": "50000.00",
                                        "ap": "50000.12",
                                        "X": "FILLED",
                                        "l": "0.014",
                                        "z": "0.014",
                                        "T": 1700000000000_i64
                                    }
                                });
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                let _ = socket
                                    .send(Message::Text(force_order.to_string().into()))
                                    .await;
                            }
                        }
                    }
                }
            } else if method == Some("UNSUBSCRIBE") {
                let resp = json!({"result": null, "id": id});
                let _ = socket.send(Message::Text(resp.to_string().into())).await;
            }
        }
    }
}

fn create_data_test_router() -> Router {
    Router::new()
        .route("/fapi/v1/ping", get(|| async { json_response(&json!({})) }))
        .route(
            "/fapi/v1/exchangeInfo",
            get(|| async {
                json_response(&json!({
                    "timezone": "UTC",
                    "serverTime": 1700000000000_i64,
                    "rateLimits": [],
                    "exchangeFilters": [],
                    "symbols": [{
                        "symbol": "BTCUSDT",
                        "pair": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "deliveryDate": 4133404800000_i64,
                        "onboardDate": 1569398400000_i64,
                        "status": "TRADING",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "marginAsset": "USDT",
                        "pricePrecision": 2,
                        "quantityPrecision": 3,
                        "baseAssetPrecision": 8,
                        "quotePrecision": 8,
                        "maintMarginPercent": "2.5000",
                        "requiredMarginPercent": "5.0000",
                        "underlyingType": "COIN",
                        "settlePlan": 0,
                        "triggerProtect": "0.0500",
                        "filters": [
                            {"filterType": "PRICE_FILTER", "minPrice": "0.10", "maxPrice": "1000000", "tickSize": "0.10"},
                            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "5"}
                        ],
                        "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"],
                        "timeInForce": ["GTC", "IOC", "FOK", "GTD"]
                    }]
                }))
            }),
        )
        .route(
            "/fapi/v1/depth",
            get(|| async {
                json_response(&json!({
                    "lastUpdateId": 1027024,
                    "E": 1700000000000_i64,
                    "T": 1700000000000_i64,
                    "bids": [["50000.00", "1.000"], ["49999.00", "2.000"]],
                    "asks": [["50001.00", "0.500"], ["50002.00", "1.500"]]
                }))
            }),
        )
        .route(
            "/fapi/v1/openInterest",
            get(|| async {
                json_response(&json!({
                    "symbol": "BTCUSDT",
                    "openInterest": "1234.567",
                    "time": 1700000000000_i64,
                }))
            }),
        )
        .route("/ws", get(handle_ws))
}

async fn start_data_test_server() -> SocketAddr {
    let router = create_data_test_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .unwrap();
    });

    let health_url = format!("http://{addr}/fapi/v1/ping");
    let http_client =
        HttpClient::new(HashMap::new(), Vec::new(), Vec::new(), None, None, None).unwrap();
    wait_until_async(
        || {
            let url = health_url.clone();
            let client = http_client.clone();
            async move { client.get(url, None, None, Some(1), None).await.is_ok() }
        },
        Duration::from_secs(5),
    )
    .await;

    addr
}

fn create_test_data_client(
    base_url_http: String,
    base_url_ws: String,
) -> (
    BinanceFuturesDataClient,
    tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    set_data_event_sender(tx);

    let config = BinanceDataClientConfig {
        product_types: vec![BinanceProductType::UsdM],
        base_url_http: Some(base_url_http),
        base_url_ws: Some(base_url_ws),
        ..Default::default()
    };

    let client =
        BinanceFuturesDataClient::new(ClientId::from("BINANCE"), config, BinanceProductType::UsdM)
            .unwrap();

    (client, rx)
}

#[rstest]
#[tokio::test]
async fn test_client_creation() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (client, _rx) = create_test_data_client(base_url_http, base_url_ws);

    assert_eq!(client.client_id(), ClientId::from("BINANCE"));
    assert_eq!(client.venue(), Some(Venue::from("BINANCE")));
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_connect_emits_instruments() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();
    assert!(client.is_connected());

    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_disconnect_sets_state() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, _rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();
    assert!(client.is_connected());

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_subscribe_trades() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events from connect
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let cmd = SubscribeTrades::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    client.subscribe_trades(&cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_subscribe_quotes() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events from connect
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let cmd = SubscribeQuotes::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    client.subscribe_quotes(&cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_subscribe_book_deltas() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events from connect
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let cmd = SubscribeBookDeltas::new(
        instrument_id,
        BookType::L2_MBP,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        false,
        None,
        None,
    );

    client.subscribe_book_deltas(&cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_subscribe_mark_prices() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events from connect
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let cmd = SubscribeMarkPrices::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    client.subscribe_mark_prices(&cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_open_interest_poll_tasks_cleared_on_disconnect() {
    // Mechanical guard for the lifecycle fix: after `disconnect`, the
    // adapter's internal `oi_poll_tasks` dict MUST be empty, so a subsequent
    // `connect` + `subscribe_open_interest` isn't short-circuited by a stale
    // handle left over from the prior connection.
    use nautilus_common::messages::data::SubscribeCustomData;
    use nautilus_core::Params;
    use nautilus_model::data::DataType;

    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, _rx) = create_test_data_client(base_url_http, base_url_ws);
    client.connect().await.unwrap();

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        serde_json::Value::String(instrument_id.to_string()),
    );
    metadata.insert("interval_secs".to_string(), serde_json::Value::from(5u64));
    let data_type = DataType::new(stringify!(OpenInterest), Some(metadata), None);
    let cmd = SubscribeCustomData {
        data_type,
        client_id: Some(ClientId::from("BINANCE")),
        venue: Some(instrument_id.venue),
        command_id: nautilus_core::UUID4::new(),
        ts_init: UnixNanos::default(),
        correlation_id: None,
        params: None,
    };
    client.subscribe(&cmd).unwrap();

    // Precondition: exactly one task registered.
    assert_eq!(client.oi_poll_task_count(), 1);

    // Disconnect — must clear the dict (not merely cancel its token).
    client.disconnect().await.unwrap();
    assert_eq!(
        client.oi_poll_task_count(),
        0,
        "disconnect must abort + clear every OI poll task",
    );
}

#[rstest]
#[tokio::test]
async fn test_open_interest_poll_survives_disconnect_reconnect_resubscribe() {
    // Regression guard: an OI poll subscription must be cleanly torn down on
    // disconnect and re-armed on a subsequent connect + subscribe. Previously
    // the finished-but-not-yet-reaped task handle left in `oi_poll_tasks`
    // could race with a fresh resubscribe, causing
    // `start_open_interest_polling` to short-circuit and strand the actor
    // with no poller.
    use nautilus_common::messages::data::{SubscribeCustomData, UnsubscribeCustomData};
    use nautilus_core::Params;
    use nautilus_model::data::DataType;

    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let build_cmd = |interval: u64| -> SubscribeCustomData {
        let mut metadata = Params::new();
        metadata.insert(
            "instrument_id".to_string(),
            serde_json::Value::String(instrument_id.to_string()),
        );
        metadata.insert("interval_secs".to_string(), serde_json::Value::from(interval));
        let data_type = DataType::new(stringify!(OpenInterest), Some(metadata), None);
        SubscribeCustomData {
            data_type,
            client_id: Some(ClientId::from("BINANCE")),
            venue: Some(instrument_id.venue),
            command_id: nautilus_core::UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            params: None,
        }
    };

    // Subscribe once → expect at least one OI event.
    client.subscribe(&build_cmd(5)).unwrap();
    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(10),
    )
    .await;

    // Disconnect: the lifecycle hook must abort and clear the OI poll task
    // so the bucket dict is empty.
    client.disconnect().await.unwrap();
    while rx.try_recv().is_ok() {}

    // Reconnect + resubscribe: a fresh poll task must spawn even though the
    // prior handle may not have been reaped yet by tokio. If the adapter
    // silently short-circuited, no new OI event would ever arrive.
    client.connect().await.unwrap();
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    client.subscribe(&build_cmd(5)).unwrap();
    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(10),
    )
    .await;

    // Clean up.
    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        serde_json::Value::String(instrument_id.to_string()),
    );
    metadata.insert("interval_secs".to_string(), serde_json::Value::from(5u64));
    let data_type = DataType::new(stringify!(OpenInterest), Some(metadata), None);
    let _ = client.unsubscribe(&UnsubscribeCustomData {
        data_type,
        client_id: Some(ClientId::from("BINANCE")),
        venue: Some(instrument_id.venue),
        command_id: nautilus_core::UUID4::new(),
        ts_init: UnixNanos::default(),
        correlation_id: None,
        params: None,
    });
}

#[rstest]
#[tokio::test]
async fn test_subscribe_custom_data_open_interest_starts_rest_poll() {
    // Proves the Rust Binance adapter starts a REST poll task when a
    // `SubscribeCustomData` for `DataType(OpenInterest, {...})` arrives.
    // Previously this branch silently no-op'd, starving any Rust actor
    // that used `DataActor::subscribe_open_interest`.
    use nautilus_common::messages::data::SubscribeCustomData;
    use nautilus_core::Params;
    use nautilus_model::data::DataType;

    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        serde_json::Value::String(instrument_id.to_string()),
    );
    metadata.insert(
        "interval_secs".to_string(),
        serde_json::Value::from(5u64),
    );
    let data_type = DataType::new(stringify!(OpenInterest), Some(metadata), None);
    let cmd = SubscribeCustomData {
        data_type,
        client_id: Some(ClientId::from("BINANCE")),
        venue: Some(instrument_id.venue),
        command_id: nautilus_core::UUID4::new(),
        ts_init: UnixNanos::default(),
        correlation_id: None,
        params: None,
    };

    client.subscribe(&cmd).unwrap();

    // Poll task should call the mock HTTP endpoint and emit an OpenInterest
    // Data event within a few seconds.
    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(15),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_subscribe_custom_data_missing_metadata_returns_error_not_panic() {
    // Previously the adapter called `DataType::instrument_id()` which panics
    // when metadata is `None`. A user-built `DataType` without metadata must
    // produce a clean `Err`, never a thread panic.
    use nautilus_common::messages::data::SubscribeCustomData;
    use nautilus_model::data::DataType;

    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, _rx) = create_test_data_client(base_url_http, base_url_ws);
    client.connect().await.unwrap();

    let data_type = DataType::new(stringify!(Liquidation), None, None);
    let cmd = SubscribeCustomData {
        data_type,
        client_id: Some(ClientId::from("BINANCE")),
        venue: None,
        command_id: nautilus_core::UUID4::new(),
        ts_init: UnixNanos::default(),
        correlation_id: None,
        params: None,
    };

    let result = client.subscribe(&cmd);
    assert!(result.is_err(), "Liquidation subscribe without metadata must error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("missing") && err.contains("instrument_id"),
        "error should mention missing instrument_id, was: {err}",
    );
}

#[rstest]
#[tokio::test]
async fn test_subscribe_custom_data_unsupported_type_returns_error() {
    // Unknown type_names must not silently succeed — that hides the fact
    // that no stream or poll task has started.
    use nautilus_common::messages::data::SubscribeCustomData;
    use nautilus_core::Params;
    use nautilus_model::data::DataType;

    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, _rx) = create_test_data_client(base_url_http, base_url_ws);
    client.connect().await.unwrap();

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        serde_json::Value::String(instrument_id.to_string()),
    );
    let data_type = DataType::new("SomeUnknownType", Some(metadata), None);
    let cmd = SubscribeCustomData {
        data_type,
        client_id: Some(ClientId::from("BINANCE")),
        venue: Some(instrument_id.venue),
        command_id: nautilus_core::UUID4::new(),
        ts_init: UnixNanos::default(),
        correlation_id: None,
        params: None,
    };

    let result = client.subscribe(&cmd);
    assert!(result.is_err(), "unknown type_name must return Err");
    assert!(
        result.unwrap_err().to_string().contains("SomeUnknownType"),
        "error should name the unsupported data_type",
    );
}

#[rstest]
#[tokio::test]
async fn test_subscribe_custom_data_liquidation_activates_force_order_stream() {
    // Proves the Rust Binance adapter activates the `@forceOrder` WS stream
    // when a `SubscribeCustomData` for `DataType(Liquidation, {...})` arrives
    // — which is the command a Rust actor dispatches via
    // `DataActor::subscribe_liquidations`. Without this override the subscribe
    // would fall back to the default `log_not_implemented`, leaving the
    // actor's handler permanently starved.
    use nautilus_common::messages::data::SubscribeCustomData;
    use nautilus_core::Params;
    use nautilus_model::data::DataType;

    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    let mut metadata = Params::new();
    metadata.insert(
        "instrument_id".to_string(),
        serde_json::Value::String(instrument_id.to_string()),
    );
    let data_type = DataType::new(stringify!(Liquidation), Some(metadata), None);
    let cmd = SubscribeCustomData {
        data_type,
        client_id: Some(ClientId::from("BINANCE")),
        venue: Some(instrument_id.venue),
        command_id: nautilus_core::UUID4::new(),
        ts_init: UnixNanos::default(),
        correlation_id: None,
        params: None,
    };

    client.subscribe(&cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_unsubscribe_trades() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");

    let sub_cmd = SubscribeTrades::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    client.subscribe_trades(&sub_cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let unsub_cmd = UnsubscribeTrades::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let result = client.unsubscribe_trades(&unsub_cmd);
    assert!(result.is_ok());
}

#[rstest]
#[tokio::test]
async fn test_unsubscribe_quotes() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");

    let sub_cmd = SubscribeQuotes::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    client.subscribe_quotes(&sub_cmd).unwrap();

    wait_until_async(
        || {
            let found = rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let unsub_cmd = UnsubscribeQuotes::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let result = client.unsubscribe_quotes(&unsub_cmd);
    assert!(result.is_ok());
}

#[rstest]
#[tokio::test]
async fn test_connect_disconnect_reconnect() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();
    assert!(client.is_connected());

    // Drain instrument events
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());

    // Reconnect
    client.connect().await.unwrap();
    assert!(client.is_connected());

    // Should emit instruments again on reconnect
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
}

#[rstest]
#[tokio::test]
async fn test_subscribe_trades_and_quotes_simultaneously() {
    let addr = start_data_test_server().await;
    let base_url_http = format!("http://{addr}");
    let base_url_ws = format!("ws://{addr}/ws");

    let (mut client, mut rx) = create_test_data_client(base_url_http, base_url_ws);

    client.connect().await.unwrap();

    // Drain instrument events
    wait_until_async(
        || {
            let found = rx
                .try_recv()
                .is_ok_and(|e| matches!(e, DataEvent::Instrument(_)));
            async move { found }
        },
        Duration::from_secs(5),
    )
    .await;
    while rx.try_recv().is_ok() {}

    let instrument_id = InstrumentId::from("BTCUSDT-PERP.BINANCE");

    let trades_cmd = SubscribeTrades::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    let quotes_cmd = SubscribeQuotes::new(
        instrument_id,
        Some(ClientId::from("BINANCE")),
        None,
        nautilus_core::UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );

    client.subscribe_trades(&trades_cmd).unwrap();
    client.subscribe_quotes(&quotes_cmd).unwrap();

    let mut data_count = 0;
    wait_until_async(
        || {
            while rx.try_recv().is_ok_and(|e| matches!(e, DataEvent::Data(_))) {
                data_count += 1;
            }
            async move { data_count >= 2 }
        },
        Duration::from_secs(5),
    )
    .await;
}
