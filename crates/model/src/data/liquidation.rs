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

//! A canonical `Liquidation` data type representing a forced-liquidation order event.

use std::{any::Any, collections::HashMap, fmt::Display, hash::Hash, sync::Arc};

use indexmap::IndexMap;
use nautilus_core::{UnixNanos, serialization::Serializable};
use serde::{Deserialize, Serialize};

use super::{CustomDataTrait, HasTsInit};
use crate::{
    enums::{OrderSide, OrderStatus},
    identifiers::InstrumentId,
    types::{Price, Quantity, fixed::FIXED_SIZE_BINARY},
};

/// Represents a forced-liquidation (venue-initiated) order event.
///
/// Venues such as Binance Futures broadcast liquidation orders on dedicated streams.
/// The canonical event captures just the information that is meaningful across venues.
/// Venue-specific fields (time-in-force, order type, accumulated fill quantity, etc.)
/// live on adapter-specific types.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.model", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.model")
)]
pub struct Liquidation {
    /// The instrument ID for the liquidation order.
    pub instrument_id: InstrumentId,
    /// The side of the liquidation order (the side being liquidated out of).
    pub side: OrderSide,
    /// The liquidation order quantity.
    pub quantity: Quantity,
    /// The liquidation order price.
    pub price: Price,
    /// The order status at the time of the event.
    pub order_status: OrderStatus,
    /// UNIX timestamp (nanoseconds) when the liquidation event occurred at the venue.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the instance was created.
    pub ts_init: UnixNanos,
}

impl Liquidation {
    /// Creates a new [`Liquidation`] instance.
    #[must_use]
    pub fn new(
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
        price: Price,
        order_status: OrderStatus,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            instrument_id,
            side,
            quantity,
            price,
            order_status,
            ts_event,
            ts_init,
        }
    }

    /// Returns the metadata for the type, for use with serialization formats.
    #[must_use]
    pub fn get_metadata(
        instrument_id: &InstrumentId,
        price_precision: u8,
        size_precision: u8,
    ) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("instrument_id".to_string(), instrument_id.to_string());
        metadata.insert("price_precision".to_string(), price_precision.to_string());
        metadata.insert("size_precision".to_string(), size_precision.to_string());
        metadata
    }

    /// Returns the field map for the type, for use with Arrow schemas.
    #[must_use]
    pub fn get_fields() -> IndexMap<String, String> {
        let mut metadata = IndexMap::new();
        metadata.insert("side".to_string(), "UInt8".to_string());
        metadata.insert("quantity".to_string(), FIXED_SIZE_BINARY.to_string());
        metadata.insert("price".to_string(), FIXED_SIZE_BINARY.to_string());
        metadata.insert("order_status".to_string(), "UInt8".to_string());
        metadata.insert("ts_event".to_string(), "UInt64".to_string());
        metadata.insert("ts_init".to_string(), "UInt64".to_string());
        metadata
    }
}

impl Display for Liquidation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{}",
            self.instrument_id,
            self.side,
            self.quantity,
            self.price,
            self.order_status,
            self.ts_event,
            self.ts_init,
        )
    }
}

impl Serializable for Liquidation {}

impl HasTsInit for Liquidation {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

/// Enables wrapping `Liquidation` in a `CustomData` envelope so that Rust
/// actors can receive events via `subscribe_data(DataType(Liquidation, ...))`.
/// The message bus uses a typed handler of `&CustomData`, so the raw type
/// would silently fail type-match on that subscription path.
impl CustomDataTrait for Liquidation {
    fn type_name(&self) -> &'static str {
        stringify!(Liquidation)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(*self)
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|o| self == o)
    }

    fn type_name_static() -> &'static str {
        stringify!(Liquidation)
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let parsed: Self = serde_json::from_value(value)?;
        Ok(Arc::new(parsed))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use nautilus_core::serialization::{
        Serializable,
        msgpack::{FromMsgPack, ToMsgPack},
    };
    use rstest::{fixture, rstest};

    use super::*;

    #[fixture]
    fn instrument_id() -> InstrumentId {
        InstrumentId::from("BTCUSDT-PERP.BINANCE")
    }

    #[fixture]
    fn quantity() -> Quantity {
        Quantity::from("0.500")
    }

    #[fixture]
    fn price() -> Price {
        Price::from("50000.10")
    }

    #[rstest]
    fn test_new(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        assert_eq!(liq.instrument_id, instrument_id);
        assert_eq!(liq.side, OrderSide::Sell);
        assert_eq!(liq.quantity, quantity);
        assert_eq!(liq.price, price);
        assert_eq!(liq.order_status, OrderStatus::Filled);
        assert_eq!(liq.ts_event.as_u64(), 1);
        assert_eq!(liq.ts_init.as_u64(), 2);
    }

    #[rstest]
    fn test_display(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        assert_eq!(
            format!("{liq}"),
            "BTCUSDT-PERP.BINANCE,SELL,0.500,50000.10,FILLED,1,2"
        );
    }

    #[rstest]
    fn test_get_ts_init(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq = Liquidation::new(
            instrument_id,
            OrderSide::Buy,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        assert_eq!(liq.ts_init(), UnixNanos::from(2));
    }

    #[rstest]
    fn test_eq_hash(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq1 = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let liq2 = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let liq3 = Liquidation::new(
            instrument_id,
            OrderSide::Buy,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        assert_eq!(liq1, liq2);
        assert_ne!(liq1, liq3);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        liq1.hash(&mut h1);
        liq2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[rstest]
    fn test_json_serialization(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        let serialized = liq.to_json_bytes().unwrap();
        let deserialized = Liquidation::from_json_bytes(&serialized).unwrap();
        assert_eq!(liq, deserialized);
    }

    #[rstest]
    fn test_msgpack_serialization(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        let serialized = liq.to_msgpack_bytes().unwrap();
        let deserialized = Liquidation::from_msgpack_bytes(&serialized).unwrap();
        assert_eq!(liq, deserialized);
    }

    #[rstest]
    fn test_serde_json(instrument_id: InstrumentId, quantity: Quantity, price: Price) {
        let liq = Liquidation::new(
            instrument_id,
            OrderSide::Sell,
            quantity,
            price,
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let json = serde_json::to_string(&liq).unwrap();
        let deserialized: Liquidation = serde_json::from_str(&json).unwrap();
        assert_eq!(liq, deserialized);
    }
}
