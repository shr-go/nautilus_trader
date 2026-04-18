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

//! A canonical `OpenInterest` data type representing a venue open-interest sample.

use std::{collections::HashMap, fmt::Display, hash::Hash};

use indexmap::IndexMap;
use nautilus_core::{UnixNanos, serialization::Serializable};
use serde::{Deserialize, Serialize};

use super::HasTsInit;
use crate::{
    identifiers::InstrumentId,
    types::{Quantity, fixed::FIXED_SIZE_BINARY},
};

/// Represents a sample of the open interest for a derivatives instrument at a venue.
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
pub struct OpenInterest {
    /// The instrument ID for the open interest sample.
    pub instrument_id: InstrumentId,
    /// The open interest value (contract-denominated quantity).
    pub value: Quantity,
    /// UNIX timestamp (nanoseconds) when the sample was generated at the venue.
    pub ts_event: UnixNanos,
    /// UNIX timestamp (nanoseconds) when the instance was created.
    pub ts_init: UnixNanos,
}

impl OpenInterest {
    /// Creates a new [`OpenInterest`] instance.
    #[must_use]
    pub fn new(
        instrument_id: InstrumentId,
        value: Quantity,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            instrument_id,
            value,
            ts_event,
            ts_init,
        }
    }

    /// Returns the metadata for the type, for use with serialization formats.
    #[must_use]
    pub fn get_metadata(
        instrument_id: &InstrumentId,
        size_precision: u8,
    ) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert("instrument_id".to_string(), instrument_id.to_string());
        metadata.insert("size_precision".to_string(), size_precision.to_string());
        metadata
    }

    /// Returns the field map for the type, for use with Arrow schemas.
    #[must_use]
    pub fn get_fields() -> IndexMap<String, String> {
        let mut metadata = IndexMap::new();
        metadata.insert("value".to_string(), FIXED_SIZE_BINARY.to_string());
        metadata.insert("ts_event".to_string(), "UInt64".to_string());
        metadata.insert("ts_init".to_string(), "UInt64".to_string());
        metadata
    }
}

impl Display for OpenInterest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.instrument_id, self.value, self.ts_event, self.ts_init
        )
    }
}

impl Serializable for OpenInterest {}

impl HasTsInit for OpenInterest {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
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
    fn value() -> Quantity {
        Quantity::from("12345.678")
    }

    #[rstest]
    fn test_new(instrument_id: InstrumentId, value: Quantity) {
        let oi = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));

        assert_eq!(oi.instrument_id, instrument_id);
        assert_eq!(oi.value, value);
        assert_eq!(oi.ts_event.as_u64(), 1);
        assert_eq!(oi.ts_init.as_u64(), 2);
    }

    #[rstest]
    fn test_display(instrument_id: InstrumentId, value: Quantity) {
        let oi = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));

        assert_eq!(format!("{oi}"), "BTCUSDT-PERP.BINANCE,12345.678,1,2");
    }

    #[rstest]
    fn test_get_ts_init(instrument_id: InstrumentId, value: Quantity) {
        let oi = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));

        assert_eq!(oi.ts_init(), UnixNanos::from(2));
    }

    #[rstest]
    fn test_eq_hash(instrument_id: InstrumentId, value: Quantity) {
        let oi1 = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));
        let oi2 = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));
        let oi3 = OpenInterest::new(
            instrument_id,
            Quantity::from("0.001"),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );

        assert_eq!(oi1, oi2);
        assert_ne!(oi1, oi3);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        oi1.hash(&mut h1);
        oi2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[rstest]
    fn test_json_serialization(instrument_id: InstrumentId, value: Quantity) {
        let oi = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));

        let serialized = oi.to_json_bytes().unwrap();
        let deserialized = OpenInterest::from_json_bytes(&serialized).unwrap();
        assert_eq!(oi, deserialized);
    }

    #[rstest]
    fn test_msgpack_serialization(instrument_id: InstrumentId, value: Quantity) {
        let oi = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));

        let serialized = oi.to_msgpack_bytes().unwrap();
        let deserialized = OpenInterest::from_msgpack_bytes(&serialized).unwrap();
        assert_eq!(oi, deserialized);
    }

    #[rstest]
    fn test_serde_json(instrument_id: InstrumentId, value: Quantity) {
        let oi = OpenInterest::new(instrument_id, value, UnixNanos::from(1), UnixNanos::from(2));
        let json = serde_json::to_string(&oi).unwrap();
        let deserialized: OpenInterest = serde_json::from_str(&json).unwrap();
        assert_eq!(oi, deserialized);
    }
}
