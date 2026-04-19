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

use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow::{
    array::{FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt8Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use nautilus_model::{
    data::Liquidation,
    enums::{OrderSide, OrderStatus},
    identifiers::InstrumentId,
    types::fixed::PRECISION_BYTES,
};

use super::{
    DecodeDataFromRecordBatch, EncodingError, KEY_INSTRUMENT_ID, KEY_PRICE_PRECISION,
    KEY_SIZE_PRECISION, decode_price, decode_quantity, extract_column, validate_precision_bytes,
};
use crate::arrow::{ArrowSchemaProvider, Data, DecodeFromRecordBatch, EncodeToRecordBatch};

impl ArrowSchemaProvider for Liquidation {
    fn get_schema(metadata: Option<HashMap<String, String>>) -> Schema {
        let fields = vec![
            Field::new("side", DataType::UInt8, false),
            Field::new("quantity", DataType::FixedSizeBinary(PRECISION_BYTES), false),
            Field::new("price", DataType::FixedSizeBinary(PRECISION_BYTES), false),
            Field::new("order_status", DataType::UInt8, false),
            Field::new("ts_event", DataType::UInt64, false),
            Field::new("ts_init", DataType::UInt64, false),
        ];

        match metadata {
            Some(metadata) => Schema::new_with_metadata(fields, metadata),
            None => Schema::new(fields),
        }
    }
}

fn parse_metadata(
    metadata: &HashMap<String, String>,
) -> Result<(InstrumentId, u8, u8), EncodingError> {
    let instrument_id_str = metadata
        .get(KEY_INSTRUMENT_ID)
        .ok_or(EncodingError::MissingMetadata(KEY_INSTRUMENT_ID))?;
    let instrument_id = InstrumentId::from_str(instrument_id_str)
        .map_err(|e| EncodingError::ParseError(KEY_INSTRUMENT_ID, e.to_string()))?;

    let price_precision = metadata
        .get(KEY_PRICE_PRECISION)
        .ok_or(EncodingError::MissingMetadata(KEY_PRICE_PRECISION))?
        .parse::<u8>()
        .map_err(|e| EncodingError::ParseError(KEY_PRICE_PRECISION, e.to_string()))?;

    let size_precision = metadata
        .get(KEY_SIZE_PRECISION)
        .ok_or(EncodingError::MissingMetadata(KEY_SIZE_PRECISION))?
        .parse::<u8>()
        .map_err(|e| EncodingError::ParseError(KEY_SIZE_PRECISION, e.to_string()))?;

    Ok((instrument_id, price_precision, size_precision))
}

impl EncodeToRecordBatch for Liquidation {
    fn encode_batch(
        metadata: &HashMap<String, String>,
        data: &[Self],
    ) -> Result<RecordBatch, ArrowError> {
        let mut side_builder = UInt8Array::builder(data.len());
        let mut quantity_builder =
            FixedSizeBinaryBuilder::with_capacity(data.len(), PRECISION_BYTES);
        let mut price_builder = FixedSizeBinaryBuilder::with_capacity(data.len(), PRECISION_BYTES);
        let mut order_status_builder = UInt8Array::builder(data.len());
        let mut ts_event_builder = UInt64Array::builder(data.len());
        let mut ts_init_builder = UInt64Array::builder(data.len());

        for liq in data {
            side_builder.append_value(liq.side as u8);
            quantity_builder
                .append_value(liq.quantity.raw.to_le_bytes())
                .unwrap();
            price_builder
                .append_value(liq.price.raw.to_le_bytes())
                .unwrap();
            order_status_builder.append_value(liq.order_status as u8);
            ts_event_builder.append_value(liq.ts_event.as_u64());
            ts_init_builder.append_value(liq.ts_init.as_u64());
        }

        RecordBatch::try_new(
            Self::get_schema(Some(metadata.clone())).into(),
            vec![
                Arc::new(side_builder.finish()),
                Arc::new(quantity_builder.finish()),
                Arc::new(price_builder.finish()),
                Arc::new(order_status_builder.finish()),
                Arc::new(ts_event_builder.finish()),
                Arc::new(ts_init_builder.finish()),
            ],
        )
    }

    fn metadata(&self) -> HashMap<String, String> {
        Liquidation::get_metadata(
            &self.instrument_id,
            self.price.precision,
            self.quantity.precision,
        )
    }
}

impl DecodeFromRecordBatch for Liquidation {
    fn decode_batch(
        metadata: &HashMap<String, String>,
        record_batch: RecordBatch,
    ) -> Result<Vec<Self>, EncodingError> {
        let (instrument_id, price_precision, size_precision) = parse_metadata(metadata)?;
        let cols = record_batch.columns();

        let side_values = extract_column::<UInt8Array>(cols, "side", 0, DataType::UInt8)?;
        let quantity_values = extract_column::<FixedSizeBinaryArray>(
            cols,
            "quantity",
            1,
            DataType::FixedSizeBinary(PRECISION_BYTES),
        )?;
        let price_values = extract_column::<FixedSizeBinaryArray>(
            cols,
            "price",
            2,
            DataType::FixedSizeBinary(PRECISION_BYTES),
        )?;
        let order_status_values =
            extract_column::<UInt8Array>(cols, "order_status", 3, DataType::UInt8)?;
        let ts_event_values = extract_column::<UInt64Array>(cols, "ts_event", 4, DataType::UInt64)?;
        let ts_init_values = extract_column::<UInt64Array>(cols, "ts_init", 5, DataType::UInt64)?;

        validate_precision_bytes(quantity_values, "quantity")?;
        validate_precision_bytes(price_values, "price")?;

        let result: Result<Vec<Self>, EncodingError> = (0..record_batch.num_rows())
            .map(|row| {
                let side = OrderSide::from_repr(side_values.value(row) as usize).ok_or_else(
                    || EncodingError::ParseError("side", format!("row {row}: invalid enum value")),
                )?;
                let quantity =
                    decode_quantity(quantity_values.value(row), size_precision, "quantity", row)?;
                let price = decode_price(price_values.value(row), price_precision, "price", row)?;
                let order_status = OrderStatus::from_repr(order_status_values.value(row) as usize)
                    .ok_or_else(|| {
                        EncodingError::ParseError(
                            "order_status",
                            format!("row {row}: invalid enum value"),
                        )
                    })?;

                Ok(Self {
                    instrument_id,
                    side,
                    quantity,
                    price,
                    order_status,
                    ts_event: ts_event_values.value(row).into(),
                    ts_init: ts_init_values.value(row).into(),
                })
            })
            .collect();

        result
    }
}

impl DecodeDataFromRecordBatch for Liquidation {
    fn decode_data_batch(
        metadata: &HashMap<String, String>,
        record_batch: RecordBatch,
    ) -> Result<Vec<Data>, EncodingError> {
        let items: Vec<Self> = Self::decode_batch(metadata, record_batch)?;
        Ok(items.into_iter().map(Data::from).collect())
    }
}
