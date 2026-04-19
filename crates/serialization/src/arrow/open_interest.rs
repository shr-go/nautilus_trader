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
    array::{FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt64Array},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    record_batch::RecordBatch,
};
use nautilus_model::{
    data::OpenInterest, identifiers::InstrumentId, types::fixed::PRECISION_BYTES,
};

use super::{
    DecodeDataFromRecordBatch, EncodingError, KEY_INSTRUMENT_ID, KEY_SIZE_PRECISION,
    decode_quantity, extract_column, validate_precision_bytes,
};
use crate::arrow::{ArrowSchemaProvider, Data, DecodeFromRecordBatch, EncodeToRecordBatch};

impl ArrowSchemaProvider for OpenInterest {
    fn get_schema(metadata: Option<HashMap<String, String>>) -> Schema {
        let fields = vec![
            Field::new("value", DataType::FixedSizeBinary(PRECISION_BYTES), false),
            Field::new("ts_event", DataType::UInt64, false),
            Field::new("ts_init", DataType::UInt64, false),
        ];

        match metadata {
            Some(metadata) => Schema::new_with_metadata(fields, metadata),
            None => Schema::new(fields),
        }
    }
}

fn parse_metadata(metadata: &HashMap<String, String>) -> Result<(InstrumentId, u8), EncodingError> {
    let instrument_id_str = metadata
        .get(KEY_INSTRUMENT_ID)
        .ok_or(EncodingError::MissingMetadata(KEY_INSTRUMENT_ID))?;
    let instrument_id = InstrumentId::from_str(instrument_id_str)
        .map_err(|e| EncodingError::ParseError(KEY_INSTRUMENT_ID, e.to_string()))?;

    let size_precision = metadata
        .get(KEY_SIZE_PRECISION)
        .ok_or(EncodingError::MissingMetadata(KEY_SIZE_PRECISION))?
        .parse::<u8>()
        .map_err(|e| EncodingError::ParseError(KEY_SIZE_PRECISION, e.to_string()))?;

    Ok((instrument_id, size_precision))
}

impl EncodeToRecordBatch for OpenInterest {
    fn encode_batch(
        metadata: &HashMap<String, String>,
        data: &[Self],
    ) -> Result<RecordBatch, ArrowError> {
        let mut value_builder = FixedSizeBinaryBuilder::with_capacity(data.len(), PRECISION_BYTES);
        let mut ts_event_builder = UInt64Array::builder(data.len());
        let mut ts_init_builder = UInt64Array::builder(data.len());

        for oi in data {
            value_builder.append_value(oi.value.raw.to_le_bytes()).unwrap();
            ts_event_builder.append_value(oi.ts_event.as_u64());
            ts_init_builder.append_value(oi.ts_init.as_u64());
        }

        RecordBatch::try_new(
            Self::get_schema(Some(metadata.clone())).into(),
            vec![
                Arc::new(value_builder.finish()),
                Arc::new(ts_event_builder.finish()),
                Arc::new(ts_init_builder.finish()),
            ],
        )
    }

    fn metadata(&self) -> HashMap<String, String> {
        OpenInterest::get_metadata(&self.instrument_id, self.value.precision)
    }
}

impl DecodeFromRecordBatch for OpenInterest {
    fn decode_batch(
        metadata: &HashMap<String, String>,
        record_batch: RecordBatch,
    ) -> Result<Vec<Self>, EncodingError> {
        let (instrument_id, size_precision) = parse_metadata(metadata)?;
        let cols = record_batch.columns();

        let value_values = extract_column::<FixedSizeBinaryArray>(
            cols,
            "value",
            0,
            DataType::FixedSizeBinary(PRECISION_BYTES),
        )?;
        let ts_event_values = extract_column::<UInt64Array>(cols, "ts_event", 1, DataType::UInt64)?;
        let ts_init_values = extract_column::<UInt64Array>(cols, "ts_init", 2, DataType::UInt64)?;

        validate_precision_bytes(value_values, "value")?;

        let result: Result<Vec<Self>, EncodingError> = (0..record_batch.num_rows())
            .map(|row| {
                let value = decode_quantity(value_values.value(row), size_precision, "value", row)?;
                Ok(Self {
                    instrument_id,
                    value,
                    ts_event: ts_event_values.value(row).into(),
                    ts_init: ts_init_values.value(row).into(),
                })
            })
            .collect();

        result
    }
}

impl DecodeDataFromRecordBatch for OpenInterest {
    fn decode_data_batch(
        metadata: &HashMap<String, String>,
        record_batch: RecordBatch,
    ) -> Result<Vec<Data>, EncodingError> {
        let items: Vec<Self> = Self::decode_batch(metadata, record_batch)?;
        Ok(items.into_iter().map(Data::from).collect())
    }
}
