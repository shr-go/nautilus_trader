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

use std::{
    collections::{HashMap, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    str::FromStr,
};

use nautilus_core::{
    UnixNanos,
    python::{
        IntoPyObjectNautilusExt,
        serialization::{from_dict_pyo3, to_dict_pyo3},
        to_pyvalue_err,
    },
    serialization::{
        Serializable,
        msgpack::{FromMsgPack, ToMsgPack},
    },
};
use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    pyclass::CompareOp,
    types::{PyDict, PyInt, PyString, PyTuple},
};

use crate::{
    data::Liquidation,
    enums::{OrderSide, OrderStatus},
    identifiers::InstrumentId,
    python::common::PY_MODULE_MODEL,
    types::{
        price::{Price, PriceRaw},
        quantity::{Quantity, QuantityRaw},
    },
};

impl Liquidation {
    /// Creates a new [`Liquidation`] from a Python object.
    ///
    /// # Errors
    ///
    /// Returns a `PyErr` if attribute extraction or type conversion fails.
    pub fn from_pyobject(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let instrument_id_obj: Bound<'_, PyAny> = obj.getattr("instrument_id")?.extract()?;
        let instrument_id_str: String = instrument_id_obj.getattr("value")?.extract()?;
        let instrument_id =
            InstrumentId::from_str(instrument_id_str.as_str()).map_err(to_pyvalue_err)?;

        let side_obj: Bound<'_, PyAny> = obj.getattr("side")?.extract()?;
        let side: OrderSide = side_obj.extract()?;

        let quantity_py: Bound<'_, PyAny> = obj.getattr("quantity")?.extract()?;
        let quantity_raw: QuantityRaw = quantity_py.getattr("raw")?.extract()?;
        let quantity_prec: u8 = quantity_py.getattr("precision")?.extract()?;
        let quantity = Quantity::from_raw(quantity_raw, quantity_prec);

        let price_py: Bound<'_, PyAny> = obj.getattr("price")?.extract()?;
        let price_raw: PriceRaw = price_py.getattr("raw")?.extract()?;
        let price_prec: u8 = price_py.getattr("precision")?.extract()?;
        let price = Price::from_raw(price_raw, price_prec);

        let order_status_obj: Bound<'_, PyAny> = obj.getattr("order_status")?.extract()?;
        let order_status: OrderStatus = order_status_obj.extract()?;

        let ts_event: u64 = obj.getattr("ts_event")?.extract()?;
        let ts_init: u64 = obj.getattr("ts_init")?.extract()?;

        Ok(Self::new(
            instrument_id,
            side,
            quantity,
            price,
            order_status,
            ts_event.into(),
            ts_init.into(),
        ))
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl Liquidation {
    /// Represents a forced-liquidation order event.
    #[new]
    fn py_new(
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
        price: Price,
        order_status: OrderStatus,
        ts_event: u64,
        ts_init: u64,
    ) -> Self {
        Self::new(
            instrument_id,
            side,
            quantity,
            price,
            order_status,
            ts_event.into(),
            ts_init.into(),
        )
    }

    fn __setstate__(&mut self, state: &Bound<'_, PyAny>) -> PyResult<()> {
        let py_tuple: &Bound<'_, PyTuple> = state.cast::<PyTuple>()?;
        let binding = py_tuple.get_item(0)?;
        let instrument_id_str = binding.cast::<PyString>()?.extract::<&str>()?;
        let side: OrderSide = py_tuple.get_item(1)?.extract()?;
        let quantity_raw = py_tuple
            .get_item(2)?
            .cast::<PyInt>()?
            .extract::<QuantityRaw>()?;
        let quantity_prec = py_tuple.get_item(3)?.cast::<PyInt>()?.extract::<u8>()?;
        let price_raw = py_tuple
            .get_item(4)?
            .cast::<PyInt>()?
            .extract::<PriceRaw>()?;
        let price_prec = py_tuple.get_item(5)?.cast::<PyInt>()?.extract::<u8>()?;
        let order_status: OrderStatus = py_tuple.get_item(6)?.extract()?;
        let ts_event = py_tuple.get_item(7)?.cast::<PyInt>()?.extract::<u64>()?;
        let ts_init = py_tuple.get_item(8)?.cast::<PyInt>()?.extract::<u64>()?;

        self.instrument_id = InstrumentId::from_str(instrument_id_str).map_err(to_pyvalue_err)?;
        self.side = side;
        self.quantity = Quantity::from_raw(quantity_raw, quantity_prec);
        self.price = Price::from_raw(price_raw, price_prec);
        self.order_status = order_status;
        self.ts_event = ts_event.into();
        self.ts_init = ts_init.into();

        Ok(())
    }

    fn __getstate__(&self, py: Python) -> PyResult<Py<PyAny>> {
        (
            self.instrument_id.to_string(),
            self.side,
            self.quantity.raw,
            self.quantity.precision,
            self.price.raw,
            self.price.precision,
            self.order_status,
            self.ts_event.as_u64(),
            self.ts_init.as_u64(),
        )
            .into_py_any(py)
    }

    fn __reduce__(&self, py: Python) -> PyResult<Py<PyAny>> {
        let safe_constructor = py.get_type::<Self>().getattr("_safe_constructor")?;
        let state = self.__getstate__(py)?;
        (safe_constructor, PyTuple::empty(py), state).into_py_any(py)
    }

    #[staticmethod]
    fn _safe_constructor() -> Self {
        Self::new(
            InstrumentId::from("NULL.NULL"),
            OrderSide::NoOrderSide,
            Quantity::zero(0),
            Price::zero(0),
            OrderStatus::Initialized,
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp, py: Python<'_>) -> Py<PyAny> {
        match op {
            CompareOp::Eq => self.eq(other).into_py_any_unwrap(py),
            CompareOp::Ne => self.ne(other).into_py_any_unwrap(py),
            _ => py.NotImplemented(),
        }
    }

    fn __hash__(&self) -> isize {
        let mut h = DefaultHasher::new();
        self.hash(&mut h);
        h.finish() as isize
    }

    fn __repr__(&self) -> String {
        format!("{}({})", stringify!(Liquidation), self)
    }

    fn __str__(&self) -> String {
        self.to_string()
    }

    #[getter]
    #[pyo3(name = "instrument_id")]
    fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter]
    #[pyo3(name = "side")]
    fn py_side(&self) -> OrderSide {
        self.side
    }

    #[getter]
    #[pyo3(name = "quantity")]
    fn py_quantity(&self) -> Quantity {
        self.quantity
    }

    #[getter]
    #[pyo3(name = "price")]
    fn py_price(&self) -> Price {
        self.price
    }

    #[getter]
    #[pyo3(name = "order_status")]
    fn py_order_status(&self) -> OrderStatus {
        self.order_status
    }

    #[getter]
    #[pyo3(name = "ts_event")]
    fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter]
    #[pyo3(name = "ts_init")]
    fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }

    #[staticmethod]
    #[pyo3(name = "fully_qualified_name")]
    fn py_fully_qualified_name() -> String {
        format!("{}:{}", PY_MODULE_MODEL, stringify!(Liquidation))
    }

    /// Returns the metadata for the type, for use with serialization formats.
    #[staticmethod]
    #[pyo3(name = "get_metadata")]
    fn py_get_metadata(
        instrument_id: &InstrumentId,
        price_precision: u8,
        size_precision: u8,
    ) -> HashMap<String, String> {
        Self::get_metadata(instrument_id, price_precision, size_precision)
    }

    /// Returns the field map for the type, for use with Arrow schemas.
    #[staticmethod]
    #[pyo3(name = "get_fields")]
    fn py_get_fields(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
        let py_dict = PyDict::new(py);
        for (k, v) in Self::get_fields() {
            py_dict.set_item(k, v)?;
        }

        Ok(py_dict)
    }

    /// Returns a new object from the given dictionary representation.
    #[staticmethod]
    #[pyo3(name = "from_dict")]
    fn py_from_dict(py: Python<'_>, values: Py<PyDict>) -> PyResult<Self> {
        from_dict_pyo3(py, values)
    }

    /// Return a dictionary representation of the object.
    #[pyo3(name = "to_dict")]
    fn py_to_dict(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        to_dict_pyo3(py, self)
    }

    /// Return JSON encoded bytes representation of the object.
    #[pyo3(name = "to_json_bytes")]
    fn py_to_json_bytes(&self, py: Python<'_>) -> Py<PyAny> {
        self.to_json_bytes().unwrap().into_py_any_unwrap(py)
    }

    /// Return `MsgPack` encoded bytes representation of the object.
    #[pyo3(name = "to_msgpack_bytes")]
    fn py_to_msgpack_bytes(&self, py: Python<'_>) -> Py<PyAny> {
        self.to_msgpack_bytes().unwrap().into_py_any_unwrap(py)
    }
}

#[pymethods]
impl Liquidation {
    #[staticmethod]
    #[pyo3(name = "from_json")]
    fn py_from_json(data: &[u8]) -> PyResult<Self> {
        Self::from_json_bytes(data).map_err(to_pyvalue_err)
    }

    #[staticmethod]
    #[pyo3(name = "from_msgpack")]
    fn py_from_msgpack(data: &[u8]) -> PyResult<Self> {
        Self::from_msgpack_bytes(data).map_err(to_pyvalue_err)
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::python::IntoPyObjectNautilusExt;
    use pyo3::Python;
    use rstest::{fixture, rstest};

    use super::*;
    use crate::{identifiers::InstrumentId, types::{Price, Quantity}};

    #[fixture]
    fn liquidation() -> Liquidation {
        Liquidation::new(
            InstrumentId::from("BTCUSDT-PERP.BINANCE"),
            OrderSide::Sell,
            Quantity::from("0.500"),
            Price::from("50000.10"),
            OrderStatus::Filled,
            UnixNanos::from(1),
            UnixNanos::from(2),
        )
    }

    #[rstest]
    fn test_to_dict(liquidation: Liquidation) {
        Python::initialize();
        Python::attach(|py| {
            let dict_string = liquidation.py_to_dict(py).unwrap().to_string();
            assert!(dict_string.contains("'type': 'Liquidation'"));
            assert!(dict_string.contains("'instrument_id': 'BTCUSDT-PERP.BINANCE'"));
            assert!(dict_string.contains("'side': 'SELL'"));
            assert!(dict_string.contains("'order_status': 'FILLED'"));
        });
    }

    #[rstest]
    fn test_from_dict(liquidation: Liquidation) {
        Python::initialize();
        Python::attach(|py| {
            let dict = liquidation.py_to_dict(py).unwrap();
            let parsed = Liquidation::py_from_dict(py, dict).unwrap();
            assert_eq!(parsed, liquidation);
        });
    }

    #[rstest]
    fn test_from_pyobject(liquidation: Liquidation) {
        Python::initialize();
        Python::attach(|py| {
            let pyobject = liquidation.into_py_any_unwrap(py);
            let parsed = Liquidation::from_pyobject(pyobject.bind(py)).unwrap();
            assert_eq!(parsed, liquidation);
        });
    }
}
