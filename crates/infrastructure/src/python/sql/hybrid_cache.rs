// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
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

//! Python bindings for GreptimeDB-PostgreSQL Hybrid Cache Database Adapter

use std::collections::HashMap;

use bytes::Bytes;
use nautilus_common::{
    cache::database::CacheDatabaseAdapter,
    custom::CustomData,
    runtime::get_runtime,
    signal::Signal,
};
use nautilus_core::python::to_pyruntime_err;
use nautilus_model::{
    data::{bar::BarType, Bar, DataType, QuoteTick, TradeTick},
    events::{OrderSnapshot, PositionSnapshot},
    identifiers::{
        AccountId, ClientId, ClientOrderId, ComponentId, InstrumentId, PositionId, StrategyId, VenueOrderId,
    },
    position::Position,
    python::{
        account::{account_any_to_pyobject, pyobject_to_account_any},
        events::order::pyobject_to_order_event,
        instruments::{instrument_any_to_pyobject, pyobject_to_instrument_any},
        orders::{order_any_to_pyobject, pyobject_to_order_any},
    },
    types::Currency,
};
use std::str::FromStr;
use pyo3::{IntoPyObjectExt, prelude::*};
use uuid::Uuid;

use crate::sql::greptime_postgres_hybrid_cache::{
    GreptimePostgresHybridCacheAdapter,
    GreptimePostgresHybridCacheConfig
};

#[pymethods]
impl GreptimePostgresHybridCacheAdapter {
    #[staticmethod]
    #[pyo3(name = "connect")]
    #[pyo3(signature = (
        user_id,
        postgres_host=None,
        postgres_port=None,
        postgres_username=None,
        postgres_password=None,
        postgres_database=None,
        greptime_host=None,
        greptime_port=None,
        greptime_database=None,
        greptime_username=None,
        greptime_password=None,
        buffer_interval_ms=None,
        batch_size=None,
        trading_mode=None
    ))]
    fn py_connect(
        user_id: String,
        postgres_host: Option<String>,
        postgres_port: Option<u16>,
        postgres_username: Option<String>,
        postgres_password: Option<String>,
        postgres_database: Option<String>,
        greptime_host: Option<String>,
        greptime_port: Option<u16>,
        greptime_database: Option<String>,
        greptime_username: Option<String>,
        greptime_password: Option<String>,
        buffer_interval_ms: Option<u64>,
        batch_size: Option<usize>,
        trading_mode: Option<String>,
    ) -> PyResult<Self> {
        let uuid = Uuid::parse_str(&user_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid user_id UUID: {}", e)))?;
        
        let config = GreptimePostgresHybridCacheConfig {
            postgres_host: postgres_host.unwrap_or_else(|| "localhost".to_string()),
            postgres_port: postgres_port.unwrap_or(5432),
            postgres_username: postgres_username.unwrap_or_else(|| "postgres".to_string()),
            postgres_password: postgres_password.unwrap_or_else(|| "".to_string()),
            postgres_database: postgres_database.unwrap_or_else(|| "fincept_terminal".to_string()),
            user_id: uuid,
            trader_id: None,
            account_id: None,
            strategy_id: None,
            position_counter: 0,
            position_id_mapping: std::collections::HashMap::new(),
            enable_multiple_trades: false, // Default to false - single position per run
            greptime_host: greptime_host.unwrap_or_else(|| "localhost".to_string()),
            greptime_port: greptime_port.unwrap_or(4000),
            greptime_database: greptime_database.unwrap_or_else(|| "nautilus_timeseries".to_string()),
            greptime_username: greptime_username,
            greptime_password: greptime_password,
            buffer_interval_ms: buffer_interval_ms.unwrap_or(100),
            batch_size: batch_size.unwrap_or(1000),
            trading_mode: trading_mode.unwrap_or_else(|| "backtest".to_string()),  // Default to backtest
        };
        
        // Use get_runtime() to handle existing runtime context
        let rt = get_runtime();
        
        let result = rt.block_on(async {
            GreptimePostgresHybridCacheAdapter::connect(config).await
        });
        result.map_err(to_pyruntime_err)
    }

    #[pyo3(name = "close")]
    fn py_close(&mut self) -> PyResult<()> {
        self.close().map_err(to_pyruntime_err)
    }

    #[pyo3(name = "flush_db")]
    fn py_flush_db(&mut self) -> PyResult<()> {
        self.flush().map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load")]
    fn py_load(&self) -> PyResult<HashMap<String, Vec<u8>>> {
        // load() is synchronous in the trait - use the trait method which handles runtime properly
        match self.load() {
            Ok(map) => {
                // Convert HashMap<String, Bytes> to HashMap<String, Vec<u8>>
                let mut result = HashMap::new();
                for (k, v) in map {
                    result.insert(k, v.to_vec());
                }
                Ok(result)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_currency")]
    fn py_load_currency(&self, code: &str) -> PyResult<Option<Currency>> {
        use ustr::Ustr;
        let code_ustr = Ustr::from(code);
        let rt = get_runtime();
        let result = rt.block_on(async {
            self.load_currency(&code_ustr).await
        });
        result.map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_currencies")]
    fn py_load_currencies(&self) -> PyResult<Vec<Currency>> {
        let rt = get_runtime();
        let result = rt.block_on(async {
            self.load_currencies().await
        });
        match result {
            Ok(map) => Ok(map.into_values().collect()),
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_instrument")]
    fn py_load_instrument(
        &self,
        py: Python,
        instrument_id: InstrumentId,
    ) -> PyResult<Option<PyObject>> {
        let rt = get_runtime();
        rt.block_on(async {
            let result = self.load_instrument(&instrument_id).await;
            match result {
                Ok(Some(instrument)) => {
                    let py_object = instrument_any_to_pyobject(py, instrument)?;
                    Ok(Some(py_object))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(to_pyruntime_err(e)),
            }
        })
    }

    #[pyo3(name = "load_instruments")]
    fn py_load_instruments(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let rt = get_runtime();
        rt.block_on(async {
            let result = self.load_instruments().await;
            match result {
                Ok(instruments) => {
                    let mut py_instruments = Vec::new();
                    for (_id, instrument) in instruments {
                        let py_object = instrument_any_to_pyobject(py, instrument)?;
                        py_instruments.push(py_object);
                    }
                    Ok(py_instruments)
                }
                Err(e) => Err(to_pyruntime_err(e)),
            }
        })
    }

    #[pyo3(name = "add_currency")]
    fn py_add_currency(&self, currency: Currency) -> PyResult<()> {
        self.add_currency(&currency).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_instrument")]
    fn py_add_instrument(&self, py: Python, instrument: PyObject) -> PyResult<()> {
        let instrument_any = pyobject_to_instrument_any(py, instrument)?;
        self.add_instrument(&instrument_any).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_account")]
    fn py_add_account(&self, py: Python, account: PyObject) -> PyResult<()> {
        let account_any = pyobject_to_account_any(py, account)?;
        self.add_account(&account_any).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_order")]
    fn py_add_order(
        &self,
        py: Python,
        order: PyObject,
        client_id: Option<ClientId>,
    ) -> PyResult<()> {
        let order_any = pyobject_to_order_any(py, order)?;
        self.add_order(&order_any, client_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_quote")]
    fn py_add_quote(&self, quote: QuoteTick) -> PyResult<()> {
        self.add_quote(&quote).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_trade")]
    fn py_add_trade(&self, trade: TradeTick) -> PyResult<()> {
        self.add_trade(&trade).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_bar")]
    fn py_add_bar(&self, bar: Bar) -> PyResult<()> {
        self.add_bar(&bar).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_indicator")]
    fn py_add_indicator(
        &self,
        instrument_id: String,
        indicator_name: String,
        indicator_type: String,
        bar_type_str: String,
        timestamp: i64,
        value: Option<f64>,
        values_json: Option<String>,
    ) -> PyResult<()> {
        // Parse instrument_id and bar_type from strings
        let instrument_id_parsed = InstrumentId::from_str(&instrument_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid instrument_id: {}", e)))?;
        
        let bar_type = BarType::from_str(&bar_type_str)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid bar_type: {}", e)))?;
        
        self.add_indicator(
            &instrument_id_parsed,
            &indicator_name,
            &indicator_type,
            &bar_type,
            timestamp,
            value,
            values_json,
        ).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_signal")]
    fn py_add_signal(&self, signal: Signal) -> PyResult<()> {
        self.add_signal(&signal).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_custom_data")]
    fn py_add_custom_data(&self, data: CustomData) -> PyResult<()> {
        self.add_custom_data(&data).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "update_account")]
    fn py_update_account(&self, py: Python, account: PyObject) -> PyResult<()> {
        let account_any = pyobject_to_account_any(py, account)?;
        self.update_account(&account_any).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "update_order")]
    fn py_update_order(&self, py: Python, event: PyObject) -> PyResult<()> {
        let order_event = pyobject_to_order_event(py, event)?;
        self.update_order(&order_event).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_quotes")]
    fn py_load_quotes(&self, instrument_id: InstrumentId) -> PyResult<Vec<QuoteTick>> {
        // load_quotes is synchronous in the trait
        self.load_quotes(&instrument_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_trades")]
    fn py_load_trades(&self, instrument_id: InstrumentId) -> PyResult<Vec<TradeTick>> {
        // load_trades is synchronous in the trait
        self.load_trades(&instrument_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_bars")]
    fn py_load_bars(&self, instrument_id: InstrumentId) -> PyResult<Vec<Bar>> {
        // load_bars is synchronous in the trait
        self.load_bars(&instrument_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_signals")]
    fn py_load_signals(&self, name: &str) -> PyResult<Vec<Signal>> {
        self.load_signals(name).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_all")]
    fn py_load_all(&self, py: Python) -> PyResult<PyObject> {
        // Use get_runtime() to handle existing runtime context
        let rt = get_runtime();
        
        let result = rt.block_on(async {
            self.load_all().await
        });
        
        match result {
            Ok(cache_map) => {
                // Convert CacheMap to Python dict with expected structure
                let mut result_dict = pyo3::types::PyDict::new(py);
                
                // Convert currencies: HashMap<Ustr, Currency> -> dict[str, Currency]
                let mut currencies_dict = pyo3::types::PyDict::new(py);
                for (code, currency) in cache_map.currencies {
                    currencies_dict.set_item(code.as_str(), currency.into_py(py))?;
                }
                let currencies_len = currencies_dict.len();
                result_dict.set_item("currencies", currencies_dict)?;
                
                // Convert instruments: HashMap<InstrumentId, InstrumentAny> -> dict[str, PyObject]
                let mut instruments_dict = pyo3::types::PyDict::new(py);
                for (id, instrument) in cache_map.instruments {
                    let py_object = instrument_any_to_pyobject(py, instrument)?;
                    instruments_dict.set_item(id.to_string(), py_object)?;
                }
                result_dict.set_item("instruments", instruments_dict)?;
                
                // Convert accounts: HashMap<AccountId, AccountAny> -> dict[str, PyObject]
                let mut accounts_dict = pyo3::types::PyDict::new(py);
                for (id, account) in cache_map.accounts {
                    let py_object = account_any_to_pyobject(py, account)?;
                    accounts_dict.set_item(id.to_string(), py_object)?;
                }
                result_dict.set_item("accounts", accounts_dict)?;
                
                // Convert orders: HashMap<ClientOrderId, OrderAny> -> dict[str, PyObject]
                let mut orders_dict = pyo3::types::PyDict::new(py);
                for (id, order) in cache_map.orders {
                    let py_object = order_any_to_pyobject(py, order)?;
                    orders_dict.set_item(id.to_string(), py_object)?;
                }
                result_dict.set_item("orders", orders_dict)?;
                
                // Convert positions: HashMap<PositionId, Position> -> dict[str, PyObject]
                let mut positions_dict = pyo3::types::PyDict::new(py);
                for (id, position) in cache_map.positions {
                    let py_object = position.into_py_any(py)?;
                    positions_dict.set_item(id.to_string(), py_object)?;
                }
                result_dict.set_item("positions", positions_dict)?;
                
                // Convert synthetics: HashMap<InstrumentId, SyntheticInstrument> -> dict[str, PyObject]
                let mut synthetics_dict = pyo3::types::PyDict::new(py);
                for (id, synthetic) in cache_map.synthetics {
                    let py_object = synthetic.into_py(py);
                    synthetics_dict.set_item(id.to_string(), py_object)?;
                }
                result_dict.set_item("synthetics", synthetics_dict)?;
                
                let py_result = result_dict.into();
                Ok(py_result)
            }
            Err(e) => {
                Err(to_pyruntime_err(e))
            }
        }
    }

    #[pyo3(name = "load_synthetics")]
    fn py_load_synthetics(&self, py: Python) -> PyResult<HashMap<String, PyObject>> {
        let rt = get_runtime();
        let result = rt.block_on(async {
            self.load_synthetics().await
        });
        match result {
            Ok(synthetics) => {
                let mut py_synthetics = HashMap::new();
                for (id, synthetic) in synthetics {
                    let py_object = synthetic.into_py(py);
                    py_synthetics.insert(id.to_string(), py_object);
                }
                Ok(py_synthetics)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_synthetic")]
    fn py_load_synthetic(
        &self,
        py: Python,
        instrument_id: InstrumentId,
    ) -> PyResult<Option<PyObject>> {
        let rt = get_runtime();
        rt.block_on(async {
            let result = self.load_synthetic(&instrument_id).await;
            match result {
                Ok(Some(synthetic)) => {
                    let py_object = synthetic.into_py(py);
                    Ok(Some(py_object))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(to_pyruntime_err(e)),
            }
        })
    }

    #[pyo3(name = "load_accounts")]
    fn py_load_accounts(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let rt = get_runtime();
        let result = rt.block_on(async {
            self.load_accounts().await
        });
        match result {
            Ok(accounts) => {
                let mut py_accounts = Vec::new();
                for (_id, account) in accounts {
                    let py_object = account_any_to_pyobject(py, account)?;
                    py_accounts.push(py_object);
                }
                Ok(py_accounts)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_account")]
    fn py_load_account(&self, py: Python, account_id: AccountId) -> PyResult<Option<PyObject>> {
        let rt = get_runtime();
        rt.block_on(async {
            let result = self.load_account(&account_id).await;
            match result {
                Ok(Some(account)) => {
                    let py_object = account_any_to_pyobject(py, account)?;
                    Ok(Some(py_object))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(to_pyruntime_err(e)),
            }
        })
    }

    #[pyo3(name = "load_orders")]
    fn py_load_orders(&self, py: Python) -> PyResult<HashMap<String, PyObject>> {
        let rt = get_runtime();
        let result = rt.block_on(async {
            self.load_orders().await
        });
        match result {
            Ok(orders) => {
                let mut py_orders = HashMap::new();
                for (id, order) in orders {
                    let py_object = order_any_to_pyobject(py, order)?;
                    py_orders.insert(id.to_string(), py_object);
                }
                Ok(py_orders)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_order")]
    fn py_load_order(
        &self,
        py: Python,
        client_order_id: ClientOrderId,
    ) -> PyResult<Option<PyObject>> {
        let rt = get_runtime();
        rt.block_on(async {
            let result = self.load_order(&client_order_id).await;
            match result {
                Ok(Some(order)) => {
                    let py_object = order_any_to_pyobject(py, order)?;
                    Ok(Some(py_object))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(to_pyruntime_err(e)),
            }
        })
    }

    #[pyo3(name = "load_positions")]
    fn py_load_positions(&self, py: Python) -> PyResult<HashMap<String, PyObject>> {
        let rt = get_runtime();
        let result = rt.block_on(async {
            self.load_positions().await
        });
        match result {
            Ok(positions) => {
                let mut py_positions = HashMap::new();
                for (id, position) in positions {
                    let py_object = position.into_py_any(py)?;
                    py_positions.insert(id.to_string(), py_object);
                }
                Ok(py_positions)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_position")]
    fn py_load_position(
        &self,
        py: Python,
        position_id: PositionId,
    ) -> PyResult<Option<PyObject>> {
        let rt = get_runtime();
        rt.block_on(async {
            let result = self.load_position(&position_id).await;
            match result {
                Ok(Some(position)) => {
                    let py_object = position.into_py_any(py)?;
                    Ok(Some(py_object))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(to_pyruntime_err(e)),
            }
        })
    }

    #[pyo3(name = "load_index_order_position")]
    fn py_load_index_order_position(&self, py: Python) -> PyResult<HashMap<String, PyObject>> {
        let result = self.load_index_order_position();
        match result {
            Ok(index) => {
                let mut py_index = HashMap::new();
                for (order_id, position) in index {
                    let py_object = position.into_py_any(py)?;
                    py_index.insert(order_id.to_string(), py_object);
                }
                Ok(py_index)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_index_order_client")]
    fn py_load_index_order_client(&self) -> PyResult<HashMap<String, String>> {
        let result = self.load_index_order_client();
        match result {
            Ok(index) => {
                let mut py_index = HashMap::new();
                for (order_id, client_id) in index {
                    py_index.insert(order_id.to_string(), client_id.to_string());
                }
                Ok(py_index)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_actor")]
    fn py_load_actor(&self, component_id: ComponentId) -> PyResult<HashMap<String, Vec<u8>>> {
        let result = self.load_actor(&component_id);
        match result {
            Ok(map) => {
                let mut result = HashMap::new();
                for (k, v) in map {
                    result.insert(k, v.to_vec());
                }
                Ok(result)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_strategy")]
    fn py_load_strategy(&self, strategy_id: StrategyId) -> PyResult<HashMap<String, Vec<u8>>> {
        let result = self.load_strategy(&strategy_id);
        match result {
            Ok(map) => {
                let mut result = HashMap::new();
                for (k, v) in map {
                    result.insert(k, v.to_vec());
                }
                Ok(result)
            }
            Err(e) => Err(to_pyruntime_err(e)),
        }
    }

    #[pyo3(name = "load_custom_data")]
    fn py_load_custom_data(&self, data_type: DataType) -> PyResult<Vec<CustomData>> {
        self.load_custom_data(&data_type).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_order_snapshot")]
    fn py_load_order_snapshot(
        &self,
        client_order_id: ClientOrderId,
    ) -> PyResult<Option<OrderSnapshot>> {
        self.load_order_snapshot(&client_order_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "load_position_snapshot")]
    fn py_load_position_snapshot(
        &self,
        position_id: PositionId,
    ) -> PyResult<Option<PositionSnapshot>> {
        self.load_position_snapshot(&position_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add")]
    fn py_add(&self, key: String, value: Vec<u8>) -> PyResult<()> {
        self.add(key, Bytes::from(value)).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_synthetic")]
    fn py_add_synthetic(&self, py: Python, synthetic: PyObject) -> PyResult<()> {
        use nautilus_model::instruments::SyntheticInstrument;
        // Extract SyntheticInstrument directly from PyObject
        let synthetic_obj = synthetic.extract::<SyntheticInstrument>(py)?;
        self.add_synthetic(&synthetic_obj).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_position")]
    fn py_add_position(&self, py: Python, position: PyObject) -> PyResult<()> {
        let position_obj = position.extract::<Position>(py)?;
        self.add_position(&position_obj).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_order_snapshot")]
    fn py_add_order_snapshot(&self, snapshot: OrderSnapshot) -> PyResult<()> {
        self.add_order_snapshot(&snapshot).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_position_snapshot")]
    fn py_add_position_snapshot(&self, snapshot: PositionSnapshot) -> PyResult<()> {
        self.add_position_snapshot(&snapshot).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "add_order_book")]
    fn py_add_order_book(&self, py: Python, order_book: PyObject) -> PyResult<()> {
        let order_book_obj = order_book.extract::<nautilus_model::orderbook::OrderBook>(py)?;
        self.add_order_book(&order_book_obj).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "delete_actor")]
    fn py_delete_actor(&self, component_id: ComponentId) -> PyResult<()> {
        self.delete_actor(&component_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "delete_strategy")]
    fn py_delete_strategy(&self, strategy_id: StrategyId) -> PyResult<()> {
        self.delete_strategy(&strategy_id).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "index_venue_order_id")]
    fn py_index_venue_order_id(
        &self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> PyResult<()> {
        self.index_venue_order_id(client_order_id, venue_order_id)
            .map_err(to_pyruntime_err)
    }

    #[pyo3(name = "index_order_position")]
    fn py_index_order_position(
        &self,
        client_order_id: ClientOrderId,
        position_id: PositionId,
    ) -> PyResult<()> {
        self.index_order_position(client_order_id, position_id)
            .map_err(to_pyruntime_err)
    }

    #[pyo3(name = "update_actor")]
    fn py_update_actor(&self) -> PyResult<()> {
        self.update_actor().map_err(to_pyruntime_err)
    }

    #[pyo3(name = "update_strategy")]
    fn py_update_strategy(&self) -> PyResult<()> {
        self.update_strategy().map_err(to_pyruntime_err)
    }

    #[pyo3(name = "update_position")]
    fn py_update_position(&self, py: Python, position: PyObject) -> PyResult<()> {
        let position_obj = position.extract::<Position>(py)?;
        self.update_position(&position_obj).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "snapshot_order_state")]
    fn py_snapshot_order_state(&self, py: Python, order: PyObject) -> PyResult<()> {
        let order_any = pyobject_to_order_any(py, order)?;
        self.snapshot_order_state(&order_any).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "snapshot_position_state")]
    fn py_snapshot_position_state(&self, py: Python, position: PyObject) -> PyResult<()> {
        let position_obj = position.extract::<Position>(py)?;
        self.snapshot_position_state(&position_obj).map_err(to_pyruntime_err)
    }

    #[pyo3(name = "heartbeat")]
    fn py_heartbeat(&self, timestamp: i64) -> PyResult<()> {
        use nautilus_core::UnixNanos;
        // UnixNanos implements From<u64>, not From<i64>
        let timestamp_u64 = if timestamp < 0 { 0 } else { timestamp as u64 };
        self.heartbeat(UnixNanos::from(timestamp_u64)).map_err(to_pyruntime_err)
    }

}


