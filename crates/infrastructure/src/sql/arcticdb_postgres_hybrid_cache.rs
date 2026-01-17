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

//! GreptimeDB-PostgreSQL Hybrid Cache Database Adapter
//!
//! Implements CacheDatabaseAdapter trait with hybrid storage:
//! - PostgreSQL (fincept_terminal schema) for relational data
//! - GreptimeDB for time series data (quotes, trades, bars, signals)

use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

use bytes::Bytes;
use nautilus_common::{
    cache::database::{CacheDatabaseAdapter, CacheMap},
    custom::CustomData,
    runtime::get_runtime,
    signal::Signal,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    accounts::AccountAny,
    data::{Bar, DataType, QuoteTick, TradeTick},
    events::{OrderEventAny, OrderSnapshot, position::snapshot::PositionSnapshot},
    identifiers::{
        AccountId, ClientId, ClientOrderId, ComponentId, InstrumentId, PositionId, StrategyId,
        VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, SyntheticInstrument},
    orderbook::OrderBook,
    orders::{Order, OrderAny},
    position::Position,
    types::Currency,
};
use sqlx::{PgPool, postgres::PgConnectOptions};
use tokio::try_join;
use ustr::Ustr;
use uuid::Uuid;

use crate::sql::{
    pg::{connect_pg, get_postgres_connect_options},
    queries::DatabaseQueries,
};

/// Configuration for FinceptTerminal hybrid cache
#[derive(Debug, Clone)]
pub struct GreptimePostgresHybridCacheConfig {
    // PostgreSQL configuration (fincept_terminal schema)
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_username: String,
    pub postgres_password: String,
    pub postgres_database: String,
    
    // User context for multi-user isolation
    pub user_id: Uuid,
    
    // GreptimeDB configuration (for time series data)
    pub greptime_host: String,              // Default: "localhost"
    pub greptime_port: u16,                 // Default: 4000 for HTTP API (4001 is gRPC)
    pub greptime_database: String,           // Default: "nautilus_timeseries"
    pub greptime_username: Option<String>,
    pub greptime_password: Option<String>,
    
    // Performance tuning
    pub buffer_interval_ms: u64,
    pub batch_size: usize,
}

impl Default for GreptimePostgresHybridCacheConfig {
    fn default() -> Self {
        Self {
            postgres_host: "localhost".to_string(),
            postgres_port: 5432,
            postgres_username: "postgres".to_string(),
            postgres_password: "password".to_string(),
            postgres_database: "fincept_terminal".to_string(),
            user_id: Uuid::new_v4(),
            
            // GreptimeDB configuration (defaults)
            greptime_host: "localhost".to_string(),
            greptime_port: 4000,
            greptime_database: "nautilus_timeseries".to_string(),
            greptime_username: None,
            greptime_password: None,
            
            buffer_interval_ms: 100,
            batch_size: 1000,
        }
    }
}

impl GreptimePostgresHybridCacheConfig {
    /// Create configuration with GreptimeDB connection settings
    pub fn new(
        postgres_url: &str,
        user_id: Uuid,
        greptime_host: Option<String>,
        greptime_port: Option<u16>,
        greptime_database: Option<String>,
    ) -> Self {
        Self {
            postgres_host: "localhost".to_string(), // TODO: Parse from postgres_url
            postgres_port: 5432,
            postgres_username: "postgres".to_string(),
            postgres_password: "password".to_string(),
            postgres_database: "fincept_terminal".to_string(),
            user_id,
            
            greptime_host: greptime_host.unwrap_or_else(|| "localhost".to_string()),
            greptime_port: greptime_port.unwrap_or(4000),
            greptime_database: greptime_database.unwrap_or_else(|| "nautilus_timeseries".to_string()),
            greptime_username: None,
            greptime_password: None,
            
            buffer_interval_ms: 100,
            batch_size: 1000,
        }
    }
}

/// Hybrid database query enum for routing to appropriate storage
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum GreptimePostgresHybridQuery {
    Close,
    
    // PostgreSQL queries (relational data)
    Add(String, Vec<u8>),
    AddCurrency(Currency),
    AddInstrument(InstrumentAny, i32), // Include exchange_id
    AddAccount(AccountAny, bool),
    AddOrder(OrderAny, Option<ClientId>, bool),
    AddOrderSnapshot(OrderSnapshot),
    AddPositionSnapshot(PositionSnapshot),
    UpdateOrder(OrderEventAny),
    
    // GreptimeDB queries (time series data)
    AddQuote(QuoteTick, i32), // Include exchange_id
    AddTrade(TradeTick, i32),  // Include exchange_id
    AddBar(Bar, i32),          // Include exchange_id
    AddSignal(Signal, i32),    // Include exchange_id
    AddCustom(CustomData),
}

/// FinceptTerminal Hybrid Cache Database Adapter
/// 
/// Implements CacheDatabaseAdapter with hybrid storage strategy:
/// - PostgreSQL for relational data using fincept_terminal schema
/// - GreptimeDB for time series data with user/exchange isolation
#[derive(Debug)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.infrastructure")
)]
pub struct GreptimePostgresHybridCacheAdapter {
    // PostgreSQL connection for relational data
    pub pg_pool: PgPool,
    
    // Message handling for database operations
    tx: tokio::sync::mpsc::UnboundedSender<GreptimePostgresHybridQuery>,
    handle: tokio::task::JoinHandle<()>,
    
    // Configuration including user context
    config: GreptimePostgresHybridCacheConfig,
}

impl GreptimePostgresHybridCacheAdapter {
    /// Create new hybrid cache adapter with user context and configuration
    pub async fn connect(config: GreptimePostgresHybridCacheConfig) -> Result<Self, sqlx::Error> {
        // Connect to PostgreSQL using fincept_terminal schema
        let pg_connect_options = get_postgres_connect_options(
            Some(config.postgres_host.clone()),
            Some(config.postgres_port),
            Some(config.postgres_username.clone()),
            Some(config.postgres_password.clone()),
            Some(config.postgres_database.clone()),
        );
        
        let pg_pool = connect_pg(pg_connect_options.clone().into()).await?;
        
        // Set up schema search path to fincept_terminal
        sqlx::query("SET search_path TO fincept_terminal, public")
            .execute(&pg_pool)
            .await?;
        
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<GreptimePostgresHybridQuery>();
        
        // Spawn task to handle database operations
        let config_clone = config.clone();
        let handle = tokio::spawn(async move {
            Self::process_hybrid_commands(rx, pg_connect_options.into(), config_clone).await;
        });
        
        Ok(Self {
            pg_pool,
            tx,
            handle,
            config,
        })
    }
    
    /// Process hybrid database commands, routing to PostgreSQL or GreptimeDB
    async fn process_hybrid_commands(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<GreptimePostgresHybridQuery>,
        pg_connect_options: PgConnectOptions,
        config: GreptimePostgresHybridCacheConfig,
    ) {
        tracing::debug!("Starting FinceptTerminal hybrid cache processing for user {}", config.user_id);
        
        let pg_pool = connect_pg(pg_connect_options).await.unwrap();
        
        // Set schema search path
        if let Err(e) = sqlx::query("SET search_path TO fincept_terminal, public")
            .execute(&pg_pool)
            .await
        {
            tracing::error!("Failed to set schema search path: {}", e);
        }
        
        // Initialize GreptimeDB connection
        #[cfg(feature = "greptime")]
        let greptime_client = match crate::sql::greptime_client::GreptimeClient::new(
            &config.greptime_host,
            config.greptime_port,
            &config.greptime_database,
            config.greptime_username.clone(),
            config.greptime_password.clone(),
        ).await {
            Ok(client) => Some(client),
            Err(e) => {
                tracing::error!("Failed to connect to GreptimeDB: {e}. Time-series operations will use PostgreSQL fallback.");
                None
            }
        };
        
        #[cfg(not(feature = "greptime"))]
        let greptime_client: Option<()> = None;
        
        // Buffering for batch operations
        let mut buffer: VecDeque<GreptimePostgresHybridQuery> = VecDeque::new();
        let mut last_drain = Instant::now();
        let buffer_interval = Duration::from_millis(config.buffer_interval_ms);
        
        // Process commands
        loop {
            if last_drain.elapsed() >= buffer_interval && !buffer.is_empty() {
                Self::drain_hybrid_buffer(&pg_pool, &mut buffer, &config).await;
                last_drain = Instant::now();
            } else {
                match rx.recv().await {
                    Some(cmd) => {
                        tracing::debug!("Received hybrid command: {:?}", cmd);
                        match cmd {
                            GreptimePostgresHybridQuery::Close => break,
                            _ => buffer.push_back(cmd),
                        }
                    }
                    None => {
                        tracing::debug!("Hybrid command channel closed");
                        break;
                    }
                }
            }
        }
        
        // Drain remaining commands
        if !buffer.is_empty() {
            Self::drain_hybrid_buffer(&pg_pool, &mut buffer, &config).await;
        }
        
        tracing::debug!("Stopped FinceptTerminal hybrid cache processing");
    }
    
    /// Drain buffer with hybrid routing logic
    async fn drain_hybrid_buffer(
        pg_pool: &PgPool,
        buffer: &mut VecDeque<GreptimePostgresHybridQuery>,
        config: &GreptimePostgresHybridCacheConfig,
    ) {
        for cmd in buffer.drain(..) {
            let result: anyhow::Result<()> = match cmd {
                GreptimePostgresHybridQuery::Close => Ok(()),
                
                // PostgreSQL operations (relational data)
                GreptimePostgresHybridQuery::Add(key, value) => {
                    Self::add_general_to_postgres(pg_pool, config.user_id, key, value).await
                }
                GreptimePostgresHybridQuery::AddCurrency(currency) => {
                    Self::add_currency_to_postgres(pg_pool, currency).await
                }
                GreptimePostgresHybridQuery::AddInstrument(instrument, exchange_id) => {
                    Self::add_instrument_to_postgres(pg_pool, config.user_id, instrument, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddAccount(account, updated) => {
                    Self::add_account_to_postgres(pg_pool, config.user_id, account, updated).await
                }
                GreptimePostgresHybridQuery::AddOrder(order, client_id, updated) => {
                    Self::add_order_to_postgres(pg_pool, config.user_id, order, client_id, updated).await
                }
                GreptimePostgresHybridQuery::UpdateOrder(event) => {
                    Self::update_order_in_postgres(pg_pool, config.user_id, event).await
                }
                
                // Time series operations (route to GreptimeDB when available)
                GreptimePostgresHybridQuery::AddQuote(quote, exchange_id) => {
                    Self::add_quote_hybrid(pg_pool, config, quote, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddTrade(trade, exchange_id) => {
                    Self::add_trade_hybrid(pg_pool, config, trade, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddBar(bar, exchange_id) => {
                    Self::add_bar_hybrid(pg_pool, config, bar, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddSignal(signal, exchange_id) => {
                    Self::add_signal_hybrid(pg_pool, config, signal, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddCustom(data) => {
                    Self::add_custom_data_hybrid(pg_pool, config, data).await
                }
                
                _ => Ok(()), // Handle other cases
            };
            
            if let Err(e) = result {
                tracing::error!("Error executing hybrid command: {e:?}");
            }
        }
    }
    
    // ==================== POSTGRESQL OPERATIONS ====================
    
    async fn add_currency_to_postgres(pg_pool: &PgPool, currency: Currency) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO nautilus_currencies (id, precision, name, currency_type)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                precision = EXCLUDED.precision,
                name = EXCLUDED.name,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(currency.code.to_string())
        .bind(currency.precision as i32)
        .bind(currency.code.to_string())
        .bind("CRYPTO") // Default classification
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_instrument_to_postgres(
        pg_pool: &PgPool, 
        _user_id: Uuid, 
        instrument: InstrumentAny, 
        exchange_id: i32
    ) -> anyhow::Result<()> {
        let instrument_id = instrument.id().to_string();
        let base_currency = instrument.base_currency().map(|c| c.code.to_string());
        let quote_currency = instrument.quote_currency().map(|c| c.code.to_string());
        let ts_event = chrono::Utc::now().timestamp_nanos();
        
        sqlx::query(
            r#"
            INSERT INTO nautilus_instruments 
            (id, exchange_id, raw_symbol, base_currency, quote_currency,
             price_precision, size_precision, price_increment, margin_init, 
             margin_maint, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (exchange_id, base_currency, quote_currency) DO UPDATE SET
                raw_symbol = EXCLUDED.raw_symbol,
                price_precision = EXCLUDED.price_precision,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(instrument_id)
        .bind(exchange_id)
        .bind(instrument.raw_symbol().to_string())
        .bind(base_currency)
        .bind(quote_currency)
        .bind(instrument.price_precision() as i32)
        .bind(instrument.size_precision().map(|p| p as i32))
        .bind(instrument.price_increment().to_string())
        .bind("0") // Default margin
        .bind("0") // Default margin
        .bind(ts_event.to_string())
        .bind(ts_event.to_string())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    // ==================== TIME SERIES OPERATIONS (HYBRID) ====================
    
    async fn add_quote_hybrid(
        pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        quote: QuoteTick,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_quote_table_name(config.user_id, exchange_id);
        
        // Try to write to GreptimeDB first
        #[cfg(feature = "greptime")]
        {
            if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                &config.greptime_host,
                config.greptime_port,
                &config.greptime_database,
                config.greptime_username.clone(),
                config.greptime_password.clone(),
            ).await {
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(quote.ts_event.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(quote.instrument_id.to_string()))
                    .with_column("bid_price", GreptimeValue::Float64(quote.bid_price.as_f64()))
                    .with_column("ask_price", GreptimeValue::Float64(quote.ask_price.as_f64()))
                    .with_column("bid_size", GreptimeValue::Float64(quote.bid_size.as_f64()))
                    .with_column("ask_size", GreptimeValue::Float64(quote.ask_size.as_f64()));
                
                if let Err(e) = client.insert(&table_name, row).await {
                    tracing::warn!("Failed to write quote to GreptimeDB: {e}, falling back to PostgreSQL");
                } else {
                    return Ok(()); // Successfully written to GreptimeDB
                }
            }
        }
        
        // Fallback to PostgreSQL
        sqlx::query(
            r#"
            INSERT INTO nautilus_quote_ticks 
            (user_id, exchange_id, instrument_id, bid_price, ask_price, 
             bid_size, ask_size, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(config.user_id)
        .bind(exchange_id)
        .bind(quote.instrument_id.to_string())
        .bind(quote.bid_price.as_f64())
        .bind(quote.ask_price.as_f64())
        .bind(quote.bid_size.as_f64())
        .bind(quote.ask_size.as_f64())
        .bind(quote.ts_event.as_i64())
        .bind(quote.ts_init.as_i64())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_trade_hybrid(
        pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        trade: TradeTick,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_trade_table_name(config.user_id, exchange_id);
        
        // Try to write to GreptimeDB first
        #[cfg(feature = "greptime")]
        {
            if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                &config.greptime_host,
                config.greptime_port,
                &config.greptime_database,
                config.greptime_username.clone(),
                config.greptime_password.clone(),
            ).await {
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(trade.ts_event.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(trade.instrument_id.to_string()))
                    .with_column("price", GreptimeValue::Float64(trade.price.as_f64()))
                    .with_column("quantity", GreptimeValue::Float64(trade.size.as_f64()))
                    .with_column("aggressor_side", GreptimeValue::String(trade.aggressor_side.to_string()))
                    .with_column("trade_id", GreptimeValue::String(trade.trade_id.map(|id| id.to_string()).unwrap_or_default()));
                
                if let Err(e) = client.insert(&table_name, row).await {
                    tracing::warn!("Failed to write trade to GreptimeDB: {e}, falling back to PostgreSQL");
                } else {
                    return Ok(()); // Successfully written to GreptimeDB
                }
            }
        }
        
        // Fallback to PostgreSQL
        sqlx::query(
            r#"
            INSERT INTO nautilus_trade_ticks 
            (user_id, exchange_id, instrument_id, price, quantity, 
             aggressor_side, trade_id, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#
        )
        .bind(config.user_id)
        .bind(exchange_id)
        .bind(trade.instrument_id.to_string())
        .bind(trade.price.as_f64())
        .bind(trade.size.as_f64())
        .bind(trade.aggressor_side.to_string())
        .bind(trade.trade_id.map(|id| id.to_string()))
        .bind(trade.ts_event.as_i64())
        .bind(trade.ts_init.as_i64())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_bar_hybrid(
        pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        bar: Bar,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_bar_table_name(config.user_id, exchange_id);
        
        // Try to write to GreptimeDB first
        #[cfg(feature = "greptime")]
        {
            if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                &config.greptime_host,
                config.greptime_port,
                &config.greptime_database,
                config.greptime_username.clone(),
                config.greptime_password.clone(),
            ).await {
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(bar.ts_event.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(bar.instrument_id.to_string()))
                    .with_column("bar_type", GreptimeValue::String(bar.bar_type.to_string()))
                    .with_column("open", GreptimeValue::Float64(bar.open.as_f64()))
                    .with_column("high", GreptimeValue::Float64(bar.high.as_f64()))
                    .with_column("low", GreptimeValue::Float64(bar.low.as_f64()))
                    .with_column("close", GreptimeValue::Float64(bar.close.as_f64()))
                    .with_column("volume", GreptimeValue::Float64(bar.volume.as_f64()));
                
                if let Err(e) = client.insert(&table_name, row).await {
                    tracing::warn!("Failed to write bar to GreptimeDB: {e}, falling back to PostgreSQL");
                } else {
                    return Ok(()); // Successfully written to GreptimeDB
                }
            }
        }
        
        // Fallback to PostgreSQL
        sqlx::query(
            r#"
            INSERT INTO nautilus_bars 
            (user_id, exchange_id, instrument_id, bar_type, step, price_type,
             aggregation_source, open_price, high_price, low_price, close_price,
             volume, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (user_id, exchange_id, instrument_id, bar_type, step, ts_event) DO NOTHING
            "#
        )
        .bind(config.user_id)
        .bind(exchange_id)
        .bind(bar.instrument_id.to_string())
        .bind(bar.bar_type.to_string())
        .bind(1) // TODO: Extract step from bar specification
        .bind("MID") // TODO: Extract from bar
        .bind("EXTERNAL") // TODO: Extract from bar
        .bind(bar.open.as_f64())
        .bind(bar.high.as_f64())
        .bind(bar.low.as_f64())
        .bind(bar.close.as_f64())
        .bind(bar.volume.as_f64())
        .bind(bar.ts_event.as_i64())
        .bind(bar.ts_init.as_i64())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_signal_hybrid(
        pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        signal: Signal,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_signal_table_name(config.user_id, exchange_id);
        
        // Try to write to GreptimeDB first
        #[cfg(feature = "greptime")]
        {
            if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                &config.greptime_host,
                config.greptime_port,
                &config.greptime_database,
                config.greptime_username.clone(),
                config.greptime_password.clone(),
            ).await {
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(signal.ts_event.as_i64()))
                    .with_column("signal_name", GreptimeValue::String(signal.name.to_string()))
                    .with_column("signal_value", GreptimeValue::Float64(signal.value.as_f64()));
                
                if let Err(e) = client.insert(&table_name, row).await {
                    tracing::warn!("Failed to write signal to GreptimeDB: {e}, falling back to PostgreSQL");
                } else {
                    return Ok(()); // Successfully written to GreptimeDB
                }
            }
        }
        
        // Fallback to PostgreSQL
        sqlx::query(
            r#"
            INSERT INTO nautilus_signals 
            (user_id, exchange_id, signal_name, signal_value, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#
        )
        .bind(config.user_id)
        .bind(exchange_id)
        .bind(signal.name.to_string())
        .bind(signal.value.as_f64())
        .bind(signal.ts_event.as_i64())
        .bind(signal.ts_init.as_i64())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    // ==================== GREPTIMEDB TABLE NAME GENERATION ====================
    
    fn get_quote_table_name(user_id: Uuid, exchange_id: i32) -> String {
        format!("user_{}_quotes_exchange_{}", user_id, exchange_id)
    }
    
    fn get_trade_table_name(user_id: Uuid, exchange_id: i32) -> String {
        format!("user_{}_trades_exchange_{}", user_id, exchange_id)
    }
    
    fn get_bar_table_name(user_id: Uuid, exchange_id: i32) -> String {
        format!("user_{}_bars_exchange_{}", user_id, exchange_id)
    }
    
    fn get_signal_table_name(user_id: Uuid, exchange_id: i32) -> String {
        format!("user_{}_signals_exchange_{}", user_id, exchange_id)
    }
    
    // Placeholder implementations for remaining PostgreSQL operations
    async fn add_general_to_postgres(_pg_pool: &PgPool, _user_id: Uuid, _key: String, _value: Vec<u8>) -> anyhow::Result<()> {
        // TODO: Implement general cache storage
        Ok(())
    }
    
    async fn add_account_to_postgres(_pg_pool: &PgPool, _user_id: Uuid, _account: AccountAny, _updated: bool) -> anyhow::Result<()> {
        // TODO: Implement account storage using nautilus_accounts table
        Ok(())
    }
    
    async fn add_order_to_postgres(_pg_pool: &PgPool, _user_id: Uuid, _order: OrderAny, _client_id: Option<ClientId>, _updated: bool) -> anyhow::Result<()> {
        // TODO: Implement order storage using nautilus_orders table
        Ok(())
    }
    
    async fn update_order_in_postgres(_pg_pool: &PgPool, _user_id: Uuid, _event: OrderEventAny) -> anyhow::Result<()> {
        // TODO: Implement order event storage using nautilus_order_events table
        Ok(())
    }
    
    async fn add_custom_data_hybrid(
        pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        data: CustomData,
    ) -> anyhow::Result<()> {
        // Try to write to GreptimeDB first
        #[cfg(feature = "greptime")]
        {
            if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                &config.greptime_host,
                config.greptime_port,
                &config.greptime_database,
                config.greptime_username.clone(),
                config.greptime_password.clone(),
            ).await {
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let table_name = format!("user_{}_custom_data", config.user_id);
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(data.ts_event.as_i64()))
                    .with_column("data_type", GreptimeValue::String(data.data_type.to_string()))
                    .with_column("value", GreptimeValue::String(serde_json::to_string(&data)?));
                
                if let Err(e) = client.insert(&table_name, row).await {
                    tracing::warn!("Failed to write custom data to GreptimeDB: {e}, falling back to PostgreSQL");
                } else {
                    return Ok(()); // Successfully written to GreptimeDB
                }
            }
        }
        
        // Fallback to PostgreSQL (if custom data table exists)
        // For now, just log that custom data was received
        tracing::debug!("Custom data received but PostgreSQL storage not yet implemented");
        Ok(())
    }
}

#[async_trait::async_trait]
impl CacheDatabaseAdapter for GreptimePostgresHybridCacheAdapter {
    fn close(&mut self) -> anyhow::Result<()> {
        let pool = self.pg_pool.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        log::debug!("Closing FinceptTerminal hybrid cache connection pool");
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                pool.close().await;
                if let Err(e) = tx.send(()) {
                    log::error!("Error closing pool: {e:?}");
                }
            });
        });

        // Cancel message handling task
        if let Err(e) = self.tx.send(GreptimePostgresHybridQuery::Close) {
            log::error!("Error sending close: {e:?}");
        }

        log::debug!("Awaiting hybrid cache task");
        tokio::task::block_in_place(|| {
            if let Err(e) = get_runtime().block_on(&mut self.handle) {
                log::error!("Error awaiting hybrid cache task: {e:?}");
            }
        });

        log::debug!("FinceptTerminal hybrid cache closed");
        Ok(rx.recv()?)
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        // Flush PostgreSQL buffer (handled by connection pool)
        // GreptimeDB writes are immediate via gRPC, no buffering needed
        Ok(())
    }

    async fn load_all(&self) -> anyhow::Result<CacheMap> {
        // Load relational data from PostgreSQL using fincept_terminal schema
        let (currencies, instruments, accounts, orders) = try_join!(
            self.load_currencies(),
            self.load_instruments(),
            self.load_accounts(),
            self.load_orders(),
        )?;

        Ok(CacheMap {
            currencies,
            instruments,
            synthetics: HashMap::new(), // TODO: Implement synthetics
            accounts,
            orders,
            positions: HashMap::new(), // TODO: Implement positions
        })
    }

    fn load(&self) -> anyhow::Result<HashMap<String, Bytes>> {
        // TODO: Load general cache items from nautilus_general table
        Ok(HashMap::new())
    }

    async fn load_currencies(&self) -> anyhow::Result<HashMap<Ustr, Currency>> {
        let rows = sqlx::query(
            "SELECT id, precision FROM nautilus_currencies ORDER BY id"
        )
        .fetch_all(&self.pg_pool)
        .await?;
        
        let mut currencies = HashMap::new();
        for row in rows {
            let code: String = row.get("id");
            let precision: i32 = row.get("precision");
            
            if let Ok(currency) = Currency::from_str_checked(&code, precision as u8) {
                currencies.insert(currency.code, currency);
            }
        }
        
        Ok(currencies)
    }

    async fn load_instruments(&self) -> anyhow::Result<HashMap<InstrumentId, InstrumentAny>> {
        // TODO: Load instruments from nautilus_instruments table
        // This requires mapping back to Nautilus InstrumentAny types
        Ok(HashMap::new())
    }

    async fn load_synthetics(&self) -> anyhow::Result<HashMap<InstrumentId, SyntheticInstrument>> {
        // TODO: Implement synthetic instruments if needed
        Ok(HashMap::new())
    }

    async fn load_accounts(&self) -> anyhow::Result<HashMap<AccountId, AccountAny>> {
        // TODO: Load accounts from nautilus_accounts table for this user
        Ok(HashMap::new())
    }

    async fn load_orders(&self) -> anyhow::Result<HashMap<ClientOrderId, OrderAny>> {
        // TODO: Load orders from nautilus_orders table for this user
        Ok(HashMap::new())
    }

    async fn load_positions(&self) -> anyhow::Result<HashMap<PositionId, Position>> {
        // TODO: Load positions from nautilus_positions table for this user
        Ok(HashMap::new())
    }

    // All the other required trait methods with delegation to appropriate storage
    fn load_index_order_position(&self) -> anyhow::Result<HashMap<ClientOrderId, Position>> { Ok(HashMap::new()) }
    fn load_index_order_client(&self) -> anyhow::Result<HashMap<ClientOrderId, ClientId>> { Ok(HashMap::new()) }
    
    async fn load_currency(&self, code: &Ustr) -> anyhow::Result<Option<Currency>> {
        let row = sqlx::query("SELECT precision FROM nautilus_currencies WHERE id = $1")
            .bind(code.as_str())
            .fetch_optional(&self.pg_pool)
            .await?;
            
        if let Some(row) = row {
            let precision: i32 = row.get("precision");
            Ok(Some(Currency::from_str_checked(code.as_str(), precision as u8)?))
        } else {
            Ok(None)
        }
    }

    async fn load_instrument(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Option<InstrumentAny>> { Ok(None) }
    async fn load_synthetic(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Option<SyntheticInstrument>> { Ok(None) }
    async fn load_account(&self, _account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> { Ok(None) }
    async fn load_order(&self, _client_order_id: &ClientOrderId) -> anyhow::Result<Option<OrderAny>> { Ok(None) }
    async fn load_position(&self, _position_id: &PositionId) -> anyhow::Result<Option<Position>> { Ok(None) }
    
    fn load_actor(&self, _component_id: &ComponentId) -> anyhow::Result<HashMap<String, Bytes>> { Ok(HashMap::new()) }
    fn delete_actor(&self, _component_id: &ComponentId) -> anyhow::Result<()> { Ok(()) }
    fn load_strategy(&self, _strategy_id: &StrategyId) -> anyhow::Result<HashMap<String, Bytes>> { Ok(HashMap::new()) }
    fn delete_strategy(&self, _component_id: &StrategyId) -> anyhow::Result<()> { Ok(()) }

    // Add methods - route to appropriate storage
    fn add(&self, key: String, value: Bytes) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::Add(key, value.into());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add query: {e}"))
    }

    fn add_currency(&self, currency: &Currency) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddCurrency(*currency);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_currency query: {e}"))
    }

    fn add_instrument(&self, instrument: &InstrumentAny) -> anyhow::Result<()> {
        // TODO: Determine exchange_id from instrument or context
        let exchange_id = 1; // Default to first exchange for now
        let query = GreptimePostgresHybridQuery::AddInstrument(instrument.clone(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_instrument query: {e}"))
    }

    fn add_synthetic(&self, _synthetic: &SyntheticInstrument) -> anyhow::Result<()> { Ok(()) }

    fn add_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddAccount(account.clone(), false);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_account query: {e}"))
    }

    fn add_order(&self, order: &OrderAny, client_id: Option<ClientId>) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddOrder(order.clone(), client_id, false);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_order query: {e}"))
    }

    fn add_order_snapshot(&self, snapshot: &OrderSnapshot) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddOrderSnapshot(snapshot.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_order_snapshot query: {e}"))
    }

    fn add_position(&self, _position: &Position) -> anyhow::Result<()> { Ok(()) }
    fn add_position_snapshot(&self, snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddPositionSnapshot(snapshot.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_position_snapshot query: {e}"))
    }

    fn add_order_book(&self, _order_book: &OrderBook) -> anyhow::Result<()> { Ok(()) }

    // Time series methods - route to hybrid storage
    fn add_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        // TODO: Determine exchange_id from quote context
        let exchange_id = 1; // Default for now
        let query = GreptimePostgresHybridQuery::AddQuote(quote.to_owned(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_quote query: {e}"))
    }

    fn load_quotes(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        // TODO: Load from PostgreSQL + GreptimeDB
        Ok(Vec::new())
    }

    fn add_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        let exchange_id = 1; // TODO: Determine from context
        let query = GreptimePostgresHybridQuery::AddTrade(trade.to_owned(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_trade query: {e}"))
    }

    fn load_trades(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        Ok(Vec::new())
    }

    fn add_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        let exchange_id = 1; // TODO: Determine from context
        let query = GreptimePostgresHybridQuery::AddBar(bar.to_owned(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_bar query: {e}"))
    }

    fn load_bars(&self, _instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        Ok(Vec::new())
    }

    fn add_signal(&self, signal: &Signal) -> anyhow::Result<()> {
        let exchange_id = 1; // TODO: Determine from context
        let query = GreptimePostgresHybridQuery::AddSignal(signal.to_owned(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_signal query: {e}"))
    }

    fn load_signals(&self, _name: &str) -> anyhow::Result<Vec<Signal>> {
        Ok(Vec::new())
    }

    fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddCustom(data.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_custom_data query: {e}"))
    }

    fn load_custom_data(&self, _data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        Ok(Vec::new())
    }

    // Snapshot methods
    fn load_order_snapshot(&self, _client_order_id: &ClientOrderId) -> anyhow::Result<Option<OrderSnapshot>> { Ok(None) }
    fn load_position_snapshot(&self, _position_id: &PositionId) -> anyhow::Result<Option<PositionSnapshot>> { Ok(None) }

    // Index methods
    fn index_venue_order_id(&self, _client_order_id: ClientOrderId, _venue_order_id: VenueOrderId) -> anyhow::Result<()> { Ok(()) }
    fn index_order_position(&self, _client_order_id: ClientOrderId, _position_id: PositionId) -> anyhow::Result<()> { Ok(()) }

    // Update methods
    fn update_actor(&self) -> anyhow::Result<()> { Ok(()) }
    fn update_strategy(&self) -> anyhow::Result<()> { Ok(()) }

    fn update_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddAccount(account.clone(), true);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send update_account query: {e}"))
    }

    fn update_order(&self, event: &OrderEventAny) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::UpdateOrder(event.clone());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send update_order query: {e}"))
    }

    fn update_position(&self, _position: &Position) -> anyhow::Result<()> { Ok(()) }

    // Snapshot methods
    fn snapshot_order_state(&self, _order: &OrderAny) -> anyhow::Result<()> { Ok(()) }
    fn snapshot_position_state(&self, _position: &Position) -> anyhow::Result<()> { Ok(()) }

    fn heartbeat(&self, _timestamp: UnixNanos) -> anyhow::Result<()> { Ok(()) }
}