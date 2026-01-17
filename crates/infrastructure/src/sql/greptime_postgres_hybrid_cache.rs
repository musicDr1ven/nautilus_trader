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
    data::{bar::BarType, Bar, DataType, QuoteTick, TradeTick},
    events::{account::state::AccountState, OrderEvent, OrderEventAny, OrderSnapshot, position::snapshot::PositionSnapshot},
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
use std::str::FromStr;
use sqlx::{PgPool, postgres::PgConnectOptions, Row};
use tokio::try_join;
use ustr::Ustr;
use uuid::Uuid;

use crate::sql::{
    models::instruments::InstrumentAnyModel,
    pg::{connect_pg, get_postgres_connect_options},
};

#[cfg(feature = "greptime")]
use crate::sql::greptime_client::GreptimeClient;

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
    
    // Cached trader_id for this user (reused across all operations)
    pub trader_id: Option<String>,
    
    // Cached account_id for this backtest (reused across all operations)
    // Same account_id is used throughout the backtest
    pub account_id: Option<String>,
    
    // Cached strategy_id for this backtest (reused across all operations)
    // Each run gets a new strategy_id (UUID string) - this is the PRIMARY KEY in nautilus_strategies table
    pub strategy_id: Option<String>,
    
    // Position counter for generating unique position IDs within a strategy run
    // Only incremented when enable_multiple_trades is true
    pub position_counter: u32,
    
    // Mapping from Nautilus position ID to our generated position ID
    // Used to ensure updates reuse the same position ID
    pub position_id_mapping: std::collections::HashMap<String, String>,
    
    // Whether multiple positions are allowed per strategy run
    // Defaults to false - all positions use the same ID ({strategy_id}_1)
    pub enable_multiple_trades: bool,
    
    // GreptimeDB configuration (for time series data)
    pub greptime_host: String,              // Default: "localhost"
    pub greptime_port: u16,                 // Default: 4000 for HTTP API (4001 is gRPC)
    pub greptime_database: String,           // Default: "nautilus_timeseries"
    pub greptime_username: Option<String>,
    pub greptime_password: Option<String>,
    
    // Performance tuning
    pub buffer_interval_ms: u64,
    pub batch_size: usize,
    
    // Trading mode: "backtest", "paper", or "live"
    pub trading_mode: String,  // Default: "backtest"
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
            trader_id: None,
            account_id: None,
            strategy_id: None,
            position_counter: 0,
            position_id_mapping: std::collections::HashMap::new(),
            enable_multiple_trades: false, // Default to false - single position per run
            
            // GreptimeDB configuration (defaults)
            greptime_host: "localhost".to_string(),
            greptime_port: 4000,
            greptime_database: "nautilus_timeseries".to_string(),
            greptime_username: None,
            greptime_password: None,
            
            buffer_interval_ms: 100,
            batch_size: 1000,
            trading_mode: "backtest".to_string(),  // Default to backtest
        }
    }
}

impl GreptimePostgresHybridCacheConfig {
    /// Parse PostgreSQL URL into components
    /// Format: postgresql://user:pass@host:port/db or postgres://user:pass@host:port/db
    fn parse_postgres_url(url: &str) -> (String, u16, String, String, String) {
        // Default values
        let mut host = "localhost".to_string();
        let mut port = 5432;
        let mut username = "postgres".to_string();
        let mut password = "password".to_string();
        let mut database = "fincept_terminal".to_string();
        
        // Remove postgresql:// or postgres:// prefix
        let url = url.strip_prefix("postgresql://")
            .or_else(|| url.strip_prefix("postgres://"))
            .unwrap_or(url);
        
        // Split on @ to separate credentials from host/db
        if let Some(at_pos) = url.find('@') {
            let creds = &url[..at_pos];
            let host_db = &url[at_pos + 1..];
            
            // Parse credentials (user:pass)
            if let Some(colon_pos) = creds.find(':') {
                username = creds[..colon_pos].to_string();
                password = creds[colon_pos + 1..].to_string();
            } else {
                username = creds.to_string();
            }
            
            // Parse host:port/db
            if let Some(slash_pos) = host_db.find('/') {
                let host_port = &host_db[..slash_pos];
                database = host_db[slash_pos + 1..].to_string();
                
                // Parse host:port
                if let Some(colon_pos) = host_port.find(':') {
                    host = host_port[..colon_pos].to_string();
                    if let Ok(p) = host_port[colon_pos + 1..].parse::<u16>() {
                        port = p;
                    }
                } else {
                    host = host_port.to_string();
                }
            } else {
                host = host_db.to_string();
            }
        } else {
            // No credentials, just host:port/db or host/db
            if let Some(slash_pos) = url.find('/') {
                let host_port = &url[..slash_pos];
                database = url[slash_pos + 1..].to_string();
                
                if let Some(colon_pos) = host_port.find(':') {
                    host = host_port[..colon_pos].to_string();
                    if let Ok(p) = host_port[colon_pos + 1..].parse::<u16>() {
                        port = p;
                    }
                } else {
                    host = host_port.to_string();
                }
            } else {
                host = url.to_string();
            }
        }
        
        (host, port, username, password, database)
    }
    
    /// Create configuration with GreptimeDB connection settings
    pub fn new(
        postgres_url: &str,
        user_id: Uuid,
        greptime_host: Option<String>,
        greptime_port: Option<u16>,
        greptime_database: Option<String>,
    ) -> Self {
        // Parse postgres_url (format: postgresql://user:pass@host:port/db)
        let (host, port, username, password, database) = Self::parse_postgres_url(postgres_url);
        
        Self {
            postgres_host: host,
            postgres_port: port,
            postgres_username: username,
            postgres_password: password,
            postgres_database: database,
            user_id,
            trader_id: None,
            account_id: None,
            strategy_id: None,
            position_counter: 0,
            position_id_mapping: std::collections::HashMap::new(),
            enable_multiple_trades: false, // Default to false - single position per run
            
            greptime_host: greptime_host.unwrap_or_else(|| "localhost".to_string()),
            greptime_port: greptime_port.unwrap_or(4000),
            greptime_database: greptime_database.unwrap_or_else(|| "nautilus_timeseries".to_string()),
            greptime_username: None,
            greptime_password: None,
            
            buffer_interval_ms: 100,
            batch_size: 1000,
            trading_mode: "backtest".to_string(),  // Default to backtest
        }
    }
    
    /// Create configuration from environment variables
    /// 
    /// Reads the following environment variables (from API project's .env file):
    /// - `GREPTIME_HOST` (default: "localhost")
    /// - `GREPTIME_PORT` (default: 4000 for HTTP API, 4001 is gRPC)
    /// - `GREPTIME_DATABASE` (default: "nautilus_timeseries")
    /// - `GREPTIME_USERNAME` (optional)
    /// - `GREPTIME_PASSWORD` (optional)
    /// - `GREPTIME_WORKING_DIR` (for WAL/temp files, used by setup script)
    /// - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE`
    /// 
    /// Note: This assumes dotenvy::dotenv() has been called (typically in main.rs)
    /// which loads from apps/hydra-terminal-api/.env
    pub fn from_env(user_id: Uuid) -> Self {
        Self {
            postgres_host: std::env::var("POSTGRES_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),
            postgres_port: std::env::var("POSTGRES_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            postgres_username: std::env::var("POSTGRES_USERNAME")
                .unwrap_or_else(|_| "postgres".to_string()),
            postgres_password: std::env::var("POSTGRES_PASSWORD")
                .unwrap_or_else(|_| "password".to_string()),
            postgres_database: std::env::var("POSTGRES_DATABASE")
                .unwrap_or_else(|_| "fincept_terminal".to_string()),
            user_id,
            trader_id: None,
            account_id: None,
            strategy_id: None,
            position_counter: 0,
            position_id_mapping: std::collections::HashMap::new(),
            enable_multiple_trades: false, // Default to false - single position per run
            
            greptime_host: std::env::var("GREPTIME_HOST")
                .unwrap_or_else(|_| "localhost".to_string()),
            greptime_port: std::env::var("GREPTIME_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(4000),
            greptime_database: std::env::var("GREPTIME_DATABASE")
                .unwrap_or_else(|_| "nautilus_timeseries".to_string()),
            greptime_username: std::env::var("GREPTIME_USERNAME").ok(),
            greptime_password: std::env::var("GREPTIME_PASSWORD").ok(),
            
            buffer_interval_ms: std::env::var("GREPTIME_BUFFER_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            batch_size: std::env::var("GREPTIME_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            trading_mode: std::env::var("TRADING_MODE")
                .unwrap_or_else(|_| "backtest".to_string()),  // Default to backtest
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
    AddSynthetic(SyntheticInstrument),
    AddAccount(AccountAny, bool),
    AddOrder(OrderAny, Option<ClientId>, bool),
    AddOrderSnapshot(OrderSnapshot),
    AddPosition(Position),
    AddPositionSnapshot(PositionSnapshot),
    AddOrderBook(OrderBook, i32), // Include exchange_id
    UpdateOrder(OrderEventAny),
    UpdatePosition(Position),
    UpdateActor(ComponentId, HashMap<String, Bytes>),
    UpdateStrategy(StrategyId, HashMap<String, Bytes>),
    SnapshotOrderState(OrderAny),
    SnapshotPositionState(Position),
    IndexVenueOrderId(ClientOrderId, VenueOrderId),
    IndexOrderPosition(ClientOrderId, PositionId),
    
    // GreptimeDB queries (time series data)
    AddQuote(QuoteTick, i32), // Include exchange_id
    AddTrade(TradeTick, i32),  // Include exchange_id
    AddBar(Bar, i32),          // Include exchange_id
    AddSignal(Signal, i32),    // Include exchange_id
    AddCustom(CustomData),
    AddIndicator {
        instrument_id: InstrumentId,
        indicator_name: String,  // api_name from strategy config
        indicator_type: String,
        bar_type: BarType,
        timestamp: i64,
        value: Option<f64>,  // Single value (None for multi-value)
        values_json: Option<String>,  // JSON string for multi-value (None for single-value)
        exchange_id: i32,
        strategy_id: Option<String>,  // UUID of the strategy_run
    },
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
    
    // Flush synchronization (using mpsc for multiple flush signals)
    flush_tx: tokio::sync::mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>,
    
    // Configuration including user context
    pub config: GreptimePostgresHybridCacheConfig,
}

impl GreptimePostgresHybridCacheAdapter {
    /// Helper function to create a new PostgreSQL connection pool within a runtime
    /// This is necessary because PgPool cannot be used across different runtimes
    async fn create_pool_in_runtime(config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<PgPool> {
        // Verify we have a runtime handle before calling connect_pg
        use tokio::runtime::Handle;
        let handle_result = Handle::try_current();
        
        if handle_result.is_err() {
            return Err(anyhow::anyhow!("No Tokio runtime handle available. This function must be called from within a Tokio runtime context."));
        }
        
        let pg_connect_options = get_postgres_connect_options(
            Some(config.postgres_host.clone()),
            Some(config.postgres_port),
            Some(config.postgres_username.clone()),
            Some(config.postgres_password.clone()),
            Some(config.postgres_database.clone()),
        );
        
        let pool = connect_pg(pg_connect_options.clone().into()).await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;
        
        // Set schema search path
        sqlx::query("SET search_path TO fincept_terminal, public")
            .execute(&pool)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to set schema: {}", e))?;
        
        Ok(pool)
    }
    
    /// Helper methods that accept a pool parameter to avoid cross-runtime pool usage
    async fn load_currencies_with_pool(pool: &PgPool, _config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<HashMap<Ustr, Currency>> {
        let rows = sqlx::query(
            "SELECT id, precision FROM fincept_terminal.nautilus_currencies ORDER BY id"
        )
        .fetch_all(pool)
        .await?;
        
        let mut currencies = HashMap::new();
        for row in &rows {
            let code: String = row.get("id");
            let precision: i32 = row.get("precision");
            
            if let Ok(mut currency) = Currency::from_str(&code) {
                currency.precision = precision as u8;
                currencies.insert(currency.code, currency);
            }
        }
        
        Ok(currencies)
    }
    
    async fn load_instruments_with_pool(pool: &PgPool, _config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<HashMap<InstrumentId, InstrumentAny>> {
        tracing::info!("load_instruments_with_pool: Querying nautilus_instruments table");
        
        // Query instruments from fincept_terminal schema using InstrumentAnyModel
        let result = sqlx::query_as::<_, InstrumentAnyModel>(
            "SELECT * FROM fincept_terminal.nautilus_instruments WHERE exchange_id IN (SELECT id FROM fincept_terminal.nautilus_exchanges WHERE is_active = TRUE)"
        )
        .fetch_all(pool)
        .await;
        
        match result {
            Ok(instruments) => {
                tracing::info!("load_instruments_with_pool: Successfully loaded {} instruments from database", instruments.len());
                
                let map: HashMap<InstrumentId, InstrumentAny> = instruments
                    .into_iter()
                    .map(|model| {
                        let instrument = model.0;
                        tracing::debug!("Loaded instrument: {}", instrument.id());
                        (instrument.id(), instrument)
                    })
                    .collect();
                    
                Ok(map)
            }
            Err(e) => {
                tracing::error!("load_instruments_with_pool: Database query failed: {:?}", e);
                tracing::error!("load_instruments_with_pool: This is likely due to missing fields in nautilus_instruments table");
                tracing::error!("load_instruments_with_pool: Ensure migration 016 was applied and 'kind' field is populated");
                // Return empty instead of error to allow backtest to continue
                // Instruments will be added as data is loaded
                Ok(HashMap::new())
            }
        }
    }
    
    async fn load_accounts_with_pool(pool: &PgPool, config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<HashMap<AccountId, AccountAny>> {
        // Load all accounts for this user from nautilus_accounts table
        // Use account_state column directly (added in migration 023)
        let rows = sqlx::query(
            r#"
            SELECT id, account_state
            FROM fincept_terminal.nautilus_accounts
            WHERE user_id = $1 AND account_state IS NOT NULL
            "#
        )
        .bind(config.user_id)
        .fetch_all(pool)
        .await?;
        
        let mut accounts = HashMap::new();
        for row in rows {
            let account_id_str: String = row.get("id");
            let account_state: Option<serde_json::Value> = row.get("account_state");
            
            if let Some(state_json) = account_state {
                // Try to deserialize as AccountAny
                match serde_json::from_value::<AccountAny>(state_json) {
                    Ok(account) => {
                        let account_id = account.id();
                        accounts.insert(account_id, account);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to deserialize account_state for {}: {}", account_id_str, e);
                    }
                }
            }
        }
        
        tracing::debug!("load_accounts: loaded {} accounts for user {}", accounts.len(), config.user_id);
        Ok(accounts)
    }
    
    async fn load_orders_with_pool(pool: &PgPool, config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<HashMap<ClientOrderId, OrderAny>> {
        // Load orders from nautilus_orders table
        // Note: Full OrderAny reconstruction from database fields is complex and would require
        // reconstructing from order events. For now, we query the database but note that
        // full reconstruction may not be possible without event replay.
        // TODO: Consider storing full order state as JSONB (similar to account_state) for easier reconstruction
        let rows = sqlx::query(
            r#"
            SELECT id, client_order_id, instrument_id, order_type, order_side, status, quantity, price, time_in_force
            FROM fincept_terminal.nautilus_orders
            WHERE user_id = $1
            "#
        )
        .bind(config.user_id)
        .fetch_all(pool)
        .await?;
        
        tracing::debug!("load_orders: loaded {} order records (OrderAny reconstruction not yet fully implemented - requires event replay)", rows.len());
        
        // TODO: Reconstruct OrderAny from database rows
        // This requires:
        // 1. Loading order events from nautilus_order_events
        // 2. Reconstructing the order state from events
        // 3. Building the full OrderAny enum variant
        // For now, return empty - full implementation would require significant event reconstruction logic
        Ok(HashMap::new())
    }
    
    async fn load_synthetics_with_pool(pool: &PgPool, config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<HashMap<InstrumentId, SyntheticInstrument>> {
        // Load synthetic instruments from nautilus_general table
        let rows = sqlx::query(
            r#"
            SELECT id, value FROM fincept_terminal.nautilus_general
            WHERE user_id = $1 AND id LIKE 'synthetic_%'
            "#
        )
        .bind(config.user_id)
        .fetch_all(pool)
        .await?;
        
        let mut synthetics = HashMap::new();
        for row in rows {
            let key: String = row.get("id");
            let value: Vec<u8> = row.get("value");
            
            // Try to deserialize JSON
            if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&value) {
                // Try to deserialize as SyntheticInstrument
                if let Ok(synthetic) = serde_json::from_value::<SyntheticInstrument>(json_value.clone()) {
                    synthetics.insert(synthetic.id, synthetic);
                } else {
                    tracing::warn!("Failed to deserialize synthetic instrument from key: {}", key);
                }
            }
        }
        
        Ok(synthetics)
    }
    
    async fn load_positions_with_pool(pool: &PgPool, config: &GreptimePostgresHybridCacheConfig) -> anyhow::Result<HashMap<PositionId, Position>> {
        // Load positions from nautilus_positions table
        // Note: Position reconstruction is complex and requires many fields
        // TODO: Consider storing full position state as JSONB for easier reconstruction
        let rows = sqlx::query(
            r#"
            SELECT id, instrument_id, side, quantity, entry, signed_qty, 
                   avg_px_open, avg_px_close, realized_pnl, unrealized_pnl,
                   quote_currency, base_currency, settlement_currency, ts_opened, ts_init
            FROM fincept_terminal.nautilus_positions
            WHERE user_id = $1
            "#
        )
        .bind(config.user_id)
        .fetch_all(pool)
        .await?;
        
        tracing::debug!("load_positions: loaded {} position records (Position reconstruction not yet fully implemented - requires all fields and event reconstruction)", rows.len());
        
        // TODO: Reconstruct Position from database rows
        // This requires:
        // 1. Loading all position fields from database
        // 2. Reconstructing PositionId, InstrumentId, etc. from strings
        // 3. Building Money objects for PnL and balances
        // 4. Reconstructing position events if needed
        // For now, return empty - full implementation would require significant reconstruction logic
        Ok(HashMap::new())
    }
    
    /// Create new hybrid cache adapter with user context and configuration
    pub async fn connect(config: GreptimePostgresHybridCacheConfig) -> anyhow::Result<Self> {
        // Connect to PostgreSQL using fincept_terminal schema
        let pg_connect_options = get_postgres_connect_options(
            Some(config.postgres_host.clone()),
            Some(config.postgres_port),
            Some(config.postgres_username.clone()),
            Some(config.postgres_password.clone()),
            Some(config.postgres_database.clone()),
        );
        
        let pg_pool = connect_pg(pg_connect_options.clone().into()).await
            .map_err(|e| anyhow::anyhow!("Failed to connect to PostgreSQL: {}", e))?;
        
        // Set up schema search path to fincept_terminal
        sqlx::query("SET search_path TO fincept_terminal, public")
            .execute(&pg_pool)
            .await?;
        
        // Initialize trader_id once (reuse existing trader per user)
        // Note: user_id must already exist in fincept_terminal.users
        // If it doesn't, the foreign key constraint will fail with a clear error
        let trader_id = Self::ensure_trader_exists(&pg_pool, config.user_id, "default").await?;
        let config_with_trader = GreptimePostgresHybridCacheConfig {
            trader_id: Some(trader_id),
            ..config
        };
        
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<GreptimePostgresHybridQuery>();
        let (flush_tx, flush_rx) = tokio::sync::mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
        
        // Spawn task to handle database operations
        // Get the current runtime handle to spawn the task with explicit handle
        let runtime_handle = tokio::runtime::Handle::try_current()
            .map_err(|_| anyhow::anyhow!("No Tokio runtime handle available. connect() must be called from within a Tokio runtime context."))?;
        
        let config_clone = config_with_trader.clone();
        let handle = runtime_handle.spawn(async move {
            Self::process_hybrid_commands(rx, pg_connect_options.into(), config_clone, flush_rx).await;
        });
        
        Ok(Self {
            pg_pool,
            tx,
            handle,
            flush_tx,
            config: config_with_trader,
        })
    }
    
    /// Process hybrid database commands, routing to PostgreSQL or GreptimeDB
    async fn process_hybrid_commands(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<GreptimePostgresHybridQuery>,
        pg_connect_options: PgConnectOptions,
        mut config: GreptimePostgresHybridCacheConfig,
        mut flush_rx: tokio::sync::mpsc::UnboundedReceiver<tokio::sync::oneshot::Sender<()>>,
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
        
        // Initialize GreptimeDB connection - REQUIRED for time-series operations
        #[cfg(feature = "greptime")]
        let greptime_client = {
            match crate::sql::greptime_client::GreptimeClient::new(
                &config.greptime_host,
                config.greptime_port,
                &config.greptime_database,
                config.greptime_username.clone(),
                config.greptime_password.clone(),
            ).await {
                Ok(client) => {
                    Some(client)
                },
                Err(e) => {
                    tracing::error!("Failed to connect to GreptimeDB: {e}. GreptimeDB is REQUIRED for time-series operations.");
                    // Continue with None - operations will fail with clear error messages
                    None
                }
            }
        };
        
        #[cfg(not(feature = "greptime"))]
        let greptime_client: Option<()> = None;
        
        // Buffering for batch operations
        let mut buffer: VecDeque<GreptimePostgresHybridQuery> = VecDeque::new();
        let mut last_drain = Instant::now();
        let buffer_interval = Duration::from_millis(config.buffer_interval_ms);
        let mut greptime_batch_buffer: HashMap<String, Vec<GreptimePostgresHybridQuery>> = HashMap::new();
        
        // Process commands
        loop {
            // Check for flush signal
            if let Ok(flush_ack) = flush_rx.try_recv() {
                // Flush all buffers immediately
                Self::drain_hybrid_buffer(&pg_pool, &mut buffer, &mut config, &greptime_client).await;
                Self::flush_greptime_batches(&greptime_client, &mut greptime_batch_buffer, config.user_id, config.strategy_id.clone()).await;
                // Acknowledge flush completion
                let _ = flush_ack.send(());
                continue;
            }
            
            if last_drain.elapsed() >= buffer_interval && !buffer.is_empty() {
                Self::drain_hybrid_buffer(&pg_pool, &mut buffer, &mut config, &greptime_client).await;
                last_drain = Instant::now();
            } else if buffer.len() >= config.batch_size {
                // Flush when batch size is reached
                Self::drain_hybrid_buffer(&pg_pool, &mut buffer, &mut config, &greptime_client).await;
                last_drain = Instant::now();
            } else {
                match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                    Ok(Some(cmd)) => {
                        tracing::debug!("Received hybrid command: {:?}", cmd);
                        match cmd {
                            GreptimePostgresHybridQuery::Close => break,
                            _ => {
                                // Route time-series data to GreptimeDB batch buffer
                                if Self::is_timeseries_query(&cmd) {
                                    let table_name = Self::get_table_name_for_query(&cmd, &config.trading_mode);
                                    greptime_batch_buffer
                                        .entry(table_name.clone())
                                        .or_insert_with(Vec::new)
                                        .push(cmd);
                                    
                                    // Flush GreptimeDB batch if it reaches batch size
                                    if greptime_batch_buffer.get(&table_name).map(|v| v.len()).unwrap_or(0) >= config.batch_size {
                                        Self::flush_greptime_table_batch(&greptime_client, &table_name, &mut greptime_batch_buffer, config.user_id, config.strategy_id.clone()).await;
                                    }
                                } else {
                                    // Relational data goes to PostgreSQL buffer
                                    buffer.push_back(cmd);
                        }
                    }
                        }
                    }
                    Ok(None) => {
                        tracing::debug!("Hybrid command channel closed");
                        break;
                    }
                    Err(_) => {
                        // Timeout - continue loop to check buffer/flush conditions
                        continue;
                    }
                }
            }
        }
        
        // Drain remaining commands
        if !buffer.is_empty() {
            Self::drain_hybrid_buffer(&pg_pool, &mut buffer, &mut config, &greptime_client).await;
        }
        
        // Flush remaining GreptimeDB batches
        Self::flush_greptime_batches(&greptime_client, &mut greptime_batch_buffer, config.user_id, config.strategy_id.clone()).await;
        
        tracing::debug!("Stopped FinceptTerminal hybrid cache processing");
    }
    
    /// Check if query is time-series data (should go to GreptimeDB)
    fn is_timeseries_query(cmd: &GreptimePostgresHybridQuery) -> bool {
        matches!(
            cmd,
            GreptimePostgresHybridQuery::AddQuote(_, _)
                | GreptimePostgresHybridQuery::AddTrade(_, _)
                | GreptimePostgresHybridQuery::AddBar(_, _)
                | GreptimePostgresHybridQuery::AddSignal(_, _)
                | GreptimePostgresHybridQuery::AddCustom(_)
                | GreptimePostgresHybridQuery::AddIndicator { .. }
                | GreptimePostgresHybridQuery::AddOrderBook(_, _)
        )
    }
    
    /// Get table name for a query (for batching)
    fn get_table_name_for_query(cmd: &GreptimePostgresHybridQuery, trading_mode: &str) -> String {
        match cmd {
            GreptimePostgresHybridQuery::AddQuote(quote, _) => {
                let exchange_name = Self::extract_exchange_name(&quote.instrument_id);
                format!("quotes_{}", exchange_name)
            }
            GreptimePostgresHybridQuery::AddTrade(trade, _) => {
                let exchange_name = Self::extract_exchange_name(&trade.instrument_id);
                format!("trades_{}_{}", exchange_name, trading_mode)
            }
            GreptimePostgresHybridQuery::AddBar(bar, _) => {
                let exchange_name = Self::extract_exchange_name(&bar.bar_type.instrument_id());
                let period = Self::extract_bar_period(&bar.bar_type);
                format!("bars_{}_{}_{}", exchange_name, trading_mode, period)
            }
            GreptimePostgresHybridQuery::AddSignal(signal, _) => {
                // For signals, we need to extract exchange from signal context if available
                // For now, use a generic name - can be enhanced later
                "signals".to_string()
            }
            GreptimePostgresHybridQuery::AddCustom(_) => "custom_data".to_string(),
            GreptimePostgresHybridQuery::AddIndicator { bar_type, .. } => {
                let exchange_name = Self::extract_exchange_name(&bar_type.instrument_id());
                let period = Self::extract_bar_period(bar_type);
                format!("indicators_{}_{}_{}", exchange_name, trading_mode, period)
            }
            GreptimePostgresHybridQuery::AddOrderBook(_, exchange_id) => {
                // Note: user_id is not available in this context, but batching will use correct table name
                // The actual table name is generated in add_order_book_hybrid using get_order_book_table_name
                format!("order_books_exchange_{}", exchange_id)
            }
            _ => "unknown".to_string(),
        }
    }
    
    /// Extract exchange name from InstrumentId (e.g., "XDG/USD.KRAKEN" -> "kraken")
    fn extract_exchange_name(instrument_id: &InstrumentId) -> String {
        let instrument_str = instrument_id.to_string();
        // InstrumentId format: "SYMBOL.EXCHANGE" or "SYMBOL/QUOTE.EXCHANGE"
        // Extract the part after the last dot
        if let Some(dot_pos) = instrument_str.rfind('.') {
            let exchange = &instrument_str[dot_pos + 1..];
            exchange.to_lowercase()
        } else {
            // Fallback: use "unknown" if no exchange found
            "unknown".to_string()
        }
    }
    
    /// Extract bar period from BarType (e.g., "XDG/USD.KRAKEN-1-MINUTE-LAST-EXTERNAL" -> "1m")
    fn extract_bar_period(bar_type: &nautilus_model::data::bar::BarType) -> String {
        let bar_type_str = bar_type.to_string();
        // BarType string format: "INSTRUMENT-STEP-AGGREGATION-PRICE-SOURCE"
        // Example: "XDG/USD.KRAKEN-1-MINUTE-LAST-EXTERNAL"
        // Split by '-' to get: [INSTRUMENT, STEP, AGGREGATION, PRICE, SOURCE]
        let parts: Vec<&str> = bar_type_str.split('-').collect();
        
        if parts.len() >= 3 {
            // parts[0] = instrument (e.g., "XDG/USD.KRAKEN")
            // parts[1] = step (e.g., "1")
            // parts[2] = aggregation (e.g., "MINUTE")
            let step_str = parts[1];
            let agg_str = parts[2];
            
            // Parse step
            if let Ok(step) = step_str.parse::<u32>() {
                // Convert aggregation to suffix
                let suffix = match agg_str {
                    "MINUTE" => "m",
                    "HOUR" => "h",
                    "DAY" => "d",
                    "WEEK" => "w",
                    "MONTH" => "mo",
                    "YEAR" => "y",
                    _ => "u", // unknown
                };
                return format!("{}{}", step, suffix);
            }
        }
        
        // Fallback: try to find pattern like "-1-MINUTE" or "-5-MINUTE"
        if let Some(minute_pos) = bar_type_str.find("-MINUTE") {
            // Find the dash before MINUTE
            if minute_pos > 0 {
                let before_minute = &bar_type_str[..minute_pos];
                if let Some(dash_pos) = before_minute.rfind('-') {
                    let step_str = &before_minute[dash_pos + 1..];
                    if let Ok(step) = step_str.parse::<u32>() {
                        return format!("{}m", step);
                    }
                }
            }
        }
        
        // Ultimate fallback
        "unknown".to_string()
    }
    
    /// Flush GreptimeDB batches for a specific table
    async fn flush_greptime_table_batch(
        greptime_client: &Option<GreptimeClient>,
        table_name: &str,
        batch_buffer: &mut HashMap<String, Vec<GreptimePostgresHybridQuery>>,
        user_id: Uuid,
        strategy_id: Option<String>,
    ) {
        if let Some(queries) = batch_buffer.remove(table_name) {
            if queries.is_empty() {
                return;
            }
            
            #[cfg(feature = "greptime")]
            if let Some(client) = greptime_client {
                // Batch insert to GreptimeDB
                let mut rows = Vec::new();
                let mut failed_conversions = 0;
                for query in queries {
                    if let Some(row) = Self::query_to_greptime_row(&query, user_id, strategy_id.clone()) {
                        rows.push(row);
                    } else {
                        failed_conversions += 1;
                    }
                }
                
                if !rows.is_empty() {
                    let row_count = rows.len();
                    let result = client.insert_batch(table_name, rows).await;
                    if let Err(e) = result {
                        tracing::error!("Failed to flush GreptimeDB batch for {} ({} rows): {:?}", table_name, row_count, e);
                    } else {
                        tracing::debug!("Flushed {} rows to GreptimeDB table {}", row_count, table_name);
                    }
                } else if failed_conversions > 0 {
                    tracing::warn!("All {} conversions failed for table {}", failed_conversions, table_name);
                }
            }
        }
    }
    
    /// Flush all GreptimeDB batches
    async fn flush_greptime_batches(
        greptime_client: &Option<GreptimeClient>,
        batch_buffer: &mut HashMap<String, Vec<GreptimePostgresHybridQuery>>,
        user_id: Uuid,
        strategy_id: Option<String>,
    ) {
        let table_names: Vec<String> = batch_buffer.keys().cloned().collect();
        for table_name in table_names {
            Self::flush_greptime_table_batch(greptime_client, &table_name, batch_buffer, user_id, strategy_id.clone()).await;
        }
    }
    
    /// Convert query to GreptimeRow (helper for batch operations)
    #[cfg(feature = "greptime")]
    fn query_to_greptime_row(cmd: &GreptimePostgresHybridQuery, user_id: Uuid, strategy_id: Option<String>) -> Option<crate::sql::greptime_client::GreptimeRow> {
        use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
        
        match cmd {
            GreptimePostgresHybridQuery::AddQuote(quote, _) => {
                Some(GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(quote.ts_event.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(quote.instrument_id.to_string()))
                    .with_column("bid_price", GreptimeValue::Float64(quote.bid_price.as_f64()))
                    .with_column("ask_price", GreptimeValue::Float64(quote.ask_price.as_f64()))
                    .with_column("bid_size", GreptimeValue::Float64(quote.bid_size.as_f64()))
                    .with_column("ask_size", GreptimeValue::Float64(quote.ask_size.as_f64())))
            }
            GreptimePostgresHybridQuery::AddTrade(trade, _) => {
                Some(GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(trade.ts_event.as_i64()))
                    .with_column("user_id", GreptimeValue::String(user_id.to_string()))
                    .with_column("strategy_id", GreptimeValue::String(strategy_id.clone().unwrap_or_else(|| "".to_string())))
                    .with_column("instrument_id", GreptimeValue::String(trade.instrument_id.to_string()))
                    .with_column("price", GreptimeValue::Float64(trade.price.as_f64()))
                    .with_column("quantity", GreptimeValue::Float64(trade.size.as_f64()))
                    .with_column("aggressor_side", GreptimeValue::String(trade.aggressor_side.to_string()))
                    .with_column("trade_id", GreptimeValue::String(trade.trade_id.to_string())))
            }
            GreptimePostgresHybridQuery::AddBar(bar, _) => {
                Some(GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(bar.ts_event.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(bar.bar_type.instrument_id().to_string()))
                    .with_column("open", GreptimeValue::Float64(bar.open.as_f64()))
                    .with_column("high", GreptimeValue::Float64(bar.high.as_f64()))
                    .with_column("low", GreptimeValue::Float64(bar.low.as_f64()))
                    .with_column("close", GreptimeValue::Float64(bar.close.as_f64()))
                    .with_column("volume", GreptimeValue::Float64(bar.volume.as_f64())))
            }
            GreptimePostgresHybridQuery::AddIndicator { instrument_id, indicator_name, indicator_type, timestamp, value, values_json, .. } => {
                // CRITICAL FIX: Always include both value and values_json columns (one can be NULL)
                // GreptimeDB requires all rows in a batch to have the same columns
                let mut row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(*timestamp))
                    .with_column("instrument_id", GreptimeValue::String(instrument_id.to_string()))
                    .with_column("indicator_name", GreptimeValue::String(indicator_name.clone()))
                    .with_column("indicator_type", GreptimeValue::String(indicator_type.clone()));
                
                // Always add value column (NULL if not provided)
                if let Some(v) = value {
                    row = row.with_column("value", GreptimeValue::Float64(*v));
                } else {
                    // Use NULL for value when not provided (for multi-value indicators)
                    row = row.with_column("value", GreptimeValue::Null);
                }
                
                // Always add values_json column (NULL if not provided)
                if let Some(json_str) = values_json {
                    row = row.with_column("values_json", GreptimeValue::Json(json_str.clone()));
                } else {
                    // Use NULL for values_json when not provided (for single-value indicators)
                    row = row.with_column("values_json", GreptimeValue::Null);
                }
                
                Some(row)
            }
            GreptimePostgresHybridQuery::AddOrderBook(order_book, _) => {
                // Extract bids and asks from OrderBook
                let bids_vec: Vec<serde_json::Value> = order_book.bids(None)
                    .map(|level| {
                        serde_json::json!([
                            level.price.value.as_f64(),
                            level.size_decimal().to_string().parse::<f64>().unwrap_or(0.0)
                        ])
                    })
                    .collect();
                
                let asks_vec: Vec<serde_json::Value> = order_book.asks(None)
                    .map(|level| {
                        serde_json::json!([
                            level.price.value.as_f64(),
                            level.size_decimal().to_string().parse::<f64>().unwrap_or(0.0)
                        ])
                    })
                    .collect();
                
                let bids_json = serde_json::to_string(&bids_vec).unwrap_or_else(|_| "[]".to_string());
                let asks_json = serde_json::to_string(&asks_vec).unwrap_or_else(|_| "[]".to_string());
                
                Some(GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(order_book.ts_last.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(order_book.instrument_id.to_string()))
                    .with_column("book_type", GreptimeValue::String(format!("{:?}", order_book.book_type)))
                    .with_column("update_count", GreptimeValue::Int64(order_book.update_count as i64))
                    .with_column("bids", GreptimeValue::Json(bids_json))
                    .with_column("asks", GreptimeValue::Json(asks_json))
                    .with_column("ts_last", GreptimeValue::Timestamp(order_book.ts_last.as_i64())))
            }
            _ => None, // Other types can be added as needed
        }
    }
    
    #[cfg(not(feature = "greptime"))]
    fn query_to_greptime_row(_cmd: &GreptimePostgresHybridQuery) -> Option<()> {
        None
    }
    
    /// Drain buffer with hybrid routing logic
    async fn drain_hybrid_buffer(
        pg_pool: &PgPool,
        buffer: &mut VecDeque<GreptimePostgresHybridQuery>,
        config: &mut GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
    ) {
        for cmd in buffer.drain(..) {
            // Capture command type before match (since match may move values)
            let cmd_type = format!("{:?}", std::mem::discriminant(&cmd));
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
                    Self::add_account_to_postgres(pg_pool, config.user_id, account, updated, config).await
                }
                GreptimePostgresHybridQuery::AddOrder(order, client_id, updated) => {
                    Self::add_order_to_postgres(pg_pool, config.user_id, order, client_id, updated, config).await
                }
                GreptimePostgresHybridQuery::AddOrderSnapshot(snapshot) => {
                    Self::add_order_snapshot_to_postgres(pg_pool, config.user_id, snapshot).await
                }
                GreptimePostgresHybridQuery::AddSynthetic(synthetic) => {
                    Self::add_synthetic_to_postgres(pg_pool, config.user_id, synthetic).await
                }
                GreptimePostgresHybridQuery::AddPosition(position) => {
                    Self::add_position_to_postgres(pg_pool, config.user_id, position, config).await
                }
                GreptimePostgresHybridQuery::AddPositionSnapshot(snapshot) => {
                    Self::add_position_snapshot_to_postgres(pg_pool, config.user_id, snapshot, config).await
                }
                GreptimePostgresHybridQuery::AddOrderBook(order_book, exchange_id) => {
                    Self::add_order_book_hybrid(pg_pool, config, &greptime_client, order_book, exchange_id).await
                }
                GreptimePostgresHybridQuery::UpdateOrder(event) => {
                    Self::update_order_in_postgres(pg_pool, config.user_id, event, config).await
                }
                GreptimePostgresHybridQuery::UpdatePosition(position) => {
                    Self::update_position_in_postgres(pg_pool, config.user_id, position, config).await
                }
                GreptimePostgresHybridQuery::UpdateActor(component_id, data) => {
                    Self::update_actor_in_postgres(pg_pool, config.user_id, component_id, data).await
                }
                GreptimePostgresHybridQuery::UpdateStrategy(strategy_id, data) => {
                    Self::update_strategy_in_postgres(pg_pool, config.user_id, strategy_id, data, config).await
                }
                GreptimePostgresHybridQuery::SnapshotOrderState(order) => {
                    Self::snapshot_order_state_in_postgres(pg_pool, config.user_id, order, config).await
                }
                GreptimePostgresHybridQuery::SnapshotPositionState(position) => {
                    Self::snapshot_position_state_in_postgres(pg_pool, config.user_id, position, config).await
                }
                GreptimePostgresHybridQuery::IndexVenueOrderId(client_order_id, venue_order_id) => {
                    Self::index_venue_order_id_in_postgres(pg_pool, config.user_id, client_order_id, venue_order_id).await
                }
                GreptimePostgresHybridQuery::IndexOrderPosition(client_order_id, position_id) => {
                    Self::index_order_position_in_postgres(pg_pool, config.user_id, client_order_id, position_id).await
                }
                
                // Time series operations (REQUIRE GreptimeDB - no PostgreSQL fallback)
                GreptimePostgresHybridQuery::AddQuote(quote, exchange_id) => {
                    Self::add_quote_hybrid(pg_pool, config, &greptime_client, quote, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddTrade(trade, exchange_id) => {
                    Self::add_trade_hybrid(pg_pool, config, &greptime_client, trade, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddBar(bar, exchange_id) => {
                    Self::add_bar_hybrid(pg_pool, config, &greptime_client, bar, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddSignal(signal, exchange_id) => {
                    Self::add_signal_hybrid(pg_pool, config, &greptime_client, signal, exchange_id).await
                }
                GreptimePostgresHybridQuery::AddCustom(data) => {
                    Self::add_custom_data_hybrid(pg_pool, config, &greptime_client, data).await
                }
                GreptimePostgresHybridQuery::AddIndicator { instrument_id, indicator_name, indicator_type, bar_type, timestamp, value, values_json, exchange_id, strategy_id } => {
                    Self::add_indicator_hybrid(pg_pool, config, &greptime_client, instrument_id, indicator_name, indicator_type, bar_type, timestamp, value, values_json, exchange_id, strategy_id).await
                }
                
                _ => Ok(()), // Handle other cases
            };
            
            if let Err(e) = result {
                tracing::error!("Error executing hybrid command: {e:?}");
            }
            // Removed success logging to reduce log file size - only log errors
        }
    }
    
    // ==================== POSTGRESQL OPERATIONS ====================
    
    async fn add_currency_to_postgres(pg_pool: &PgPool, currency: Currency) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_currencies (id, precision, name, currency_type)
            VALUES ($1, $2, $3, $4::fincept_terminal.CURRENCY_TYPE)
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
        let quote_currency = instrument.quote_currency().code.to_string();
        let ts_event = chrono::Utc::now().timestamp_nanos();
        
        // Map XDG to DOGE (Kraken uses XDG but database stores as DOGE)
        let base_currency_db = base_currency.as_ref().map(|c| {
            if c == "XDG" { "DOGE" } else { c.as_str() }
        });
        let quote_currency_db = if quote_currency == "XDG" { "DOGE" } else { quote_currency.as_str() };
        
        // Ensure currencies exist before inserting instrument
        if let Some(base_curr) = base_currency_db {
            // Try to insert base currency if it doesn't exist (will be ignored if it does)
            sqlx::query(
                r#"INSERT INTO fincept_terminal.nautilus_currencies (id, precision, name, currency_type) 
                   VALUES ($1, $2, $3, $4::fincept_terminal.CURRENCY_TYPE) 
                   ON CONFLICT (id) DO NOTHING"#
            )
            .bind(base_curr)
            .bind(8i32) // Default precision
            .bind(base_curr)
            .bind("CRYPTO")
            .execute(pg_pool)
            .await?;
        }
        
        // Ensure quote currency exists
        sqlx::query(
            r#"INSERT INTO fincept_terminal.nautilus_currencies (id, precision, name, currency_type) 
               VALUES ($1, $2, $3, $4::fincept_terminal.CURRENCY_TYPE) 
               ON CONFLICT (id) DO NOTHING"#
        )
        .bind(quote_currency_db)
        .bind(8i32) // Default precision
        .bind(quote_currency_db)
        .bind("CRYPTO")
        .execute(pg_pool)
        .await?;
        
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_instruments 
            (id, exchange_id, raw_symbol, base_currency, quote_currency,
             price_precision, size_precision, price_increment, margin_init, 
             margin_maint, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO UPDATE SET
                raw_symbol = EXCLUDED.raw_symbol,
                price_precision = EXCLUDED.price_precision,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(instrument_id)
        .bind(exchange_id)
        .bind(instrument.raw_symbol().to_string())
        .bind(base_currency_db.map(|s| s.to_string()))
        .bind(quote_currency_db.to_string())
        .bind(instrument.price_precision() as i32)
        .bind(instrument.size_precision() as i32)
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
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        quote: QuoteTick,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_quote_table_name(config.user_id, exchange_id);
        
        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (quotes) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;
            
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(quote.ts_event.as_i64()))
                    .with_column("instrument_id", GreptimeValue::String(quote.instrument_id.to_string()))
                    .with_column("bid_price", GreptimeValue::Float64(quote.bid_price.as_f64()))
                    .with_column("ask_price", GreptimeValue::Float64(quote.ask_price.as_f64()))
                    .with_column("bid_size", GreptimeValue::Float64(quote.bid_size.as_f64()))
                    .with_column("ask_size", GreptimeValue::Float64(quote.ask_size.as_f64()));
                
            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write quote to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;
        
        Ok(())
        }
        
        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (quotes) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
    }
    
    async fn add_trade_hybrid(
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        trade: TradeTick,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        // Extract exchange name from instrument_id for consistent table naming
        let exchange_name = Self::extract_exchange_name(&trade.instrument_id);
        let table_name = format!("trades_{}_{}", exchange_name, config.trading_mode);
        
        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (trades) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;
            
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(trade.ts_event.as_i64()))
                    .with_column("user_id", GreptimeValue::String(config.user_id.to_string()))
                    .with_column("strategy_id", GreptimeValue::String(config.strategy_id.clone().unwrap_or_else(|| "".to_string())))
                    .with_column("instrument_id", GreptimeValue::String(trade.instrument_id.to_string()))
                    .with_column("price", GreptimeValue::Float64(trade.price.as_f64()))
                    .with_column("quantity", GreptimeValue::Float64(trade.size.as_f64()))
                    .with_column("aggressor_side", GreptimeValue::String(trade.aggressor_side.to_string()))
                .with_column("trade_id", GreptimeValue::String(trade.trade_id.to_string()));
                
            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write trade to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;
        
        Ok(())
        }
        
        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (trades) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
    }
    
    async fn add_bar_hybrid(
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        bar: Bar,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_bar_table_name(config.user_id, exchange_id);
        
        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (bars) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;
            
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(bar.ts_event.as_i64()))
                .with_column("instrument_id", GreptimeValue::String(bar.bar_type.instrument_id().to_string()))
                    .with_column("bar_type", GreptimeValue::String(bar.bar_type.to_string()))
                    .with_column("open", GreptimeValue::Float64(bar.open.as_f64()))
                    .with_column("high", GreptimeValue::Float64(bar.high.as_f64()))
                    .with_column("low", GreptimeValue::Float64(bar.low.as_f64()))
                    .with_column("close", GreptimeValue::Float64(bar.close.as_f64()))
                    .with_column("volume", GreptimeValue::Float64(bar.volume.as_f64()));
                
            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write bar to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;
        
        Ok(())
        }
        
        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (bars) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
    }
    
    async fn add_signal_hybrid(
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        signal: Signal,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        let table_name = Self::get_signal_table_name(config.user_id, exchange_id);
        
        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (signals) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;
            
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
            // Parse signal.value as f64 if possible, otherwise store as string
            let signal_value = signal.value.parse::<f64>()
                .map(GreptimeValue::Float64)
                .unwrap_or_else(|_| GreptimeValue::String(signal.value.clone()));
                
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(signal.ts_event.as_i64()))
                    .with_column("signal_name", GreptimeValue::String(signal.name.to_string()))
                .with_column("signal_value", signal_value);
                
            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write signal to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;
        
        Ok(())
        }
        
        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (signals) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
    }
    
    async fn add_indicator_hybrid(
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        instrument_id: InstrumentId,
        indicator_name: String,  // This is the api_name from strategy config (e.g., "slow_ema")
        indicator_type: String,  // The indicator type (e.g., "EMA", "MACD")
        bar_type: BarType,
        timestamp: i64,
        value: Option<f64>,
        values_json: Option<String>,
        _exchange_id: i32,
        strategy_id: Option<String>,
    ) -> anyhow::Result<()> {
        // Generate table name: indicators_{exchange}_{trading_mode}_{period}
        // All indicator types go into the same table, distinguished by indicator_type column
        let exchange_name = Self::extract_exchange_name(&instrument_id);
        let period = Self::extract_bar_period(&bar_type);
        let table_name = format!("indicators_{}_{}_{}", exchange_name, config.trading_mode, period);

        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (indicators) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;

            use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};

            let mut row = GreptimeRow::new()
                .with_column("timestamp", GreptimeValue::Timestamp(timestamp))
                .with_column("indicator_type", GreptimeValue::String(indicator_type.clone()))
                .with_column("indicator_name", GreptimeValue::String(indicator_name.clone()))
                .with_column("instrument_id", GreptimeValue::String(instrument_id.to_string()));

            // Add strategy_id if available (for filtering by backtest run)
            if let Some(sid) = &strategy_id {
                row = row.with_column("strategy_id", GreptimeValue::String(sid.clone()));
            }

            // Add value field (single-value indicators) or values_json (multi-value indicators)
            if let Some(v) = value {
                row = row.with_column("value", GreptimeValue::Float64(v));
            }
            if let Some(json_str) = &values_json {
                row = row.with_column("values_json", GreptimeValue::Json(json_str.clone()));
            }

            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write indicator to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;

            Ok(())
        }

        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (indicators) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
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
    
    fn get_order_book_table_name(user_id: Uuid, exchange_id: i32) -> String {
        format!("user_{}_order_books_exchange_{}", user_id, exchange_id)
    }
    
    fn get_signal_table_name(user_id: Uuid, exchange_id: i32) -> String {
        format!("user_{}_signals_exchange_{}", user_id, exchange_id)
    }
    
    // ==================== POSTGRESQL OPERATIONS (CONTINUED) ====================
    
    async fn add_general_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        key: String,
        value: Vec<u8>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(key)
        .bind(value)
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn ensure_instrument_exists(pg_pool: &PgPool, instrument_id: &str) -> anyhow::Result<()> {
        // Check if instrument exists, if not return error (instruments should be inserted first)
        // This is a best-effort check - if instrument insertion is queued but not yet committed,
        // we'll still get a foreign key error, but this helps with ordering issues
        let instrument_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM fincept_terminal.nautilus_instruments WHERE id = $1)"
        )
        .bind(instrument_id)
        .fetch_one(pg_pool)
        .await?;
        
        if !instrument_exists {
            // Don't fail immediately - the instrument might be in the queue
            // The foreign key constraint will catch it if it truly doesn't exist
            // This is just a warning to help with ordering
            tracing::warn!("Instrument {} not found in database - may cause foreign key error if not queued", instrument_id);
        }
        
        Ok(())
    }
    
    async fn ensure_account_exists(pg_pool: &PgPool, user_id: Uuid, account_id: &str) -> anyhow::Result<()> {
        // Check if account exists, if not create a placeholder
        let account_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM fincept_terminal.nautilus_accounts WHERE user_id = $1 AND id = $2)"
        )
        .bind(user_id)
        .bind(account_id)
        .fetch_one(pg_pool)
        .await?;
        
        if !account_exists {
            // Create a placeholder account to satisfy foreign key constraint
            // The account will be properly populated when add_account is called
            sqlx::query(
                r#"
                INSERT INTO fincept_terminal.nautilus_accounts 
                (id, user_id, account_type, base_currency, is_cash_account, calculate_account_state)
                VALUES ($1, $2, $3::fincept_terminal.ACCOUNT_TYPE, $4, $5, $6)
                ON CONFLICT (id) DO NOTHING
                "#
            )
            .bind(account_id)
            .bind(user_id)
            .bind("Cash")
            .bind::<Option<String>>(None)
            .bind(true)
            .bind(false)
            .execute(pg_pool)
            .await?;
        }
        
        Ok(())
    }
    
    async fn ensure_trader_exists(pg_pool: &PgPool, user_id: Uuid, trader_id: &str) -> anyhow::Result<String> {
        // First, check if there's ANY trader for this user (reuse existing trader per user)
        let existing_trader_id: Option<String> = sqlx::query_scalar(
            "SELECT id FROM fincept_terminal.nautilus_traders WHERE user_id = $1 LIMIT 1"
        )
        .bind(user_id)
        .fetch_optional(pg_pool)
        .await?;
        
        if let Some(existing_id) = existing_trader_id {
            // Reuse the existing trader for this user
            return Ok(existing_id);
        }
        
        // No trader exists for this user, create a new one
        // Use a default ID if the passed trader_id is empty or use the passed one
        let trader_id_to_use = if trader_id.is_empty() {
            "default".to_string()
                } else {
            trader_id.to_string()
        };
        
        // Try to insert trader - if user_id doesn't exist, foreign key constraint will fail
        match sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_traders (id, user_id, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO NOTHING
            "#
        )
        .bind(&trader_id_to_use)
        .bind(user_id)
        .bind(format!("Trader {}", trader_id_to_use))
        .execute(pg_pool)
        .await
        {
            Ok(_) => Ok(trader_id_to_use),
            Err(e) => {
                // Check if this is a foreign key constraint violation for user_id
                let error_msg = e.to_string();
                if error_msg.contains("nautilus_traders_user_id_fkey") || error_msg.contains("violates foreign key constraint") {
                    anyhow::bail!(
                        "User ID {} does not exist in fincept_terminal.users table. \
                        Please ensure the user_id from the strategy config exists in the database. \
                        Original error: {}",
                        user_id,
                        error_msg
                    );
                }
                Err(anyhow::anyhow!("Failed to create trader: {}", e))
            }
        }
    }
    
    async fn ensure_strategy_exists(
        pg_pool: &PgPool,
        user_id: Uuid,
        strategy_id: &str,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Each run should create a NEW strategy record with a NEW strategy_id
        // Generate a new UUID string for this run (this is the PRIMARY KEY in nautilus_strategies.id)
        let new_strategy_id = Uuid::new_v4().to_string();
        
        // Try to get base name from OUR strategies table (fincept_terminal.strategies)
        // Extract base name from nautilus_strategy_id (e.g., "DynamicStrategy-000" -> "DynamicStrategy")
        let base_name = if let Some(dash_pos) = strategy_id.rfind('-') {
            &strategy_id[..dash_pos]
        } else {
            strategy_id
        };
        
        // Generate unique name: "{base_name}-{uuid}" (e.g., "DynamicStrategy-172e2f8a-a1d6-459d-814b-d95c2581134c")
        let strategy_name = format!("{}-{}", base_name, new_strategy_id);
        
        // Insert new strategy record with new strategy_id (id column is PRIMARY KEY)
        // Note: After migration 024, nautilus_strategies table has 'id' as PRIMARY KEY (TEXT)
        // We store the nautilus_strategy_id in order_id_tag for reference
        // We store the unique name in the name column
        let result = sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_strategies (id, user_id, order_id_tag, name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO NOTHING
            "#
        )
        .bind(&new_strategy_id)
        .bind(user_id)
        .bind(strategy_id) // Store nautilus strategy_id in order_id_tag for reference
        .bind(&strategy_name) // Store unique name: "{base_name}-{uuid}"
        .execute(pg_pool)
        .await?;
        
        if result.rows_affected() > 0 {
            tracing::info!("Successfully inserted strategy record: id={}, nautilus_strategy_id={}", new_strategy_id, strategy_id);
        } else {
            tracing::warn!("Strategy record insert returned 0 rows (may have conflicted): id={}, nautilus_strategy_id={}", new_strategy_id, strategy_id);
        }
        
        // Link the new nautilus strategy back to the most recent strategy_run for this user
        // Match by user_id and most recent date_time_started where nautilus_strategy_id IS NULL
        let update_result = sqlx::query(
            r#"
            UPDATE fincept_terminal.strategy_runs
            SET nautilus_strategy_id = $1
            WHERE strategy_run_id = (
                SELECT sr.strategy_run_id
                FROM fincept_terminal.strategy_runs sr
                INNER JOIN fincept_terminal.strategies s ON sr.strategy_id = s.strategy_id
                WHERE s.user_id = $2
                  AND sr.nautilus_strategy_id IS NULL
                ORDER BY sr.date_time_started DESC
                LIMIT 1
            )
            "#
        )
        .bind(&new_strategy_id)
        .bind(user_id)
        .execute(pg_pool)
        .await?;
        
        if update_result.rows_affected() > 0 {
            tracing::info!("Successfully linked nautilus strategy {} to strategy_run for user {}", new_strategy_id, user_id);
        } else {
            tracing::warn!("No strategy_run found to link nautilus strategy {} for user {} (may be expected if no active runs)", new_strategy_id, user_id);
        }
        
        // Cache the new strategy_id in config for use by positions/orders
        config.strategy_id = Some(new_strategy_id.clone());
        config.position_counter = 0; // Reset counter for new strategy run
        config.position_id_mapping.clear(); // Clear mapping for new strategy run
        
        Ok(())
    }
    
    async fn add_account_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        account: AccountAny,
        updated: bool,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Note: user_id must already exist in fincept_terminal.users
        // If it doesn't, the foreign key constraint will fail with a clear error
        let original_account_id = account.id().to_string();
        
        // CRITICAL FIX: Reuse cached account_id if it exists (from first call)
        // This ensures we use the same account_id throughout the run, even if strategy_id becomes available later
        // This prevents creating duplicate accounts when strategy_id is set after the first account creation
        let account_id = if let Some(cached_account_id) = &config.account_id {
            // Reuse the account_id from first call (prevents creating duplicate accounts)
            cached_account_id.clone()
        } else {
            // Generate new account_id per run: combine original_account_id with strategy_id or run_id
            // If strategy_id is None (account added before first order), generate a unique run_id
            let new_account_id = if let Some(strategy_id) = &config.strategy_id {
                format!("{}_{}", original_account_id, strategy_id)
            } else {
                // If strategy_id not set yet (account added before first order), generate a unique run_id
                // This ensures we create a NEW account record instead of updating the old one
                let run_id = Uuid::new_v4().to_string();
                tracing::warn!("strategy_id is None when adding account - generating unique run_id: {}", run_id);
                format!("{}_{}", original_account_id, run_id)
            };
            // Cache the new account_id for reuse in subsequent calls
            config.account_id = Some(new_account_id.clone());
            new_account_id
        };
        
        // Use Account trait methods through AccountAny (enum_dispatch provides these)
        // Note: AccountAny doesn't directly implement Account trait methods, so we pattern match
        // Map account type to enum value (database expects "Cash", "Margin", "Betting")
        let account_type = match &account {
            AccountAny::Cash(_) => "Cash",
            AccountAny::Margin(_) => "Margin",
        };
        let is_cash_account = match &account {
            AccountAny::Cash(_) => true,
            AccountAny::Margin(_) => false,
        };
        let calculated_account_state = match &account {
            AccountAny::Cash(cash) => cash.base.calculate_account_state,
            AccountAny::Margin(margin) => margin.base.calculate_account_state,
        };
        
        let base_currency = account.base_currency().map(|c| c.code.to_string());
        
        // Get current account state with ALL balances (not just from last event)
        // The account object maintains current balances for all currencies
        // We need to construct AccountState from the account's current state, not just last_event()
        let last_event = account.last_event();
        let (ts_event, ts_init, starting_balances, account_state_json) = if let Some(ref state) = last_event {
            // Use timestamps from last event
            let ts_event = state.ts_event.as_i64();
            let ts_init = state.ts_init.as_i64();
            
            // Extract balances from AccountState for starting_balances
            let balances_json: Vec<serde_json::Value> = state.balances.iter()
                .map(|balance| {
                    serde_json::json!({
                        "amount": balance.total.to_string(),
                        "currency": balance.total.currency.code.to_string()
                    })
                })
                .collect();
            let starting_balances = Some(serde_json::Value::Array(balances_json));
            
            // Construct a NEW AccountState from the account's CURRENT balances
            // This ensures we have ALL currencies, not just those that changed in the last event
            // Access current balances from the account object itself
            // Note: account.balances is a HashMap<Currency, AccountBalance>, need to convert to Vec
            let current_balances_map = match &account {
                AccountAny::Cash(cash) => &cash.base.balances,
                AccountAny::Margin(margin) => &margin.base.balances,
            };
            
            let current_balances_vec: Vec<_> = current_balances_map.values().cloned().collect();
            
            // Build AccountState with current balances from account, but use metadata from last event
            let current_account_state = AccountState {
                account_id: state.account_id.clone(),
                account_type: state.account_type,
                base_currency: state.base_currency.clone(),
                balances: current_balances_vec, // Use current balances from account (converted to Vec)
                margins: state.margins.clone(),
                is_reported: state.is_reported,
                event_id: state.event_id.clone(),
                ts_event: state.ts_event,
                ts_init: state.ts_init,
            };
            
            // Serialize the current AccountState with all balances
            let account_state_json = serde_json::to_value(&current_account_state)?;
            
            (ts_event, ts_init, starting_balances, Some(account_state_json))
        } else {
            // Fallback to current time if no events
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;
            (now, now, None, None)
        };
        
        // Check if account already exists (needed for both starting_balances and account_state logic)
        let account_exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(SELECT 1 FROM fincept_terminal.nautilus_accounts WHERE user_id = $1 AND id = $2)
            "#
        )
        .bind(user_id)
        .bind(&account_id)
                .fetch_one(pg_pool)
                .await?;
        
        // CRITICAL FIX: Only set starting_balances on initial creation, not on updates
        // If account already exists, set starting_balances to None so it doesn't get updated
        let starting_balances_for_db = if account_exists {
            None  // Don't update starting_balances if account already exists
        } else {
            starting_balances.clone()  // Only set starting_balances on first INSERT (clone to avoid move)
        };
        
        // Determine if we should update account_state based on trading_mode
        // For backtest: update at start (first call when account doesn't exist) and end (when updated=true)
        // For paper/live: update real-time (every time updated=true)
        let should_update_account_state = match config.trading_mode.as_str() {
            "backtest" => {
                // Update if: account doesn't exist yet (initial creation) OR updated=true (final update)
                !account_exists || updated
            }
            "paper" | "live" => {
                // For paper/live: update real-time whenever account changes (updated=true)
                updated
            }
            _ => true, // Default: update
        };
        
        // Store account metadata with starting_balances and account_state
        // Note: Table has PRIMARY KEY (id) - each run gets a NEW account_id
        // account_state stores ONLY the current AccountState (last event), not the entire AccountAny
        let result = if should_update_account_state {
            // Update account_state column with current AccountState
            if let Some(ref state_json) = account_state_json {
                sqlx::query(
                    r#"
                    INSERT INTO fincept_terminal.nautilus_accounts 
                    (id, user_id, account_type, base_currency, is_cash_account, calculate_account_state, starting_balances, account_state)
                    VALUES ($1, $2, $3::fincept_terminal.ACCOUNT_TYPE, $4, $5, $6, $7, $8)
                    ON CONFLICT (id) DO UPDATE SET
                        account_type = EXCLUDED.account_type,
                        base_currency = EXCLUDED.base_currency,
                        user_id = EXCLUDED.user_id,
                        calculate_account_state = EXCLUDED.calculate_account_state,
                        account_state = EXCLUDED.account_state,
                        updated_at = CURRENT_TIMESTAMP
                    "#
                )
                .bind(account_id.clone())
                .bind(user_id)
                .bind(account_type)
                .bind(base_currency)
                .bind(is_cash_account)
                .bind(calculated_account_state)
                .bind(starting_balances_for_db)  // Use conditional starting_balances (None if account exists)
                .bind(state_json)
                .execute(pg_pool)
                .await
            } else {
                // No account state available - insert without account_state
                sqlx::query(
                    r#"
                    INSERT INTO fincept_terminal.nautilus_accounts 
                    (id, user_id, account_type, base_currency, is_cash_account, calculate_account_state, starting_balances)
                    VALUES ($1, $2, $3::fincept_terminal.ACCOUNT_TYPE, $4, $5, $6, $7)
                    ON CONFLICT (id) DO UPDATE SET
                        account_type = EXCLUDED.account_type,
                        base_currency = EXCLUDED.base_currency,
                        user_id = EXCLUDED.user_id,
                        calculate_account_state = EXCLUDED.calculate_account_state,
                        updated_at = CURRENT_TIMESTAMP
                    "#
                )
                .bind(account_id.clone())
                .bind(user_id)
                .bind(account_type)
                .bind(base_currency)
                .bind(is_cash_account)
                .bind(calculated_account_state)
                .bind(starting_balances_for_db)  // Use conditional starting_balances (None if account exists)
                .execute(pg_pool)
                .await
            }
        } else {
            // Don't update account_state column
            sqlx::query(
                r#"
                INSERT INTO fincept_terminal.nautilus_accounts 
                (id, user_id, account_type, base_currency, is_cash_account, calculate_account_state, starting_balances)
                VALUES ($1, $2, $3::fincept_terminal.ACCOUNT_TYPE, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                    account_type = EXCLUDED.account_type,
                    base_currency = EXCLUDED.base_currency,
                    user_id = EXCLUDED.user_id,
                    calculate_account_state = EXCLUDED.calculate_account_state,
                    updated_at = CURRENT_TIMESTAMP
                "#
            )
                .bind(account_id.clone())
                .bind(user_id)
                .bind(account_type)
                .bind(base_currency)
                .bind(is_cash_account)
                .bind(calculated_account_state)
                .bind(starting_balances_for_db)  // Use conditional starting_balances (None if account exists)
                .execute(pg_pool)
                .await
        };
        
        // If account INSERT fails, log the error and try to create a placeholder
        match result {
            Ok(_) => {
                // Account inserted/updated successfully
            }
            Err(e) => {
                // Account INSERT failed - log error and create placeholder
                let error_msg = format!("{}", e);
                let truncated_error = if error_msg.len() > 200 {
                    &error_msg[..200]
                } else {
                    &error_msg
                };
                tracing::error!("Failed to insert account {}: {}", account_id, truncated_error);
                // Try to create placeholder to satisfy foreign key constraint
                if let Err(placeholder_err) = Self::ensure_account_exists(pg_pool, user_id, &account_id).await {
                    let placeholder_error_msg = format!("{}", placeholder_err);
                    let truncated_placeholder = if placeholder_error_msg.len() > 200 {
                        &placeholder_error_msg[..200]
                    } else {
                        &placeholder_error_msg
                    };
                    tracing::error!("Failed to create placeholder account {}: {}", account_id, truncated_placeholder);
                }
            }
        }
        
        // Store account state as JSONB in account_events table
        // Serialize the full account state for reconstruction
        let account_json = serde_json::to_value(&account)?;
        
        // Generate unique ID for account event (format: account_id_timestamp)
        let event_id = format!("{}_{}", account_id, ts_event);
        
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_account_events 
            (id, user_id, account_id, account_state, ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO NOTHING
            "#
        )
        .bind(event_id)
        .bind(user_id)
        .bind(account_id)
        .bind(account_json)
        .bind(ts_event)
        .bind(ts_init)
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_order_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        order: OrderAny,
        client_id: Option<ClientId>,
        updated: bool,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Store order metadata
        // Note: User should already exist - created at initialization
        let order_id = order.client_order_id().to_string();
        let instrument_id = order.instrument_id().to_string();
        let order_type = order.order_type().to_string();
        let order_side = order.order_side().to_string();
        let status = order.status().to_string();
        
        // Get nautilus_strategy_id from order (e.g., "DynamicStrategy-000")
        // This is used to create the strategy record if needed
        let nautilus_strategy_id = order.strategy_id().to_string();

        // Ensure strategy run exists and strategy_id is cached (only once)
        if config.strategy_id.is_none() {
            tracing::info!("Creating new strategy record for nautilus_strategy_id: {}", nautilus_strategy_id);

            Self::ensure_strategy_exists(pg_pool, user_id, &nautilus_strategy_id, config).await?;

            tracing::info!("Strategy record created with strategy_id: {:?}", config.strategy_id);
        }

        // Use the cached strategy_id (UUID) for database storage
        // This is the PRIMARY KEY in nautilus_strategies and links to strategy_runs.nautilus_strategy_id
        let strategy_id = config.strategy_id.clone().unwrap_or_else(|| {
            tracing::warn!("strategy_id not set in config after ensure_strategy_exists - using nautilus_strategy_id as fallback");
            nautilus_strategy_id.clone()
        });
        
        // Get last_event for ts_init/ts_last extraction
        let last_event = order.last_event();
        
        // Get account_id from cached config ONLY
        // Orders DO NOT have account_id - we must use the cached value from config
        // The same account_id is used throughout the backtest
        let account_id = match config.account_id.as_ref() {
            Some(cached_acc_id) => cached_acc_id.clone(),
            None => {
                return Err(anyhow::anyhow!("Order {} is missing account_id - cannot insert into database. Config lacks account_id. An account must be added before orders can be inserted.", order_id));
            }
        };
        
        // Use cached trader_id from config (initialized once per adapter)
        let trader_id = config.trader_id.as_ref().expect("trader_id should be initialized in connect()");
        
        // Clone values for error logging (they'll be moved by .bind())
        let strategy_id_for_log = strategy_id.clone();
        let account_id_for_log = account_id.clone();
        let trader_id_for_log = trader_id.clone();
        
        // Get ts_init and ts_last from order (table has ts_init and ts_last, not ts_event)
        let ts_init = match last_event {
            OrderEventAny::Initialized(e) => e.ts_init.as_i64(),
            OrderEventAny::Denied(e) => e.ts_init.as_i64(),
            OrderEventAny::Emulated(e) => e.ts_init.as_i64(),
            OrderEventAny::Released(e) => e.ts_init.as_i64(),
            OrderEventAny::Submitted(e) => e.ts_init.as_i64(),
            OrderEventAny::Accepted(e) => e.ts_init.as_i64(),
            OrderEventAny::Rejected(e) => e.ts_init.as_i64(),
            OrderEventAny::Canceled(e) => e.ts_init.as_i64(),
            OrderEventAny::Expired(e) => e.ts_init.as_i64(),
            OrderEventAny::Triggered(e) => e.ts_init.as_i64(),
            OrderEventAny::PendingUpdate(e) => e.ts_init.as_i64(),
            OrderEventAny::PendingCancel(e) => e.ts_init.as_i64(),
            OrderEventAny::ModifyRejected(e) => e.ts_init.as_i64(),
            OrderEventAny::CancelRejected(e) => e.ts_init.as_i64(),
            OrderEventAny::Updated(e) => e.ts_init.as_i64(),
            OrderEventAny::Filled(e) => e.ts_init.as_i64(),
        };
        let ts_last = last_event.ts_event().as_i64();
        
        // Get init_id from order (UUID4 that identifies the initialization event)
        let init_id = order.init_id().to_string();
        
        // Get price - use order.price() if available
        // For market orders, price() may be None, but we'll store it as-is
        // The actual execution price will be captured in order events (Filled events have last_px)
        let price = order.price().map(|p| p.to_string());
        
        // Get quote currency from instrument to determine base_asset_price
        // Query the instrument from the database to get quote_currency
        let quote_currency: Option<String> = sqlx::query_scalar(
            "SELECT quote_currency FROM fincept_terminal.nautilus_instruments WHERE id = $1"
        )
        .bind(&instrument_id)
        .fetch_optional(pg_pool)
        .await?
        .flatten();
        
        // Determine base_asset_price based on quote currency
        // For backtesting: set to 1.0 if USD, otherwise None (will be populated in live trading)
        // TODO: In live/paper trading, implement async price fetching for non-USD quote currencies
        // This will require an asynchronous process to get current USD price of USDT, USDC, etc.
        let (base_asset_price, base_asset_price_timestamp, base_asset_price_source) = 
            if let Some(quote) = &quote_currency {
                if quote == "USD" {
                    // USD quote currency - no conversion needed, set to 1.0
                    (Some(1.0), Some(ts_last), Some("backtest".to_string()))
                } else {
                    // Non-USD quote currency - leave as None for backtesting
                    // TODO: In live trading, implement async price fetching here
                    // Example: fetch USDT/USD or USDC/USD price from Coinbase/Binance API
                    (None, None, None)
                }
            } else {
                // Instrument not found in database - leave as None
                (None, None, None)
            };
        
        let result = sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_orders 
            (user_id, id, trader_id, strategy_id, instrument_id, client_order_id, account_id, order_type, order_side, status, 
             quantity, price, time_in_force, init_id, ts_init, ts_last, base_asset_price, base_asset_price_timestamp, base_asset_price_source)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (id) DO UPDATE SET
                status = EXCLUDED.status,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                base_asset_price = EXCLUDED.base_asset_price,
                base_asset_price_timestamp = EXCLUDED.base_asset_price_timestamp,
                base_asset_price_source = EXCLUDED.base_asset_price_source,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(order_id.clone())
        .bind(&trader_id)
        .bind(strategy_id)
        .bind(instrument_id)
        .bind(order_id.clone())
        .bind(account_id)
        .bind(order_type)
        .bind(order_side)
        .bind(status)
        .bind(order.quantity().to_string())
        .bind(price)
        .bind(order.time_in_force().to_string())
        .bind(init_id)
        .bind(ts_init.to_string())
        .bind(ts_last.to_string())
        .bind(base_asset_price)
        .bind(base_asset_price_timestamp)
        .bind(base_asset_price_source)
        .execute(pg_pool)
        .await?;
        
        // Store order event
        let last_event = order.last_event();
        Self::add_order_event_to_postgres(pg_pool, user_id, last_event.clone(), client_id, config).await?;
        
        Ok(())
    }
    
    async fn add_order_event_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        event: OrderEventAny,
        client_id: Option<ClientId>,
        config: &GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Note: User and instrument should already exist - created at initialization
        let instrument_id = event.instrument_id().to_string();
        let client_order_id = event.client_order_id().to_string();
        
        // Get ts_event and ts_init from the event
        let (ts_event, ts_init) = match &event {
            OrderEventAny::Initialized(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Denied(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Emulated(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Released(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Submitted(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Accepted(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Rejected(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Canceled(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Expired(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Triggered(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::PendingUpdate(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::PendingCancel(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::ModifyRejected(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::CancelRejected(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Updated(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
            OrderEventAny::Filled(e) => (e.ts_event.as_i64(), e.ts_init.as_i64()),
        };
        
        // Get event kind (table uses "kind" not "event_type")
        let event_kind = match &event {
            OrderEventAny::Initialized(_) => "Initialized",
            OrderEventAny::Denied(_) => "Denied",
            OrderEventAny::Emulated(_) => "Emulated",
            OrderEventAny::Released(_) => "Released",
            OrderEventAny::Submitted(_) => "Submitted",
            OrderEventAny::Accepted(_) => "Accepted",
            OrderEventAny::Rejected(_) => "Rejected",
            OrderEventAny::Canceled(_) => "Canceled",
            OrderEventAny::Expired(_) => "Expired",
            OrderEventAny::Triggered(_) => "Triggered",
            OrderEventAny::PendingUpdate(_) => "PendingUpdate",
            OrderEventAny::PendingCancel(_) => "PendingCancel",
            OrderEventAny::ModifyRejected(_) => "ModifyRejected",
            OrderEventAny::CancelRejected(_) => "CancelRejected",
            OrderEventAny::Updated(_) => "Updated",
            OrderEventAny::Filled(_) => "Filled",
        };
        
        // Get order details from event for the INSERT
        let instrument_id = event.instrument_id().to_string();

        // Use the cached strategy_id (UUID) from config for database storage
        // This is the PRIMARY KEY in nautilus_strategies and links to strategy_runs.nautilus_strategy_id
        let strategy_id = config.strategy_id.clone().unwrap_or_else(|| {
            let fallback = event.strategy_id().to_string();
            tracing::warn!("strategy_id not set in config for order event - using nautilus_strategy_id as fallback: {}", fallback);
            fallback
        });

        // Use cached trader_id from config (initialized once per adapter)
        let trader_id = config.trader_id.as_ref().expect("trader_id should be initialized in connect()");
        
        // Extract event-specific fields
        // For Filled events, we need last_px, last_qty, trade_id, etc.
        // For other events, most fields will be None
        let (trade_id, currency, order_type, order_side, quantity, time_in_force, liquidity_side,
             post_only, reduce_only, quote_quantity, reconciliation, price, last_px, last_qty,
             trigger_price, trigger_type, limit_offset, trailing_offset, trailing_offset_type,
             expire_time, display_qty, emulation_trigger, trigger_instrument_id, contingency_type,
             order_list_id, linked_order_ids, parent_order_id, exec_algorithm_id, exec_algorithm_params,
             exec_spawn_id, venue_order_id, account_id, position_id, commission, tags) = match &event {
            OrderEventAny::Filled(e) => (
                e.trade_id().map(|id| id.to_string()), // trade_id() returns Option<TradeId>
                e.currency().map(|c| c.to_string()),
                e.order_type().map(|ot| ot.to_string()),
                e.order_side().map(|os| os.to_string()),
                e.quantity().map(|q| q.to_string()),
                e.time_in_force().map(|tif| tif.to_string()),
                e.liquidity_side().map(|ls| ls.to_string()),
                e.post_only(),
                e.reduce_only(),
                e.quote_quantity(),
                Some(e.reconciliation()), // Wrap bool in Some for database binding
                e.price().map(|p| p.to_string()),
                e.last_px().map(|p| p.to_string()), // This is the execution price!
                e.last_qty().map(|q| q.to_string()),
                e.trigger_price().map(|p| p.to_string()),
                e.trigger_type().map(|tt| tt.to_string()),
                e.limit_offset().map(|lo| lo.to_string()),
                e.trailing_offset().map(|to| to.to_string()),
                e.trailing_offset_type().map(|tot| tot.to_string()),
                e.expire_time().map(|et| et.to_string()),
                e.display_qty().map(|dq| dq.to_string()),
                e.emulation_trigger().map(|et| et.to_string()),
                e.trigger_instrument_id().map(|tid| tid.to_string()),
                e.contingency_type().map(|ct| ct.to_string()),
                e.order_list_id().map(|oli| oli.to_string()),
                e.linked_order_ids().as_ref().map(|ids| ids.iter().map(|id| id.to_string()).collect::<Vec<String>>()),
                e.parent_order_id().map(|poi| poi.to_string()),
                e.exec_algorithm_id().map(|eai| eai.to_string()),
                None::<String>, // exec_algorithm_params - not available on OrderFilled
                e.exec_spawn_id().map(|esi| esi.to_string()),
                e.venue_order_id().map(|voi| voi.to_string()),
                e.account_id().map(|ai| ai.to_string()),
                e.position_id().map(|pi| pi.to_string()),
                e.commission().map(|c| c.to_string()),
                None::<Vec<String>>, // tags - not available on OrderFilled
            ),
            _ => (
                // For other event types, most fields are not available
                // Only extract what's available via OrderEvent trait methods
                None, // trade_id: Option<String>
                None, // currency: Option<String>
                None, // order_type: Option<String>
                None, // order_side: Option<String>
                None, // quantity: Option<String>
                None, // time_in_force: Option<String>
                None, // liquidity_side: Option<String>
                None, // post_only: Option<bool>
                None, // reduce_only: Option<bool>
                None, // quote_quantity: Option<bool>
                Some(false), // reconciliation: bool (must match Filled branch type)
                None, // price: Option<String>
                None, // last_px: Option<String>
                None, // last_qty: Option<String>
                None, // trigger_price: Option<String>
                None, // trigger_type: Option<String>
                None, // limit_offset: Option<String>
                None, // trailing_offset: Option<String>
                None, // trailing_offset_type: Option<String>
                None, // expire_time: Option<String>
                None, // display_qty: Option<String>
                None, // emulation_trigger: Option<String>
                None, // trigger_instrument_id: Option<String>
                None, // contingency_type: Option<String>
                None, // order_list_id: Option<String>
                None, // linked_order_ids: Option<Vec<String>>
                None, // parent_order_id: Option<String>
                None, // exec_algorithm_id: Option<String>
                None, // exec_algorithm_params: Option<String>
                None, // exec_spawn_id: Option<String>
                None, // venue_order_id: Option<String>
                event.account_id().map(|ai| ai.to_string()), // account_id: Option<String>
                None, // position_id: Option<String>
                None, // commission: Option<String>
                None, // tags: Option<Vec<String>>
            ),
        };
        
        // For trailing_offset_type enum and exec_algorithm_params JSONB, we need to handle them specially
        // PostgreSQL requires explicit casting for enum and JSONB types
        // Build SQL fragments for both
        let trailing_offset_type_sql = if let Some(tot_str) = &trailing_offset_type {
            format!("'{}'::fincept_terminal.TRAILING_OFFSET_TYPE", tot_str.replace("'", "''"))
        } else {
            "NULL::fincept_terminal.TRAILING_OFFSET_TYPE".to_string()
        };
        
        // For exec_algorithm_params, we need to cast the bound parameter to JSONB
        // Always bind it (even if None), and cast in SQL using CAST function
        // When None, sqlx will bind NULL, and CAST(NULL AS jsonb) works
        // It's bound as the 36th parameter (after exec_algorithm_id which is $35)
        let exec_algorithm_params_sql = "CAST($36 AS jsonb)".to_string();
        
        // Build the query with both casts
        let query_str = format!(
            r#"
            INSERT INTO fincept_terminal.nautilus_order_events 
            (id, user_id, kind, trader_id, strategy_id, instrument_id, client_order_id, client_id,
             trade_id, currency, order_type, order_side, quantity, time_in_force, liquidity_side,
             post_only, reduce_only, quote_quantity, reconciliation, price, last_px, last_qty,
             trigger_price, trigger_type, limit_offset, trailing_offset, trailing_offset_type,
             expire_time, display_qty, emulation_trigger, trigger_instrument_id, contingency_type,
             order_list_id, linked_order_ids, parent_order_id, exec_algorithm_id, exec_algorithm_params,
             exec_spawn_id, venue_order_id, account_id, position_id, commission, tags,
             ts_event, ts_init)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, {}, $27, $28, $29, $30, $31, $32, $33, $34, $35, {}, $37, $38, $39, $40, $41, $42, $43, $44)
            ON CONFLICT (id) DO UPDATE SET
                kind = EXCLUDED.kind,
                trade_id = EXCLUDED.trade_id,
                last_px = EXCLUDED.last_px,
                last_qty = EXCLUDED.last_qty,
                price = EXCLUDED.price,
                quantity = EXCLUDED.quantity,
                updated_at = CURRENT_TIMESTAMP
            "#,
            trailing_offset_type_sql,
            exec_algorithm_params_sql
        );
        
        let result = sqlx::query(&query_str)
            .bind(format!("{}_{}", client_order_id, ts_event))
            .bind(user_id)
            .bind(event_kind)
            .bind(&trader_id)
            .bind(strategy_id)
            .bind(instrument_id)
            .bind(client_order_id)
            .bind(client_id.map(|id| id.to_string()))
            .bind(trade_id)
            .bind(currency)
            .bind(order_type)
            .bind(order_side)
            .bind(quantity)
            .bind(time_in_force)
            .bind(liquidity_side)
            .bind(post_only)
            .bind(reduce_only)
            .bind(quote_quantity)
            .bind(reconciliation)
            .bind(price)
            .bind(last_px)
            .bind(last_qty)
            .bind(trigger_price)
            .bind(trigger_type)
            .bind(limit_offset)
            .bind(trailing_offset)
            // trailing_offset_type is embedded in SQL above (column 27, no parameter)
            .bind(expire_time) // $27
            .bind(display_qty) // $28
            .bind(emulation_trigger) // $29
            .bind(trigger_instrument_id) // $30
            .bind(contingency_type) // $31
            .bind(order_list_id) // $32
            .bind(linked_order_ids) // $33
            .bind(parent_order_id) // $34
            .bind(exec_algorithm_id) // $35
            // exec_algorithm_params is embedded as CAST($36 AS jsonb) (column 37, parameter $36)
            .bind(exec_algorithm_params.as_deref()) // $36 - must be bound here to match CAST($36 AS jsonb)
            .bind(exec_spawn_id) // $37
            .bind(venue_order_id)
            .bind(account_id)
            .bind(position_id)
            .bind(commission)
            .bind(tags)
            .bind(ts_event.to_string())
            .bind(ts_init.to_string())
            .execute(pg_pool)
            .await;
        
        result?;
        
        Ok(())
    }
    
    async fn update_order_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        event: OrderEventAny,
        config: &GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Update order status based on event
        let client_order_id = event.client_order_id().to_string();
        let status = match &event {
            OrderEventAny::Filled(_) => "Filled",
            OrderEventAny::Canceled(_) => "Canceled",
            OrderEventAny::Rejected(_) => "Rejected",
            OrderEventAny::Expired(_) => "Expired",
            _ => "PendingUpdate", // Default for other events
        };
        
        // Extract execution price from Filled events (last_px)
        // For market orders, this is the actual execution price
        let execution_price: Option<String> = match &event {
            OrderEventAny::Filled(e) => {
                // OrderFilled events have last_px() method with the execution price
                e.last_px().map(|p| p.to_string())
            }
            _ => None,
        };
        
        // Get instrument_id to determine quote currency for base_asset_price update
        let instrument_id = event.instrument_id().to_string();
        let quote_currency: Option<String> = sqlx::query_scalar(
            "SELECT quote_currency FROM fincept_terminal.nautilus_instruments WHERE id = $1"
        )
        .bind(&instrument_id)
        .fetch_optional(pg_pool)
        .await?
        .flatten();
        
        // Update base_asset_price if this is a Filled event and quote currency is USD
        // For backtesting: set to 1.0 if USD, otherwise None
        // TODO: In live/paper trading, implement async price fetching for non-USD quote currencies
        let (base_asset_price, base_asset_price_timestamp, base_asset_price_source) = 
            if matches!(&event, OrderEventAny::Filled(_)) {
                if let Some(quote) = &quote_currency {
                    if quote == "USD" {
                        // USD quote currency - no conversion needed, set to 1.0
                        let ts_event = event.ts_event().as_i64();
                        (Some(1.0), Some(ts_event), Some("backtest".to_string()))
                    } else {
                        // Non-USD quote currency - leave as None for backtesting
                        // TODO: In live trading, implement async price fetching here
                        (None, None, None)
                    }
                } else {
                    (None, None, None)
                }
            } else {
                (None, None, None)
            };
        
        // Build UPDATE query with conditional fields based on what we have
        let query = if execution_price.is_some() && base_asset_price.is_some() {
            sqlx::query(
                r#"
                UPDATE fincept_terminal.nautilus_orders 
                SET status = $1::fincept_terminal.ORDER_STATUS, 
                    price = $2,
                    base_asset_price = $3,
                    base_asset_price_timestamp = $4,
                    base_asset_price_source = $5,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = $6 AND id = $7
                "#
            )
            .bind(status)
            .bind(execution_price)
            .bind(base_asset_price)
            .bind(base_asset_price_timestamp)
            .bind(base_asset_price_source)
            .bind(user_id)
            .bind(client_order_id.clone())
        } else if execution_price.is_some() {
            sqlx::query(
                r#"
                UPDATE fincept_terminal.nautilus_orders 
                SET status = $1::fincept_terminal.ORDER_STATUS, 
                    price = $2,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = $3 AND id = $4
                "#
            )
            .bind(status)
            .bind(execution_price)
            .bind(user_id)
            .bind(client_order_id.clone())
        } else if base_asset_price.is_some() {
            sqlx::query(
                r#"
                UPDATE fincept_terminal.nautilus_orders 
                SET status = $1::fincept_terminal.ORDER_STATUS, 
                    base_asset_price = $2,
                    base_asset_price_timestamp = $3,
                    base_asset_price_source = $4,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = $5 AND id = $6
                "#
            )
            .bind(status)
            .bind(base_asset_price)
            .bind(base_asset_price_timestamp)
            .bind(base_asset_price_source)
            .bind(user_id)
            .bind(client_order_id.clone())
        } else {
            sqlx::query(
                r#"
                UPDATE fincept_terminal.nautilus_orders 
                SET status = $1::fincept_terminal.ORDER_STATUS, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = $2 AND id = $3
                "#
            )
            .bind(status)
            .bind(user_id)
            .bind(client_order_id.clone())
        };
        
        query.execute(pg_pool).await?;
        
        // Store the event
        Self::add_order_event_to_postgres(pg_pool, user_id, event, None, config).await?;
        
        Ok(())
    }
    
    async fn add_custom_data_hybrid(
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        data: CustomData,
    ) -> anyhow::Result<()> {
        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (custom data) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;
            
                use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
                
                let table_name = format!("user_{}_custom_data", config.user_id);
                let row = GreptimeRow::new()
                    .with_column("timestamp", GreptimeValue::Timestamp(data.ts_event.as_i64()))
                    .with_column("data_type", GreptimeValue::String(data.data_type.to_string()))
                    .with_column("value", GreptimeValue::String(serde_json::to_string(&data)?));
                
            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write custom data to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;
            
            Ok(())
        }
        
        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (custom data) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
    }
    
    // ==================== ADDITIONAL POSTGRESQL OPERATIONS ====================
    
    async fn add_synthetic_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        synthetic: SyntheticInstrument,
    ) -> anyhow::Result<()> {
        // Store synthetic instrument metadata
        // Note: Synthetic instruments may be stored as JSONB in nautilus_general or a dedicated table
        let synthetic_json = serde_json::to_value(&synthetic)?;
        let key = format!("synthetic_{}", synthetic.id);
        
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(key)
        .bind(serde_json::to_vec(&synthetic_json)?)
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_position_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        position: Position,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // strategy_id should already be cached from when the first order was added
        // If it's still None, something went wrong - log a warning but continue
        if config.strategy_id.is_none() {
            tracing::warn!("strategy_id is None when adding position - this should have been set when the first order was added");
        }
        
        let nautilus_position_id = position.id.to_string();
        
        // Check if we've already generated an ID for this Nautilus position
        let position_id = if let Some(existing_id) = config.position_id_mapping.get(&nautilus_position_id) {
            existing_id.clone()
        } else if let Some(strategy_id) = &config.strategy_id {
            // Generate new position ID based on strategy_id
            let new_id = if config.enable_multiple_trades {
                // Increment counter for each new position when multiple trades enabled
                config.position_counter += 1;
                format!("{}_{}", strategy_id, config.position_counter)
                } else {
                // Use fixed ID (_1) when multiple trades disabled - all positions share same ID
                format!("{}_{}", strategy_id, 1)
            };
            config.position_id_mapping.insert(nautilus_position_id.clone(), new_id.clone());
            new_id
        } else {
            // Fallback to original position ID if strategy_id not set yet
            // This should not happen in normal flow, but provides safety
            nautilus_position_id
        };
        let position_id_for_log = position_id.clone();
        let instrument_id = position.instrument_id.to_string();
        // Note: User, instrument, strategy, trader, and account should already exist - created at initialization
        let account_id = position.account_id.to_string();
        
        // Use cached trader_id from config (initialized once per adapter)
        let trader_id = config.trader_id.as_ref().expect("trader_id should be initialized in connect()");

        // Use the cached strategy_id (UUID) from config for database storage
        // This is the PRIMARY KEY in nautilus_strategies and links to strategy_runs.nautilus_strategy_id
        let strategy_id = config.strategy_id.clone().unwrap_or_else(|| {
            let fallback = position.strategy_id.to_string();
            tracing::warn!("strategy_id not set in config for position - using nautilus_strategy_id as fallback: {}", fallback);
            fallback
        });

        // Store position in nautilus_positions table
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_positions
            (id, user_id, trader_id, strategy_id, instrument_id, account_id,
             opening_order_id, closing_order_id, entry, side, signed_qty, quantity, peak_qty,
             quote_currency, base_currency, settlement_currency,
             avg_px_open, avg_px_close, realized_return, realized_pnl, unrealized_pnl,
             commissions, duration_ns, ts_opened, ts_closed, ts_init, ts_last)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
            ON CONFLICT ON CONSTRAINT nautilus_positions_pkey DO UPDATE SET
                signed_qty = EXCLUDED.signed_qty,
                quantity = EXCLUDED.quantity,
                avg_px_open = EXCLUDED.avg_px_open,
                avg_px_close = EXCLUDED.avg_px_close,
                realized_pnl = EXCLUDED.realized_pnl,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                ts_last = EXCLUDED.ts_last,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(position_id)
        .bind(user_id)
        .bind(&trader_id)
        .bind(&strategy_id)
        .bind(instrument_id)
        .bind(account_id)
        .bind(position.opening_order_id.to_string())
        .bind(position.closing_order_id.map(|id| id.to_string())) // closing_order_id is nullable
        .bind(position.entry.to_string())
        .bind(position.side.to_string())
        .bind(position.signed_qty)
        .bind(position.quantity.to_string())
        .bind(position.peak_qty.to_string())
        .bind(position.quote_currency.code.to_string())
        .bind(position.base_currency.map(|c| c.code.to_string()))
        .bind(position.settlement_currency.code.to_string())
        .bind(position.avg_px_open)
        .bind(position.avg_px_close.map(|p| p))
        .bind(position.realized_return)
        .bind(position.realized_pnl.map(|p| p.to_string()))
        .bind(None::<String>) // unrealized_pnl - not directly available, would need calculation
        .bind({
            // Convert commissions HashMap<Currency, Money> to TEXT[] array
            // Each Money is formatted as "AMOUNT CURRENCY" string
            let mut commission_strings: Vec<String> = Vec::new();
            for (currency, money) in &position.commissions {
                commission_strings.push(format!("{} {}", money, currency.code));
            }
            commission_strings
        })
        .bind(position.duration_ns.to_string())
        .bind(position.ts_opened.as_i64().to_string())
        .bind(position.ts_closed.map(|t| t.as_i64().to_string()))
        .bind(position.ts_init.as_i64().to_string())
        .bind(position.ts_last.as_i64().to_string())
        .execute(pg_pool)
        .await
        .map_err(|e| {
            tracing::error!("Failed to insert position {}: {}", position_id_for_log, e);
            e
        })?;
        
        Ok(())
    }
    
    async fn add_order_snapshot_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        snapshot: OrderSnapshot,
    ) -> anyhow::Result<()> {
        // Store order snapshot - this is already handled by the existing add_order_snapshot
        // but we need to route it through the query system
        // For now, serialize and store in nautilus_general
        let snapshot_json = serde_json::to_value(&snapshot)?;
        let key = format!("order_snapshot_{}", snapshot.client_order_id);
        
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(key)
        .bind(serde_json::to_vec(&snapshot_json)?)
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_position_snapshot_to_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        snapshot: PositionSnapshot,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Note: User, instrument, strategy, trader, and account should already exist - created at initialization
        let instrument_id = snapshot.instrument_id.to_string();
        let account_id = snapshot.account_id.to_string();
        
        // Use cached trader_id from config (initialized once per adapter)
        let trader_id = config.trader_id.as_ref().expect("trader_id should be initialized in connect()");

        // Use the cached strategy_id (UUID) from config for database storage
        // This is the PRIMARY KEY in nautilus_strategies and links to strategy_runs.nautilus_strategy_id
        let strategy_id = config.strategy_id.clone().unwrap_or_else(|| {
            let fallback = snapshot.strategy_id.to_string();
            tracing::warn!("strategy_id not set in config for position snapshot - using nautilus_strategy_id as fallback: {}", fallback);
            fallback
        });

        let nautilus_position_id = snapshot.position_id.to_string();

        // Check if we've already generated an ID for this Nautilus position
        let position_id = if let Some(existing_id) = config.position_id_mapping.get(&nautilus_position_id) {
            existing_id.clone()
        } else if let Some(cached_strategy_id) = &config.strategy_id {
            // Generate new position ID based on strategy_id
            let new_id = if config.enable_multiple_trades {
                // Increment counter for each new position when multiple trades enabled
                config.position_counter += 1;
                format!("{}_{}", cached_strategy_id, config.position_counter)
                } else {
                // Use fixed ID (_1) when multiple trades disabled - all positions share same ID
                format!("{}_{}", cached_strategy_id, 1)
            };
            config.position_id_mapping.insert(nautilus_position_id.clone(), new_id.clone());
            new_id
        } else {
            // Fallback to original position ID if strategy_id not set yet
            nautilus_position_id
        };

        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_positions
            (id, user_id, trader_id, strategy_id, instrument_id, account_id,
             opening_order_id, closing_order_id, entry, side, signed_qty, quantity, peak_qty,
             quote_currency, base_currency, settlement_currency,
             avg_px_open, avg_px_close, realized_return, realized_pnl, unrealized_pnl,
             commissions, duration_ns, ts_opened, ts_closed, ts_init, ts_last)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
            ON CONFLICT ON CONSTRAINT nautilus_positions_pkey DO UPDATE SET
                signed_qty = EXCLUDED.signed_qty,
                quantity = EXCLUDED.quantity,
                peak_qty = EXCLUDED.peak_qty,
                avg_px_open = EXCLUDED.avg_px_open,
                avg_px_close = EXCLUDED.avg_px_close,
                realized_return = EXCLUDED.realized_return,
                realized_pnl = EXCLUDED.realized_pnl,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                commissions = EXCLUDED.commissions,
                duration_ns = EXCLUDED.duration_ns,
                ts_opened = EXCLUDED.ts_opened,
                ts_closed = EXCLUDED.ts_closed,
                ts_init = EXCLUDED.ts_init,
                ts_last = EXCLUDED.ts_last,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(position_id)
        .bind(user_id)
        .bind(&trader_id)
        .bind(&strategy_id)
        .bind(snapshot.instrument_id.to_string())
        .bind(snapshot.account_id.to_string())
        .bind(snapshot.opening_order_id.to_string())
        .bind(snapshot.closing_order_id.map(|id| id.to_string()))
        .bind(snapshot.entry.to_string())
        .bind(snapshot.side.to_string())
        .bind(snapshot.signed_qty)
        .bind(snapshot.quantity.to_string())
        .bind(snapshot.peak_qty.to_string())
        .bind(snapshot.quote_currency.to_string())
        .bind(snapshot.base_currency.map(|c| c.to_string()))
        .bind(snapshot.settlement_currency.to_string())
        .bind(snapshot.avg_px_open)
        .bind(snapshot.avg_px_close)
        .bind(snapshot.realized_return)
        .bind(snapshot.realized_pnl.map(|p| p.to_string()))
        .bind(snapshot.unrealized_pnl.map(|p| p.to_string()))
        .bind({
            // Convert commissions Vec<Money> to TEXT[] array
            // Each Money is already formatted as "AMOUNT CURRENCY" string
            snapshot.commissions.iter().map(|money| money.to_string()).collect::<Vec<String>>()
        })
        .bind(snapshot.duration_ns.map(|d| d.to_string()))
        .bind(snapshot.ts_opened.to_string())
        .bind(snapshot.ts_closed.map(|t| t.to_string()))
        .bind(snapshot.ts_init.to_string())
        .bind(snapshot.ts_last.to_string())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn add_order_book_hybrid(
        _pg_pool: &PgPool,
        config: &GreptimePostgresHybridCacheConfig,
        greptime_client: &Option<GreptimeClient>,
        order_book: OrderBook,
        exchange_id: i32,
    ) -> anyhow::Result<()> {
        // Order books are time-series data - store in GreptimeDB, not PostgreSQL
        let table_name = Self::get_order_book_table_name(config.user_id, exchange_id);
        
        #[cfg(feature = "greptime")]
        {
            let client = greptime_client.as_ref()
                .ok_or_else(|| anyhow::anyhow!("GreptimeDB is not available. Time-series data (order books) cannot be stored in PostgreSQL. Please ensure GreptimeDB is running and accessible."))?;
            
            use crate::sql::greptime_client::{GreptimeRow, GreptimeValue};
            
            // Extract bids and asks from OrderBook
            // OrderBook has bids() and asks() methods that return iterators over BookLevel
            // Each BookLevel has price and size information
            let bids_vec: Vec<serde_json::Value> = order_book.bids(None)
                .map(|level| {
                    serde_json::json!([
                        level.price.value.as_f64(),
                        level.size_decimal().to_string().parse::<f64>().unwrap_or(0.0)
                    ])
                })
                .collect();
            
            let asks_vec: Vec<serde_json::Value> = order_book.asks(None)
                .map(|level| {
                    serde_json::json!([
                        level.price.value.as_f64(),
                        level.size_decimal().to_string().parse::<f64>().unwrap_or(0.0)
                    ])
                })
                .collect();
            
            let bids_json = serde_json::to_string(&bids_vec)?;
            let asks_json = serde_json::to_string(&asks_vec)?;
            
            let row = GreptimeRow::new()
                .with_column("timestamp", GreptimeValue::Timestamp(order_book.ts_last.as_i64()))
                .with_column("instrument_id", GreptimeValue::String(order_book.instrument_id.to_string()))
                .with_column("book_type", GreptimeValue::String(format!("{:?}", order_book.book_type)))
                .with_column("update_count", GreptimeValue::Int64(order_book.update_count as i64))
                .with_column("bids", GreptimeValue::Json(bids_json))
                .with_column("asks", GreptimeValue::Json(asks_json))
                .with_column("ts_last", GreptimeValue::Timestamp(order_book.ts_last.as_i64()));
            
            client.insert(&table_name, row).await
                .map_err(|e| anyhow::anyhow!("Failed to write order book to GreptimeDB: {}. Time-series data cannot be stored in PostgreSQL.", e))?;
            
            Ok(())
        }
        
        #[cfg(not(feature = "greptime"))]
        {
            anyhow::bail!("GreptimeDB feature is not enabled. Time-series data (order books) cannot be stored. Enable the 'greptime' feature to use GreptimeDB.")
        }
    }
    
    async fn update_position_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        position: Position,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Update position - same as add_position but with updated flag
        Self::add_position_to_postgres(pg_pool, user_id, position, config).await
    }
    
    async fn update_actor_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        component_id: ComponentId,
        data: HashMap<String, Bytes>,
    ) -> anyhow::Result<()> {
        // Update actor state by storing key-value pairs in nautilus_general
        for (key, value) in data {
            let full_key = format!("actor_{}_{}", component_id, key);
            sqlx::query(
                r#"
                INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
                VALUES ($1, $2, $3)
                ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
                "#
            )
            .bind(user_id)
            .bind(full_key)
            .bind(value.to_vec())
            .execute(pg_pool)
            .await?;
        }
        
        Ok(())
    }
    
    async fn update_strategy_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        strategy_id: StrategyId,
        data: HashMap<String, Bytes>,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        let nautilus_strategy_id = strategy_id.to_string();
        
        // Generate a new UUID string for this strategy run (this is the PRIMARY KEY in nautilus_strategies.id)
        // Each run gets a NEW strategy_id - don't reuse existing ones
        let new_strategy_id = Uuid::new_v4().to_string();
        
        // Extract base name from nautilus_strategy_id (e.g., "DynamicStrategy-000" -> "DynamicStrategy")
        let base_name = if let Some(dash_pos) = nautilus_strategy_id.rfind('-') {
            &nautilus_strategy_id[..dash_pos]
        } else {
            &nautilus_strategy_id
        };
        
        // Generate unique name: "{base_name}-{uuid}" (e.g., "DynamicStrategy-172e2f8a-a1d6-459d-814b-d95c2581134c")
        let strategy_name = format!("{}-{}", base_name, new_strategy_id);
        
        // Always create a NEW strategy record for this run
        // Use new_strategy_id (UUID string) as the PRIMARY KEY (id column)
        // Store nautilus_strategy_id in order_id_tag for reference
        // Store unique name in name column
        tracing::info!("Creating new strategy record via update_strategy: id={}, nautilus_strategy_id={}, name={}", new_strategy_id, nautilus_strategy_id, strategy_name);
        let result = sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_strategies (id, user_id, order_id_tag, name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO NOTHING
            "#
        )
        .bind(&new_strategy_id)
        .bind(user_id)
        .bind(&nautilus_strategy_id) // Store nautilus strategy_id in order_id_tag
        .bind(&strategy_name) // Store unique name: "{base_name}-{uuid}"
        .execute(pg_pool)
        .await?;
        
        if result.rows_affected() > 0 {
            tracing::info!("Successfully inserted strategy record via update_strategy: id={}, nautilus_strategy_id={}, name={}", new_strategy_id, nautilus_strategy_id, strategy_name);
        } else {
            tracing::warn!("Strategy record insert returned 0 rows (may have conflicted): id={}, nautilus_strategy_id={}", new_strategy_id, nautilus_strategy_id);
        }
        
        // Cache the new strategy_id in config for position ID generation
        config.strategy_id = Some(new_strategy_id.clone());
        config.position_counter = 0; // Reset counter for new strategy run
        config.position_id_mapping.clear(); // Clear mapping for new strategy run
        
        // Update strategy state by storing key-value pairs in nautilus_general
        // Use the new strategy_id as the key prefix (no mapping needed - strategy_id is the key)
        for (key, value) in data {
            let full_key = format!("strategy_{}_{}", new_strategy_id, key);
            sqlx::query(
                r#"
                INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
                VALUES ($1, $2, $3)
                ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
                "#
            )
            .bind(user_id)
            .bind(full_key)
            .bind(value.to_vec())
            .execute(pg_pool)
            .await?;
        }
        
        Ok(())
    }
    
    async fn snapshot_order_state_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        order: OrderAny,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Create order snapshot from current order state
        let order_json = serde_json::to_value(&order)?;
        let key = format!("order_snapshot_{}", order.client_order_id());
        
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(key)
        .bind(serde_json::to_vec(&order_json)?)
        .execute(pg_pool)
        .await?;
        
        // Also update the order record itself
        let client_id = None; // Snapshot doesn't need client_id
        Self::add_order_to_postgres(pg_pool, user_id, order, client_id, true, config).await?;
        
        Ok(())
    }
    
    async fn snapshot_position_state_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        position: Position,
        config: &mut GreptimePostgresHybridCacheConfig,
    ) -> anyhow::Result<()> {
        // Create position snapshot - same as updating the position
        Self::add_position_to_postgres(pg_pool, user_id, position, config).await
    }
    
    async fn index_venue_order_id_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        // Create index mapping client_order_id to venue_order_id
        // Store in nautilus_orders table by updating the venue_order_id field
        sqlx::query(
            r#"
            UPDATE fincept_terminal.nautilus_orders
            SET venue_order_id = $1, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = $2 AND id = $3
            "#
        )
        .bind(venue_order_id.to_string())
        .bind(user_id)
        .bind(client_order_id.to_string())
        .execute(pg_pool)
        .await?;
        
        // Also store in nautilus_general for quick lookup
        let key = format!("venue_order_index_{}", client_order_id);
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(key)
        .bind(venue_order_id.to_string().into_bytes())
        .execute(pg_pool)
        .await?;
        
        Ok(())
    }
    
    async fn index_order_position_in_postgres(
        pg_pool: &PgPool,
        user_id: Uuid,
        client_order_id: ClientOrderId,
        position_id: PositionId,
    ) -> anyhow::Result<()> {
        // Create index mapping client_order_id to position_id
        // Store in nautilus_general for quick lookup
        let key = format!("order_position_index_{}", client_order_id);
        sqlx::query(
            r#"
            INSERT INTO fincept_terminal.nautilus_general (user_id, id, value)
            VALUES ($1, $2, $3)
            ON CONFLICT ON CONSTRAINT nautilus_general_pkey DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = CURRENT_TIMESTAMP
            "#
        )
        .bind(user_id)
        .bind(key)
        .bind(position_id.to_string().into_bytes())
        .execute(pg_pool)
        .await?;
        
        // Also update the order record with position_id if the table has that field
        // (This depends on the actual schema)
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl CacheDatabaseAdapter for GreptimePostgresHybridCacheAdapter {
    fn close(&mut self) -> anyhow::Result<()> {
        log::debug!("Closing FinceptTerminal hybrid cache");
        
        // Flush all pending operations before closing
        if let Err(e) = self.flush() {
            log::warn!("Error during flush before close: {e:?}");
                }

        // Send close signal to processing task
        if let Err(e) = self.tx.send(GreptimePostgresHybridQuery::Close) {
            log::warn!("Error sending close signal (task may have terminated): {e:?}");
        }

        // Wait for task to complete with timeout
        log::debug!("Awaiting hybrid cache task completion");
        let rt = get_runtime();
        tokio::task::block_in_place(|| {
            rt.block_on(async {
                match tokio::time::timeout(Duration::from_secs(10), &mut self.handle).await {
                    Ok(Ok(())) => {
                        log::debug!("Hybrid cache task completed successfully");
                        Ok(())
                    }
                    Ok(Err(e)) => {
                log::error!("Error awaiting hybrid cache task: {e:?}");
                        Err(anyhow::anyhow!("Task error: {}", e))
                    }
                    Err(_) => {
                        log::warn!("Task did not complete within timeout, aborting");
                        self.handle.abort();
                        Ok(())
                    }
                }
            })
        })?;
        
        // Close PostgreSQL connection pool
        log::debug!("Closing PostgreSQL connection pool");
        let pool = self.pg_pool.clone();
        let rt2 = get_runtime();
        tokio::task::block_in_place(|| {
            rt2.block_on(async {
                pool.close().await;
            });
        });
        
        log::debug!("FinceptTerminal hybrid cache closed successfully");
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        // Create oneshot channel for flush acknowledgment
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        
        // Send flush signal to processing task
        if let Err(e) = self.flush_tx.send(ack_tx) {
            tracing::warn!("Failed to send flush signal (task may have terminated): {}", e);
            return Err(anyhow::anyhow!("Failed to send flush signal: {}", e));
        }
        
        // Wait for flush to complete (with timeout)
        let rt = get_runtime();
        tokio::task::block_in_place(|| {
            rt.block_on(async {
                match tokio::time::timeout(Duration::from_secs(5), ack_rx).await {
                    Ok(Ok(())) => {
                        tracing::debug!("Flush completed successfully");
        Ok(())
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Flush acknowledgment error: {}", e);
                        Err(anyhow::anyhow!("Flush failed: {}", e))
                    }
                    Err(_) => {
                        tracing::warn!("Flush timed out after 5 seconds");
                        Err(anyhow::anyhow!("Flush timed out"))
                    }
                }
            })
        })
    }

    async fn load_all(&self) -> anyhow::Result<CacheMap> {
        // Create a new pool within this runtime context to avoid cross-runtime pool usage
        // The existing self.pg_pool cannot be used if we're in a different runtime
        let pool = Self::create_pool_in_runtime(&self.config).await?;
        
        // Load relational data from PostgreSQL using fincept_terminal schema
        // Use the new pool instead of self.pg_pool
        let (currencies, instruments, accounts, orders) = try_join!(
            Self::load_currencies_with_pool(&pool, &self.config),
            Self::load_instruments_with_pool(&pool, &self.config),
            Self::load_accounts_with_pool(&pool, &self.config),
            Self::load_orders_with_pool(&pool, &self.config),
        )?;

        let synthetics = Self::load_synthetics_with_pool(&pool, &self.config).await?;
        let positions = Self::load_positions_with_pool(&pool, &self.config).await?;
        
        Ok(CacheMap {
            currencies,
            instruments,
            synthetics,
            accounts,
            orders,
            positions,
        })
    }

    fn load(&self) -> anyhow::Result<HashMap<String, Bytes>> {
        // Use tokio::spawn pattern like cache.rs, but we need to create a new pool
        // since PgPool cannot be used across different runtimes
        let user_id = self.config.user_id;
        let config = self.config.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        
        // Use get_runtime() to handle existing runtime context
        let rt = get_runtime();
        
        rt.spawn(async move {
            // Create a new connection pool within this runtime
            let pool = match Self::create_pool_in_runtime(&config).await {
                Ok(p) => p,
                Err(e) => {
                    log::error!("Failed to create pool in load(): {e:?}");
                    let _ = tx.send(HashMap::new());
                    return;
                }
            };
            
            let result = sqlx::query(
                r#"
                SELECT id, value FROM fincept_terminal.nautilus_general
                WHERE user_id = $1
                "#
            )
            .bind(user_id)
            .fetch_all(&pool)
            .await
            .map_err(|e| e);
            
            match result {
                Ok(rows) => {
                    let mut cache: HashMap<String, Bytes> = HashMap::new();
                    for row in &rows {
                        let key: String = row.get("id");
                        let value: Vec<u8> = row.get("value");
                        cache.insert(key, Bytes::from(value));
                    }
                    if let Err(e) = tx.send(cache) {
                        log::error!("Failed to send general items: {e:?}");
                    }
                }
                Err(e) => {
                    log::error!("Failed to load general cache: {e:?}");
                    if let Err(e) = tx.send(HashMap::new()) {
                        log::error!("Failed to send empty general items: {e:?}");
                    }
                }
            }
        });
        
        Ok(rx.recv()?)
    }

    async fn load_currencies(&self) -> anyhow::Result<HashMap<Ustr, Currency>> {
        Self::load_currencies_with_pool(&self.pg_pool, &self.config).await
    }

    async fn load_instruments(&self) -> anyhow::Result<HashMap<InstrumentId, InstrumentAny>> {
        Self::load_instruments_with_pool(&self.pg_pool, &self.config).await
    }

    async fn load_synthetics(&self) -> anyhow::Result<HashMap<InstrumentId, SyntheticInstrument>> {
        // Load synthetic instruments from nautilus_general table
        let rows = sqlx::query(
            r#"
            SELECT id, value FROM fincept_terminal.nautilus_general
            WHERE user_id = $1 AND id LIKE 'synthetic_%'
            "#
        )
        .bind(self.config.user_id)
        .fetch_all(&self.pg_pool)
        .await?;
        
        let mut synthetics = HashMap::new();
        for row in rows {
            let key: String = row.get("id");
            let value: Vec<u8> = row.get("value");
            
            // Try to deserialize JSON
            if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&value) {
                // Try to deserialize as SyntheticInstrument
                if let Ok(synthetic) = serde_json::from_value::<SyntheticInstrument>(json_value.clone()) {
                    synthetics.insert(synthetic.id, synthetic);
                } else {
                    tracing::warn!("Failed to deserialize synthetic instrument from key: {}", key);
                }
            }
        }
        
        Ok(synthetics)
    }

    async fn load_accounts(&self) -> anyhow::Result<HashMap<AccountId, AccountAny>> {
        Self::load_accounts_with_pool(&self.pg_pool, &self.config).await
    }

    async fn load_orders(&self) -> anyhow::Result<HashMap<ClientOrderId, OrderAny>> {
        Self::load_orders_with_pool(&self.pg_pool, &self.config).await
    }

    async fn load_positions(&self) -> anyhow::Result<HashMap<PositionId, Position>> {
        Self::load_positions_with_pool(&self.pg_pool, &self.config).await
    }

    // All the other required trait methods with delegation to appropriate storage
    fn load_index_order_position(&self) -> anyhow::Result<HashMap<ClientOrderId, Position>> {
        // Load order-position index from PostgreSQL
        // Query nautilus_order_events for order-position relationships
        let user_id = self.config.user_id;
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            // Query order-position relationships from nautilus_order_events
            // Note: This requires positions to be loaded first, which is complex
            // For now, return empty - full implementation would require Position reconstruction
            let result = sqlx::query(
                r#"
                SELECT DISTINCT client_order_id, position_id
                FROM fincept_terminal.nautilus_order_events
                WHERE user_id = $1 AND position_id IS NOT NULL
                "#
            )
            .bind(user_id)
            .fetch_all(&pool)
            .await;
            
            match result {
                Ok(_rows) => {
                    // TODO: Reconstruct Position objects from position_id
                    // This requires loading positions first, which is not yet fully implemented
                    tracing::debug!("Order-position index query succeeded but Position reconstruction not yet implemented");
                    Ok(HashMap::new())
                }
                Err(_) => {
                    Ok(HashMap::new())
                }
            }
        })
    }
    
    fn load_index_order_client(&self) -> anyhow::Result<HashMap<ClientOrderId, ClientId>> {
        let user_id = self.config.user_id;
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            let result = sqlx::query(
                r#"
                SELECT DISTINCT client_order_id, client_id
                FROM fincept_terminal.nautilus_order_events
                WHERE user_id = $1 AND client_id IS NOT NULL
                "#
            )
            .bind(user_id)
            .fetch_all(&pool)
            .await
            .map_err(|e| e);
            
            match result {
                Ok(rows) => {
                    let mut index = HashMap::new();
                    for row in &rows {
                        let client_order_id_str: String = row.get("client_order_id");
                        let client_id_str: Option<String> = row.get("client_id");
                        if let Some(cid_str) = client_id_str {
                            // Parse identifiers - ClientOrderId and ClientId implement From<&str>
                            let coid = ClientOrderId::from(client_order_id_str.as_str());
                            let cid = ClientId::from(cid_str.as_str());
                            index.insert(coid, cid);
                        }
                    }
                    Ok(index)
                }
                Err(_) => {
                    Ok(HashMap::new())
                }
            }
        })
    }
    
    async fn load_currency(&self, code: &Ustr) -> anyhow::Result<Option<Currency>> {
        let row = sqlx::query("SELECT precision FROM nautilus_currencies WHERE id = $1")
            .bind(code.as_str())
            .fetch_optional(&self.pg_pool)
            .await?;
            
        if let Some(row) = row {
            let precision: i32 = row.get("precision");
            // Currency::from_str only takes the code
            let mut currency = Currency::from_str(code.as_str())?;
            currency.precision = precision as u8;
            Ok(Some(currency))
        } else {
            Ok(None)
        }
    }

    async fn load_instrument(&self, instrument_id: &InstrumentId) -> anyhow::Result<Option<InstrumentAny>> {
        // Load single instrument using the same logic as load_instruments_with_pool
        use crate::sql::models::instruments::InstrumentAnyModel;
        
        let instrument_id_str = instrument_id.to_string();
        let result = sqlx::query_as::<_, InstrumentAnyModel>(
            r#"
            SELECT * FROM fincept_terminal.nautilus_instruments
            WHERE id = $1
            LIMIT 1
            "#
        )
        .bind(&instrument_id_str)
        .fetch_optional(&self.pg_pool)
        .await;
        
        match result {
            Ok(Some(model)) => Ok(Some(model.0)),
            Ok(None) => Ok(None),
            Err(e) => {
                tracing::warn!("Failed to load instrument {}: {}", instrument_id_str, e);
                Ok(None)
            }
        }
    }
    
    async fn load_synthetic(&self, instrument_id: &InstrumentId) -> anyhow::Result<Option<SyntheticInstrument>> {
        // Load synthetic instrument from nautilus_general
        let key = format!("synthetic_{}", instrument_id);
        let row = sqlx::query(
            r#"
            SELECT value FROM fincept_terminal.nautilus_general
            WHERE user_id = $1 AND id = $2
            "#
        )
        .bind(self.config.user_id)
        .bind(&key)
        .fetch_optional(&self.pg_pool)
        .await?;
        
        if let Some(row) = row {
            let value: Vec<u8> = row.get("value");
            if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&value) {
                if let Ok(synthetic) = serde_json::from_value::<SyntheticInstrument>(json_value) {
                    return Ok(Some(synthetic));
                }
            }
        }
        
        Ok(None)
    }
    
    async fn load_account(&self, account_id: &AccountId) -> anyhow::Result<Option<AccountAny>> {
        // Load account with account_state directly from nautilus_accounts table
        // (account_state column was added in migration 023)
        let row = sqlx::query(
            r#"
            SELECT id, account_state
            FROM fincept_terminal.nautilus_accounts
            WHERE user_id = $1 AND id = $2 AND account_state IS NOT NULL
            "#
        )
        .bind(self.config.user_id)
        .bind(account_id.to_string())
        .fetch_optional(&self.pg_pool)
        .await?;
        
        // Deserialize account_state JSONB to AccountAny
        if let Some(row) = row {
            let account_state: Option<serde_json::Value> = row.get("account_state");
            if let Some(state_json) = account_state {
                // Try to deserialize as AccountAny
                match serde_json::from_value::<AccountAny>(state_json) {
                    Ok(account) => return Ok(Some(account)),
                    Err(e) => {
                        tracing::warn!("Failed to deserialize account_state for {}: {}", account_id, e);
                    }
                }
            } else {
                tracing::debug!("Account {} found but account_state is NULL", account_id);
            }
        }
        Ok(None)
    }
    
    async fn load_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<Option<OrderAny>> {
        // Load order from nautilus_orders table
        // Note: Full OrderAny reconstruction requires event replay from nautilus_order_events
        // TODO: Consider storing full order state as JSONB for easier reconstruction
        let row = sqlx::query(
            r#"
            SELECT id, client_order_id, instrument_id, order_type, order_side, status, quantity, price, time_in_force
            FROM fincept_terminal.nautilus_orders
            WHERE user_id = $1 AND client_order_id = $2
            "#
        )
        .bind(self.config.user_id)
        .bind(client_order_id.to_string())
        .fetch_optional(&self.pg_pool)
        .await?;
        
        if row.is_some() {
            tracing::debug!("Order {} found in database (OrderAny reconstruction not yet fully implemented - requires event replay)", client_order_id);
            // TODO: Reconstruct OrderAny from database row and events
            // This requires loading all events from nautilus_order_events and replaying them
        }
        
        Ok(None)
    }
    
    async fn load_position(&self, position_id: &PositionId) -> anyhow::Result<Option<Position>> {
        // Load position from nautilus_positions table
        // Note: Full Position reconstruction requires all database fields and potentially event replay
        // TODO: Consider storing full position state as JSONB for easier reconstruction
        let row = sqlx::query(
            r#"
            SELECT id, instrument_id, side, quantity, entry, signed_qty, 
                   avg_px_open, avg_px_close, realized_pnl, unrealized_pnl,
                   quote_currency, base_currency, settlement_currency, ts_opened, ts_init
            FROM fincept_terminal.nautilus_positions
            WHERE user_id = $1 AND id = $2
            "#
        )
        .bind(self.config.user_id)
        .bind(position_id.to_string())
        .fetch_optional(&self.pg_pool)
        .await?;
        
        if row.is_some() {
            tracing::debug!("Position {} found in database (Position reconstruction not yet fully implemented - requires all fields and event reconstruction)", position_id);
            // TODO: Reconstruct Position from database row
            // This requires mapping all database fields to Position struct fields
        }
        
        Ok(None)
    }
    
    fn load_actor(&self, component_id: &ComponentId) -> anyhow::Result<HashMap<String, Bytes>> {
        let user_id = self.config.user_id;
        let component_key = format!("actor_{}", component_id);
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            let result = sqlx::query(
                r#"
                SELECT id, value FROM fincept_terminal.nautilus_general
                WHERE user_id = $1 AND id LIKE $2
                "#
            )
            .bind(user_id)
            .bind(format!("{}%", component_key))
            .fetch_all(&pool)
            .await
            .map_err(|e| e);
            
            match result {
                Ok(rows) => {
                    let mut cache: HashMap<String, Bytes> = HashMap::new();
                    for row in &rows {
                        let key: String = row.get("id");
                        let value: Vec<u8> = row.get("value");
                        cache.insert(key, Bytes::from(value));
                    }
                    Ok(cache)
                }
                Err(_) => {
                    Ok(HashMap::new())
                }
            }
        })
    }
    
    fn delete_actor(&self, component_id: &ComponentId) -> anyhow::Result<()> {
        let user_id = self.config.user_id;
        let component_key = format!("actor_{}", component_id);
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            sqlx::query(
                r#"
                DELETE FROM fincept_terminal.nautilus_general
                WHERE user_id = $1 AND id LIKE $2
                "#
            )
            .bind(user_id)
            .bind(format!("{}%", component_key))
            .execute(&pool)
            .await?;
            
            Ok::<(), anyhow::Error>(())
        })?;
        
        Ok(())
    }
    
    fn load_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<HashMap<String, Bytes>> {
        let user_id = self.config.user_id;
        let strategy_id_str = strategy_id.to_string();
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            // Get the most recent strategy_id (id column) for this nautilus_strategy_id
            // The id column is the PRIMARY KEY (UUID string) we generate per run
            let strategy_id: Option<String> = sqlx::query_scalar(
                r#"
                SELECT id FROM fincept_terminal.nautilus_strategies
                WHERE user_id = $1 AND order_id_tag = $2
                ORDER BY created_at DESC
                LIMIT 1
                "#
            )
            .bind(user_id)
            .bind(&strategy_id_str) // order_id_tag stores the nautilus_strategy_id
            .fetch_optional(&pool)
            .await?;
            
            // Use the strategy_id (our generated UUID) to load data
            // If not found, fallback to using nautilus_strategy_id directly
            let strategy_key = if let Some(run_id) = strategy_id {
                format!("strategy_{}", run_id)
            } else {
                format!("strategy_{}", strategy_id_str)
            };
            
            let result = sqlx::query(
                r#"
                SELECT id, value FROM fincept_terminal.nautilus_general
                WHERE user_id = $1 AND id LIKE $2
                "#
            )
            .bind(user_id)
            .bind(format!("{}%", strategy_key))
            .fetch_all(&pool)
            .await
            .map_err(|e| e);
            
            match result {
                Ok(rows) => {
                    let mut cache: HashMap<String, Bytes> = HashMap::new();
                    for row in &rows {
                        let key: String = row.get("id");
                        let value: Vec<u8> = row.get("value");
                        cache.insert(key, Bytes::from(value));
                    }
                    Ok(cache)
                }
                Err(_) => {
                    Ok(HashMap::new())
                }
            }
        })
    }
    
    fn delete_strategy(&self, strategy_id: &StrategyId) -> anyhow::Result<()> {
        let user_id = self.config.user_id;
        let strategy_id_str = strategy_id.to_string();
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            // Get the most recent strategy_id (id column) for this nautilus_strategy_id
            let strategy_id: Option<String> = sqlx::query_scalar(
                r#"
                SELECT id FROM fincept_terminal.nautilus_strategies
                WHERE user_id = $1 AND order_id_tag = $2
                ORDER BY created_at DESC
                LIMIT 1
                "#
            )
            .bind(user_id)
            .bind(&strategy_id_str) // order_id_tag stores the nautilus_strategy_id
            .fetch_optional(&pool)
            .await?;
            
            // Delete strategy state data from nautilus_general using strategy_id
            let strategy_key = if let Some(run_id) = &strategy_id {
                format!("strategy_{}", run_id)
            } else {
                format!("strategy_{}", strategy_id_str)
            };
            
            sqlx::query(
                r#"
                DELETE FROM fincept_terminal.nautilus_general
                WHERE user_id = $1 AND id LIKE $2
                "#
            )
            .bind(user_id)
            .bind(format!("{}%", strategy_key))
            .execute(&pool)
            .await?;
            
            // Delete the strategy record from nautilus_strategies if we found a strategy_id
            if let Some(run_id) = strategy_id {
                sqlx::query(
                    r#"
                    DELETE FROM fincept_terminal.nautilus_strategies
                    WHERE user_id = $1 AND id = $2
                    "#
                )
                .bind(user_id)
                .bind(run_id)
                .execute(&pool)
                .await?;
            }
            
            Ok::<(), anyhow::Error>(())
        })?;
        
        Ok(())
    }

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
        // Determine exchange_id from instrument by querying PostgreSQL
        let instrument_id = instrument.id();
        let pool = self.pg_pool.clone();
        let instrument_id_str = instrument_id.to_string();
        
        let rt = get_runtime();
        let exchange_id = rt.block_on(async {
            Self::get_exchange_id_from_instrument(&pool, &instrument_id).await
        });
        
        let query = GreptimePostgresHybridQuery::AddInstrument(instrument.clone(), exchange_id);
        match self.tx.send(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to send add_instrument query: {e}"))
        }
    }

    fn add_synthetic(&self, synthetic: &SyntheticInstrument) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddSynthetic(synthetic.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_synthetic query: {e}"))
    }

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

    fn add_position(&self, position: &Position) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddPosition(position.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_position query: {e}"))
    }
    fn add_position_snapshot(&self, snapshot: &PositionSnapshot) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddPositionSnapshot(snapshot.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_position_snapshot query: {e}"))
    }

    fn add_order_book(&self, order_book: &OrderBook) -> anyhow::Result<()> {
        // Determine exchange_id from order_book's instrument_id
        let pool = self.pg_pool.clone();
        let instrument_id = order_book.instrument_id;
        
        let rt = get_runtime();
        let exchange_id = rt.block_on(async {
            Self::get_exchange_id_from_instrument(&pool, &instrument_id).await
        });
        
        let query = GreptimePostgresHybridQuery::AddOrderBook(order_book.to_owned(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_order_book query: {e}"))
    }

    // Time series methods - route to hybrid storage
    fn add_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        // Determine exchange_id from quote's instrument_id
        let pool = self.pg_pool.clone();
        let instrument_id = quote.instrument_id;
        
        let rt = get_runtime();
        let exchange_id = rt.block_on(async {
            Self::get_exchange_id_from_instrument(&pool, &instrument_id).await
        });
        
        let query = GreptimePostgresHybridQuery::AddQuote(quote.to_owned(), exchange_id);
        match self.tx.send(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to send add_quote query: {e}"))
        }
    }

    fn load_quotes(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        let user_id = self.config.user_id;
        let instrument_id_str = instrument_id.to_string();
        let greptime_host = self.config.greptime_host.clone();
        let greptime_port = self.config.greptime_port;
        let greptime_database = self.config.greptime_database.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            // Try GreptimeDB first
            #[cfg(feature = "greptime")]
            {
                if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                    &greptime_host,
                    greptime_port,
                    &greptime_database,
                    None,
                    None,
                ).await {
                    // Try each exchange until we find data
                    for exchange_id in 1..=10 {
                        let table_name = format!("user_{}_quotes_exchange_{}", user_id, exchange_id);
                        let sql = format!(
                            "SELECT timestamp, instrument_id, bid_price, ask_price, bid_size, ask_size FROM {} WHERE instrument_id = '{}' ORDER BY timestamp ASC",
                            table_name, instrument_id_str
                        );
                        
                        if let Ok(rows) = client.query(&sql).await {
                            if !rows.is_empty() {
                                // Convert GreptimeRow to QuoteTick
                                let mut quotes = Vec::new();
                                for row in rows {
                                    match Self::greptime_row_to_quote_tick(&row) {
                                        Ok(quote) => quotes.push(quote),
                                        Err(e) => {
                                            tracing::warn!("Failed to convert GreptimeRow to QuoteTick: {}", e);
                                            continue;
                                        }
                                    }
                                }
                                if !quotes.is_empty() {
                                    return Ok(quotes);
                                }
                            }
                        }
                    }
                }
            }
            
            // Time-series data should only be in GreptimeDB, not PostgreSQL
            // If GreptimeDB query failed, return empty result rather than falling back to PostgreSQL
            tracing::warn!("Failed to load quotes from GreptimeDB. Time-series data is not stored in PostgreSQL.");
            Ok(Vec::new())
        })
    }

    fn add_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        // Determine exchange_id from trade's instrument_id
        let pool = self.pg_pool.clone();
        let instrument_id = trade.instrument_id;
        
        let rt = get_runtime();
        let exchange_id = rt.block_on(async {
            Self::get_exchange_id_from_instrument(&pool, &instrument_id).await
        });
        
        let query = GreptimePostgresHybridQuery::AddTrade(trade.to_owned(), exchange_id);
        match self.tx.send(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to send add_trade query: {e}"))
        }
    }

    fn load_trades(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        let user_id = self.config.user_id;
        let instrument_id_str = instrument_id.to_string();
        let greptime_host = self.config.greptime_host.clone();
        let greptime_port = self.config.greptime_port;
        let greptime_database = self.config.greptime_database.clone();
        let trading_mode = self.config.trading_mode.clone();  // Capture before async move
        
        let rt = get_runtime();
        rt.block_on(async move {
            // Try GreptimeDB first
            #[cfg(feature = "greptime")]
            {
                if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                    &greptime_host,
                    greptime_port,
                    &greptime_database,
                    None,
                    None,
                ).await {
                    // Extract exchange name from instrument_id (e.g., "XDG/USD.KRAKEN" -> "kraken")
                    let exchange_name = if let Some(dot_pos) = instrument_id_str.rfind('.') {
                        instrument_id_str[dot_pos + 1..].to_lowercase()
                    } else {
                        "unknown".to_string()
                    };
                    
                    // Try the specific exchange table with trading mode suffix
                    let table_name = format!("trades_{}_{}", exchange_name, trading_mode);
                    let sql = format!(
                        "SELECT timestamp, instrument_id, price, quantity, aggressor_side, trade_id FROM {} WHERE instrument_id = '{}' ORDER BY timestamp ASC",
                        table_name, instrument_id_str
                    );
                    
                    if let Ok(rows) = client.query(&sql).await {
                        if !rows.is_empty() {
                            // Convert GreptimeRow to TradeTick
                            let mut trades = Vec::new();
                            for row in rows {
                                match Self::greptime_row_to_trade_tick(&row) {
                                    Ok(trade) => trades.push(trade),
                                    Err(e) => {
                                        tracing::warn!("Failed to convert GreptimeRow to TradeTick: {}", e);
                                        continue;
                                    }
                                }
                            }
                            if !trades.is_empty() {
                                return Ok(trades);
                            }
                        }
                    }
                }
            }
            
            // Time-series data should only be in GreptimeDB, not PostgreSQL
            tracing::warn!("Failed to load trades from GreptimeDB. Time-series data is not stored in PostgreSQL.");
            Ok(Vec::new())
        })
    }

    fn add_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        // Determine exchange_id from bar's instrument_id
        let pool = self.pg_pool.clone();
        let instrument_id = bar.bar_type.instrument_id();
        
        let rt = get_runtime();
        let exchange_id = rt.block_on(async {
            Self::get_exchange_id_from_instrument(&pool, &instrument_id).await
        });
        
        let query = GreptimePostgresHybridQuery::AddBar(bar.to_owned(), exchange_id);
        match self.tx.send(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to send add_bar query: {e}"))
        }
    }

    fn load_bars(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        let user_id = self.config.user_id;
        let instrument_id_str = instrument_id.to_string();
        let greptime_host = self.config.greptime_host.clone();
        let greptime_port = self.config.greptime_port;
        let greptime_database = self.config.greptime_database.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            // Try GreptimeDB first
            #[cfg(feature = "greptime")]
            {
                if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                    &greptime_host,
                    greptime_port,
                    &greptime_database,
                    None,
                    None,
                ).await {
                    // Try each exchange until we find data
                    for exchange_id in 1..=10 {
                        let table_name = format!("user_{}_bars_exchange_{}", user_id, exchange_id);
                        let sql = format!(
                            "SELECT timestamp, instrument_id, bar_type, open, high, low, close, volume FROM {} WHERE instrument_id = '{}' ORDER BY timestamp ASC",
                            table_name, instrument_id_str
                        );
                        
                        if let Ok(rows) = client.query(&sql).await {
                            if !rows.is_empty() {
                                // Convert GreptimeRow to Bar
                                let mut bars = Vec::new();
                                for row in rows {
                                    match Self::greptime_row_to_bar(&row) {
                                        Ok(bar) => bars.push(bar),
                                        Err(e) => {
                                            tracing::warn!("Failed to convert GreptimeRow to Bar: {}", e);
                                            continue;
                                        }
                                    }
                                }
                                if !bars.is_empty() {
                                    return Ok(bars);
                                }
                            }
                        }
                    }
                }
            }
            
            // Time-series data should only be in GreptimeDB, not PostgreSQL
            tracing::warn!("Failed to load bars from GreptimeDB. Time-series data is not stored in PostgreSQL.");
            Ok(Vec::new())
        })
    }

    fn add_signal(&self, signal: &Signal) -> anyhow::Result<()> {
        // Signals don't have instrument_id, so we use default exchange_id
        // In the future, signals could be associated with instruments/strategies
        let exchange_id = 1; // Default for signals
        let query = GreptimePostgresHybridQuery::AddSignal(signal.to_owned(), exchange_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_signal query: {e}"))
    }

    fn load_signals(&self, name: &str) -> anyhow::Result<Vec<Signal>> {
        let user_id = self.config.user_id;
        let signal_name = name.to_string();
        let greptime_host = self.config.greptime_host.clone();
        let greptime_port = self.config.greptime_port;
        let greptime_database = self.config.greptime_database.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            // Try GreptimeDB first
            #[cfg(feature = "greptime")]
            {
                if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                    &greptime_host,
                    greptime_port,
                    &greptime_database,
                    None,
                    None,
                ).await {
                    // Try each exchange until we find data
                    for exchange_id in 1..=10 {
                        let table_name = format!("user_{}_signals_exchange_{}", user_id, exchange_id);
                        let sql = format!(
                            "SELECT timestamp, signal_name, signal_value FROM {} WHERE signal_name = '{}' ORDER BY timestamp ASC",
                            table_name, signal_name
                        );
                        
                        if let Ok(rows) = client.query(&sql).await {
                            if !rows.is_empty() {
                                // Convert GreptimeRow to Signal
                                let mut signals = Vec::new();
                                for row in rows {
                                    match Self::greptime_row_to_signal(&row) {
                                        Ok(signal) => signals.push(signal),
                                        Err(e) => {
                                            tracing::warn!("Failed to convert GreptimeRow to Signal: {}", e);
                                            continue;
                                        }
                                    }
                                }
                                if !signals.is_empty() {
                                    return Ok(signals);
                                }
                            }
                        }
                    }
                }
            }
            
            // Time-series data should only be in GreptimeDB, not PostgreSQL
            tracing::warn!("Failed to load signals from GreptimeDB. Time-series data is not stored in PostgreSQL.");
            Ok(Vec::new())
        })
    }

    fn add_custom_data(&self, data: &CustomData) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddCustom(data.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send add_custom_data query: {e}"))
    }

    fn load_custom_data(&self, data_type: &DataType) -> anyhow::Result<Vec<CustomData>> {
        let user_id = self.config.user_id;
        let data_type_str = data_type.to_string();
        let greptime_host = self.config.greptime_host.clone();
        let greptime_port = self.config.greptime_port;
        let greptime_database = self.config.greptime_database.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            // Try GreptimeDB first
            #[cfg(feature = "greptime")]
            {
                if let Ok(client) = crate::sql::greptime_client::GreptimeClient::new(
                    &greptime_host,
                    greptime_port,
                    &greptime_database,
                    None,
                    None,
                ).await {
                    let table_name = format!("user_{}_custom_data", user_id);
                    let sql = format!(
                        "SELECT timestamp, data_type, value FROM {} WHERE data_type = '{}' ORDER BY timestamp ASC",
                        table_name, data_type_str
                    );
                    
                    if let Ok(rows) = client.query(&sql).await {
                        if !rows.is_empty() {
                            // Convert GreptimeRow to CustomData
                            let mut custom_data = Vec::new();
                            for row in rows {
                                match Self::greptime_row_to_custom_data(&row) {
                                    Ok(data) => custom_data.push(data),
                                    Err(e) => {
                                        tracing::warn!("Failed to convert GreptimeRow to CustomData: {}", e);
                                        continue;
                                    }
                                }
                            }
                            if !custom_data.is_empty() {
                                return Ok(custom_data);
                            }
                        }
                    }
                }
            }
            
            // Time-series data should only be in GreptimeDB, not PostgreSQL
            tracing::warn!("Failed to load custom data from GreptimeDB. Time-series data is not stored in PostgreSQL.");
            Ok(Vec::new())
        })
    }

    // Snapshot methods
    fn load_order_snapshot(&self, client_order_id: &ClientOrderId) -> anyhow::Result<Option<OrderSnapshot>> {
        let client_order_id_str = client_order_id.to_string();
        let user_id = self.config.user_id;
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            // Try to load from nautilus_general first (where snapshots are stored)
            let result = sqlx::query(
                r#"
                SELECT value FROM fincept_terminal.nautilus_general
                WHERE user_id = $1 AND id = $2
                "#
            )
            .bind(user_id)
            .bind(format!("order_snapshot_{}", client_order_id_str))
            .fetch_optional(&pool)
            .await;
            
            match result {
                Ok(Some(row)) => {
                    let value: Vec<u8> = row.get("value");
                    if let Ok(snapshot_json) = serde_json::from_slice::<serde_json::Value>(&value) {
                        // Deserialize OrderSnapshot from JSON
                        match serde_json::from_value::<OrderSnapshot>(snapshot_json) {
                            Ok(snapshot) => Ok(Some(snapshot)),
                            Err(e) => {
                                tracing::warn!("Failed to deserialize OrderSnapshot: {}", e);
                                Ok(None)
                            }
                        }
                    } else {
                        Ok(None)
                    }
                }
                Ok(None) => {
                    // Try loading from nautilus_orders table as fallback
                    // Reconstruct OrderSnapshot from order data would require replaying events
                    // For now, return None
                    Ok(None)
                }
                Err(e) => {
                    tracing::error!("Failed to load order snapshot: {e:?}");
                    Ok(None)
                }
            }
        })
    }
    
    fn load_position_snapshot(&self, position_id: &PositionId) -> anyhow::Result<Option<PositionSnapshot>> {
        let position_id_str = position_id.to_string();
        let position_id_str_clone = position_id_str.clone();
        let user_id = self.config.user_id;
        let config = self.config.clone();
        
        let rt = get_runtime();
        rt.block_on(async move {
            let pool = Self::create_pool_in_runtime(&config).await?;
            
            // Load from nautilus_positions table
            let result = sqlx::query(
                r#"
                SELECT * FROM fincept_terminal.nautilus_positions
                WHERE user_id = $1 AND id = $2
                "#
            )
            .bind(user_id)
            .bind(&position_id_str)
            .fetch_optional(&pool)
            .await;
            
            match result {
                Ok(Some(_row)) => {
                    // Try to reconstruct PositionSnapshot from database row
                    // First, try to load from nautilus_general if a snapshot was stored there
                    let snapshot_key = format!("position_snapshot_{}", position_id_str_clone);
                    if let Ok(snapshot_row) = sqlx::query(
                        r#"
                        SELECT value FROM fincept_terminal.nautilus_general
                        WHERE user_id = $1 AND id = $2
                        "#
                    )
                    .bind(config.user_id)
                    .bind(&snapshot_key)
                    .fetch_optional(&pool)
                    .await
                    {
                        if let Some(snapshot_row) = snapshot_row {
                            let value: Vec<u8> = snapshot_row.get("value");
                            if let Ok(snapshot_json) = serde_json::from_slice::<serde_json::Value>(&value) {
                                match serde_json::from_value::<PositionSnapshot>(snapshot_json) {
                                    Ok(snapshot) => return Ok(Some(snapshot)),
                                    Err(e) => {
                                        tracing::warn!("Failed to deserialize PositionSnapshot from JSON: {}", e);
                                    }
                                }
                            }
                        }
                    }
                    
                    // Fallback: PositionSnapshot reconstruction from database fields would be complex
                    // and require many fields. For now, return None.
                    tracing::debug!("Position snapshot found but deserialization from database fields not yet fully implemented");
                    Ok(None)
                }
                Ok(None) => {
                    Ok(None)
                }
                Err(e) => {
                    tracing::error!("Failed to load position snapshot: {e:?}");
                    Ok(None)
                }
            }
        })
    }

    // Index methods
    fn index_venue_order_id(&self, client_order_id: ClientOrderId, venue_order_id: VenueOrderId) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::IndexVenueOrderId(client_order_id, venue_order_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send index_venue_order_id query: {e}"))
    }
    
    fn index_order_position(&self, client_order_id: ClientOrderId, position_id: PositionId) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::IndexOrderPosition(client_order_id, position_id);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send index_order_position query: {e}"))
    }

    // Update methods
    fn update_actor(&self) -> anyhow::Result<()> {
        // Note: This method signature doesn't provide component_id or data
        // In practice, this should be called with context. For now, log a warning.
        tracing::warn!("update_actor() called without component_id or data - this may need to be called with context");
        Ok(())
    }
    
    fn update_strategy(&self) -> anyhow::Result<()> {
        // Note: This method signature doesn't provide strategy_id or data
        // In practice, this should be called with context. For now, log a warning.
        tracing::warn!("update_strategy() called without strategy_id or data - this may need to be called with context");
        Ok(())
    }

    fn update_account(&self, account: &AccountAny) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::AddAccount(account.clone(), true);
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send update_account query: {e}"))
    }

    fn update_order(&self, event: &OrderEventAny) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::UpdateOrder(event.clone());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send update_order query: {e}"))
    }

    fn update_position(&self, position: &Position) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::UpdatePosition(position.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send update_position query: {e}"))
    }

    // Snapshot methods
    fn snapshot_order_state(&self, order: &OrderAny) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::SnapshotOrderState(order.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send snapshot_order_state query: {e}"))
    }
    
    fn snapshot_position_state(&self, position: &Position) -> anyhow::Result<()> {
        let query = GreptimePostgresHybridQuery::SnapshotPositionState(position.to_owned());
        self.tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send snapshot_position_state query: {e}"))
    }

    fn heartbeat(&self, _timestamp: UnixNanos) -> anyhow::Result<()> { Ok(()) }
}

impl GreptimePostgresHybridCacheAdapter {
    // ==================== INDICATOR STORAGE ====================
    
    /// Add indicator data to GreptimeDB (not part of CacheDatabaseAdapter trait)
    pub fn add_indicator(
        &self,
        instrument_id: &InstrumentId,
        indicator_name: &str,
        indicator_type: &str,
        bar_type: &BarType,
        timestamp: i64,
        value: Option<f64>,
        values_json: Option<String>,
    ) -> anyhow::Result<()> {
        // Determine exchange_id from instrument_id
        let pool = self.pg_pool.clone();
        let instrument_id_clone = *instrument_id;

        let rt = get_runtime();
        let exchange_id = rt.block_on(async {
            Self::get_exchange_id_from_instrument(&pool, &instrument_id_clone).await
        });

        // Get strategy_id from config (UUID string format)
        let strategy_id = self.config.strategy_id.clone();

        let query = GreptimePostgresHybridQuery::AddIndicator {
            instrument_id: instrument_id_clone,
            indicator_name: indicator_name.to_string(),
            indicator_type: indicator_type.to_string(),
            bar_type: bar_type.clone(),
            timestamp,
            value,
            values_json,
            exchange_id,
            strategy_id,
        };

        match self.tx.send(query) {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("Failed to send add_indicator query: {e}")),
        }
    }
    
    // ==================== EXCHANGE ID RESOLUTION ====================
    
    /// Get exchange_id from instrument_id by querying PostgreSQL
    /// Falls back to exchange_id = 1 if not found
    async fn get_exchange_id_from_instrument(
        pg_pool: &PgPool,
        instrument_id: &InstrumentId,
    ) -> i32 {
        let instrument_id_str = instrument_id.to_string();
        
        // Try to query PostgreSQL for exchange_id
        if let Ok(row) = sqlx::query(
            r#"
            SELECT exchange_id FROM fincept_terminal.nautilus_instruments
            WHERE id = $1
            LIMIT 1
            "#
        )
        .bind(&instrument_id_str)
        .fetch_optional(pg_pool)
        .await
        {
            if let Some(row) = row {
                if let Ok(exchange_id) = row.try_get::<i32, _>("exchange_id") {
                    return exchange_id;
                }
            }
        }
        
        // Fallback: try to extract from instrument_id string format (SYMBOL.EXCHANGE)
        // This is a best-effort approach
        if let Some(dot_pos) = instrument_id_str.rfind('.') {
            let venue = &instrument_id_str[dot_pos + 1..];
            // Try to map common venue names to exchange_ids
            // This is a fallback - ideally we'd query the exchanges table
            tracing::debug!("Could not find exchange_id for instrument {}, using default", instrument_id_str);
        }
        
        // Default fallback
        1
    }
    
    // ==================== GREPTIMEDB DATA CONVERSION UTILITIES ====================
    
    /// Convert GreptimeRow to QuoteTick
    #[cfg(feature = "greptime")]
    fn greptime_row_to_quote_tick(row: &crate::sql::greptime_client::GreptimeRow) -> anyhow::Result<QuoteTick> {
        use crate::sql::greptime_client::GreptimeValue;
        use nautilus_model::types::{Price, Quantity};
        use nautilus_core::UnixNanos;
        
        let get_value = |col: &str| -> anyhow::Result<&GreptimeValue> {
            row.data.get(col)
                .ok_or_else(|| anyhow::anyhow!("Missing column: {}", col))
        };
        
        let timestamp = match get_value("timestamp")? {
            GreptimeValue::Timestamp(ts) => UnixNanos::from(*ts as u64),
            _ => anyhow::bail!("timestamp must be Timestamp type"),
        };
        
        let instrument_id = match get_value("instrument_id")? {
            GreptimeValue::String(s) => InstrumentId::from(s.as_str()),
            _ => anyhow::bail!("instrument_id must be String type"),
        };
        
        let bid_price = match get_value("bid_price")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse bid_price: {}", e))?,
            _ => anyhow::bail!("bid_price must be Float64 type"),
        };
        
        let ask_price = match get_value("ask_price")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse ask_price: {}", e))?,
            _ => anyhow::bail!("ask_price must be Float64 type"),
        };
        
        let bid_size = match get_value("bid_size")? {
            GreptimeValue::Float64(v) => Quantity::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse bid_size: {}", e))?,
            _ => anyhow::bail!("bid_size must be Float64 type"),
        };
        
        let ask_size = match get_value("ask_size")? {
            GreptimeValue::Float64(v) => Quantity::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse ask_size: {}", e))?,
            _ => anyhow::bail!("ask_size must be Float64 type"),
        };
        
        Ok(QuoteTick::new(
            instrument_id,
            bid_price,
            ask_price,
            bid_size,
            ask_size,
            timestamp,
            timestamp, // Use ts_event as ts_init
        ))
    }
    
    /// Convert GreptimeRow to TradeTick
    #[cfg(feature = "greptime")]
    fn greptime_row_to_trade_tick(row: &crate::sql::greptime_client::GreptimeRow) -> anyhow::Result<TradeTick> {
        use crate::sql::greptime_client::GreptimeValue;
        use nautilus_model::{types::{Price, Quantity}, enums::AggressorSide, identifiers::TradeId};
        use nautilus_core::UnixNanos;
        
        let get_value = |col: &str| -> anyhow::Result<&GreptimeValue> {
            row.data.get(col)
                .ok_or_else(|| anyhow::anyhow!("Missing column: {}", col))
        };
        
        let timestamp = match get_value("timestamp")? {
            GreptimeValue::Timestamp(ts) => UnixNanos::from(*ts as u64),
            _ => anyhow::bail!("timestamp must be Timestamp type"),
        };
        
        let instrument_id = match get_value("instrument_id")? {
            GreptimeValue::String(s) => InstrumentId::from(s.as_str()),
            _ => anyhow::bail!("instrument_id must be String type"),
        };
        
        let price = match get_value("price")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse price: {}", e))?,
            _ => anyhow::bail!("price must be Float64 type"),
        };
        
        let quantity = match get_value("quantity")? {
            GreptimeValue::Float64(v) => Quantity::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse quantity: {}", e))?,
            _ => anyhow::bail!("quantity must be Float64 type"),
        };
        
        let aggressor_side = match get_value("aggressor_side")? {
            GreptimeValue::String(s) => {
                match s.as_str() {
                    "BUYER" | "Buyer" | "buyer" => AggressorSide::Buyer,
                    "SELLER" | "Seller" | "seller" => AggressorSide::Seller,
                    "NO_AGGRESSOR" | "NoAggressor" | "no_aggressor" => AggressorSide::NoAggressor,
                    _ => anyhow::bail!("Invalid aggressor_side: {}", s),
                }
            },
            _ => anyhow::bail!("aggressor_side must be String type"),
        };
        
        let trade_id = match get_value("trade_id")? {
            GreptimeValue::String(s) => TradeId::from(s.as_str()),
            _ => anyhow::bail!("trade_id must be String type"),
        };
        
        Ok(TradeTick::new(
            instrument_id,
            price,
            quantity,
            aggressor_side,
            trade_id,
            timestamp,
            timestamp, // Use ts_event as ts_init
        ))
    }
    
    /// Convert GreptimeRow to Bar
    #[cfg(feature = "greptime")]
    fn greptime_row_to_bar(row: &crate::sql::greptime_client::GreptimeRow) -> anyhow::Result<Bar> {
        use crate::sql::greptime_client::GreptimeValue;
        use nautilus_model::{types::{Price, Quantity}, data::bar::BarType};
        use nautilus_core::UnixNanos;
        use std::str::FromStr;
        
        let get_value = |col: &str| -> anyhow::Result<&GreptimeValue> {
            row.data.get(col)
                .ok_or_else(|| anyhow::anyhow!("Missing column: {}", col))
        };
        
        let timestamp = match get_value("timestamp")? {
            GreptimeValue::Timestamp(ts) => UnixNanos::from(*ts as u64),
            _ => anyhow::bail!("timestamp must be Timestamp type"),
        };
        
        let bar_type_str = match get_value("bar_type")? {
            GreptimeValue::String(s) => s.clone(),
            _ => anyhow::bail!("bar_type must be String type"),
        };
        
        let bar_type = BarType::from_str(&bar_type_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse bar_type '{}': {:?}", bar_type_str, e))?;
        
        let open = match get_value("open")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse open: {}", e))?,
            _ => anyhow::bail!("open must be Float64 type"),
        };
        
        let high = match get_value("high")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse high: {}", e))?,
            _ => anyhow::bail!("high must be Float64 type"),
        };
        
        let low = match get_value("low")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse low: {}", e))?,
            _ => anyhow::bail!("low must be Float64 type"),
        };
        
        let close = match get_value("close")? {
            GreptimeValue::Float64(v) => Price::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse close: {}", e))?,
            _ => anyhow::bail!("close must be Float64 type"),
        };
        
        let volume = match get_value("volume")? {
            GreptimeValue::Float64(v) => Quantity::from_str(&format!("{:.8}", v))
                .map_err(|e| anyhow::anyhow!("Failed to parse volume: {}", e))?,
            _ => anyhow::bail!("volume must be Float64 type"),
        };
        
        Ok(Bar::new(
            bar_type,
            open,
            high,
            low,
            close,
            volume,
            timestamp,
            timestamp, // Use ts_event as ts_init
        ))
    }
    
    /// Convert GreptimeRow to Signal
    #[cfg(feature = "greptime")]
    fn greptime_row_to_signal(row: &crate::sql::greptime_client::GreptimeRow) -> anyhow::Result<Signal> {
        use crate::sql::greptime_client::GreptimeValue;
        use nautilus_core::UnixNanos;
        use ustr::Ustr;
        
        let get_value = |col: &str| -> anyhow::Result<&GreptimeValue> {
            row.data.get(col)
                .ok_or_else(|| anyhow::anyhow!("Missing column: {}", col))
        };
        
        let timestamp = match get_value("timestamp")? {
            GreptimeValue::Timestamp(ts) => UnixNanos::from(*ts as u64),
            _ => anyhow::bail!("timestamp must be Timestamp type"),
        };
        
        let signal_name = match get_value("signal_name")? {
            GreptimeValue::String(s) => Ustr::from(s.as_str()),
            _ => anyhow::bail!("signal_name must be String type"),
        };
        
        let signal_value = match get_value("signal_value")? {
            GreptimeValue::String(s) => s.clone(),
            GreptimeValue::Float64(v) => format!("{:.8}", v),
            _ => anyhow::bail!("signal_value must be String or Float64 type"),
        };
        
        Ok(Signal::new(
            signal_name,
            signal_value,
            timestamp,
            timestamp, // Use ts_event as ts_init
        ))
    }
    
    /// Convert GreptimeRow to CustomData
    #[cfg(feature = "greptime")]
    fn greptime_row_to_custom_data(row: &crate::sql::greptime_client::GreptimeRow) -> anyhow::Result<CustomData> {
        use crate::sql::greptime_client::GreptimeValue;
        use nautilus_model::data::DataType;
        use nautilus_core::UnixNanos;
        use bytes::Bytes;
        
        let get_value = |col: &str| -> anyhow::Result<&GreptimeValue> {
            row.data.get(col)
                .ok_or_else(|| anyhow::anyhow!("Missing column: {}", col))
        };
        
        let timestamp = match get_value("timestamp")? {
            GreptimeValue::Timestamp(ts) => UnixNanos::from(*ts as u64),
            _ => anyhow::bail!("timestamp must be Timestamp type"),
        };
        
        let data_type_str = match get_value("data_type")? {
            GreptimeValue::String(s) => s.clone(),
            _ => anyhow::bail!("data_type must be String type"),
        };
        
        let data_type = DataType::new(&data_type_str, None);
        
        let value_str = match get_value("value")? {
            GreptimeValue::String(s) => s.clone(),
            _ => anyhow::bail!("value must be String type (JSON)"),
        };
        
        // Parse JSON string back to bytes
        let value = Bytes::from(value_str.into_bytes());
        
        Ok(CustomData::new(
            data_type,
            value,
            timestamp,
            timestamp, // Use ts_event as ts_init
        ))
    }
    
    /// Health check for PostgreSQL connection
    pub async fn health_check_postgres(&self) -> anyhow::Result<bool> {
        sqlx::query("SELECT 1")
            .execute(&self.pg_pool)
            .await
            .map(|_| true)
            .map_err(|e| anyhow::anyhow!("PostgreSQL health check failed: {}", e))
    }
    
    /// Health check for GreptimeDB connection
    #[cfg(feature = "greptime")]
    pub async fn health_check_greptime(&self) -> anyhow::Result<bool> {
        // Try to create a test client and query
        let client = crate::sql::greptime_client::GreptimeClient::new(
            &self.config.greptime_host,
            self.config.greptime_port,
            &self.config.greptime_database,
            self.config.greptime_username.clone(),
            self.config.greptime_password.clone(),
        ).await?;
        
        // Simple query to test connection
        client.query("SELECT 1").await?;
        Ok(true)
    }
    
    #[cfg(not(feature = "greptime"))]
    pub async fn health_check_greptime(&self) -> anyhow::Result<bool> {
        anyhow::bail!("GreptimeDB feature is not enabled")
    }
    
    /// Combined health check for both databases
    pub async fn health_check(&self) -> anyhow::Result<(bool, bool)> {
        let pg_health = self.health_check_postgres().await.unwrap_or(false);
        let gt_health = self.health_check_greptime().await.unwrap_or(false);
        Ok((pg_health, gt_health))
    }
}