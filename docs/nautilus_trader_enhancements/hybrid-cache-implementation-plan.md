# HybridCacheDatabaseAdaptor Implementation Plan

## Overview

Create a `HybridCacheDatabaseAdaptor` that combines PostgreSQL for relational data with ArcticDB for time series data, leveraging the strengths of each database system.

## Data Storage Strategy

### PostgreSQL (Relational Data)
- **Currencies**: Metadata, codes, precision
- **Instruments**: All instrument types and their specifications
- **Accounts**: Account states and balances
- **Orders**: Order lifecycle and events
- **Positions**: Position states and snapshots
- **Order/Position Snapshots**: Point-in-time state captures
- **General Cache Items**: Key-value pairs for system state

### ArcticDB (Time Series Data)
- **QuoteTicks**: Bid/ask price and volume data
- **TradeTicks**: Executed trade data
- **Bars**: OHLCV aggregated data
- **Signals**: Trading signals with timestamps
- **CustomData**: User-defined time series data

## Architecture Design

### Core Structure

```rust
#[derive(Debug)]
pub struct HybridCacheDatabaseAdaptor {
    // PostgreSQL for relational data
    pg_pool: PgPool,
    pg_tx: tokio::sync::mpsc::UnboundedSender<PostgresQuery>,
    pg_handle: tokio::task::JoinHandle<()>,
    
    // ArcticDB for time series data
    arctic_client: ArcticClient,
    arctic_tx: tokio::sync::mpsc::UnboundedSender<ArcticQuery>,
    arctic_handle: tokio::task::JoinHandle<()>,
    
    // Configuration
    config: HybridCacheConfig,
}
```

### Configuration

```rust
#[derive(Debug, Clone)]
pub struct HybridCacheConfig {
    // PostgreSQL configuration
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_username: String,
    pub pg_password: String,
    pub pg_database: String,
    
    // ArcticDB configuration
    pub arctic_uri: String,           // e.g., "s3://bucket/path?region=us-east-1"
    pub arctic_library: String,      // Library name for nautilus data
    pub arctic_aws_access_key: Option<String>,
    pub arctic_aws_secret_key: Option<String>,
    pub arctic_aws_region: String,
    
    // Performance tuning
    pub buffer_interval_ms: u64,
    pub batch_size: usize,
}
```

## Query Routing

### PostgreSQL Query Types
```rust
#[derive(Debug, Clone)]
pub enum PostgresQuery {
    Close,
    // Core relational data
    AddCurrency(Currency),
    AddInstrument(InstrumentAny),
    AddAccount(AccountAny, bool),
    AddOrder(OrderAny, Option<ClientId>, bool),
    AddOrderSnapshot(OrderSnapshot),
    AddPositionSnapshot(PositionSnapshot),
    UpdateOrder(OrderEventAny),
    // General cache items
    Add(String, Vec<u8>),
}
```

### ArcticDB Query Types
```rust
#[derive(Debug, Clone)]
pub enum ArcticQuery {
    Close,
    // Time series data
    AddQuote(QuoteTick),
    AddTrade(TradeTick),
    AddBar(Bar),
    AddSignal(Signal),
    AddCustomData(CustomData),
    // Batch operations for performance
    AddQuoteBatch(Vec<QuoteTick>),
    AddTradeBatch(Vec<TradeTick>),
    AddBarBatch(Vec<Bar>),
}
```

## Implementation Details

### 1. Connection Management

```rust
impl HybridCacheDatabaseAdaptor {
    pub async fn connect(config: HybridCacheConfig) -> Result<Self, anyhow::Error> {
        // Initialize PostgreSQL connection
        let pg_connect_options = get_postgres_connect_options(
            Some(config.pg_host.clone()),
            Some(config.pg_port),
            Some(config.pg_username.clone()),
            Some(config.pg_password.clone()),
            Some(config.pg_database.clone()),
        );
        let pg_pool = connect_pg(pg_connect_options.clone().into()).await?;
        
        // Initialize ArcticDB connection
        let arctic_client = ArcticClient::new(&config.arctic_uri)?
            .with_library(&config.arctic_library)?;
        
        // Set up message channels
        let (pg_tx, pg_rx) = tokio::sync::mpsc::unbounded_channel();
        let (arctic_tx, arctic_rx) = tokio::sync::mpsc::unbounded_channel();
        
        // Spawn processing tasks
        let pg_handle = tokio::spawn(async move {
            Self::process_postgres_commands(pg_rx, pg_connect_options.into()).await;
        });
        
        let arctic_handle = tokio::spawn(async move {
            Self::process_arctic_commands(arctic_rx, arctic_client).await;
        });
        
        Ok(Self {
            pg_pool,
            pg_tx,
            pg_handle,
            arctic_client,
            arctic_tx,
            arctic_handle,
            config,
        })
    }
}
```

### 2. ArcticDB Schema Design

```rust
// Symbol naming convention for ArcticDB
fn get_arctic_symbol(instrument_id: &InstrumentId, data_type: &str) -> String {
    format!("{}/{}", instrument_id, data_type)
}

// Examples:
// - "EUR/USD.IDEALPRO/quotes"
// - "AAPL.NASDAQ/trades" 
// - "ES.CME/bars_1min"
// - "my_strategy/signals"
```

### 3. Time Series Data Handlers

```rust
impl HybridCacheDatabaseAdaptor {
    // ArcticDB operations
    async fn process_arctic_commands(
        mut rx: tokio::sync::mpsc::UnboundedReceiver<ArcticQuery>,
        client: ArcticClient,
    ) {
        let mut buffer: VecDeque<ArcticQuery> = VecDeque::new();
        let mut last_drain = Instant::now();
        let buffer_interval = Duration::from_millis(100); // Configurable
        
        loop {
            if last_drain.elapsed() >= buffer_interval && !buffer.is_empty() {
                Self::drain_arctic_buffer(&client, &mut buffer).await;
                last_drain = Instant::now();
            } else {
                match rx.recv().await {
                    Some(ArcticQuery::Close) => break,
                    Some(query) => buffer.push_back(query),
                    None => break,
                }
            }
        }
        
        // Final drain
        if !buffer.is_empty() {
            Self::drain_arctic_buffer(&client, &mut buffer).await;
        }
    }
    
    async fn drain_arctic_buffer(
        client: &ArcticClient,
        buffer: &mut VecDeque<ArcticQuery>,
    ) {
        // Group queries by symbol for batch writing
        let mut quote_batches: HashMap<String, Vec<QuoteTick>> = HashMap::new();
        let mut trade_batches: HashMap<String, Vec<TradeTick>> = HashMap::new();
        let mut bar_batches: HashMap<String, Vec<Bar>> = HashMap::new();
        
        for query in buffer.drain(..) {
            match query {
                ArcticQuery::AddQuote(quote) => {
                    let symbol = get_arctic_symbol(&quote.instrument_id, "quotes");
                    quote_batches.entry(symbol).or_default().push(quote);
                }
                ArcticQuery::AddTrade(trade) => {
                    let symbol = get_arctic_symbol(&trade.instrument_id, "trades");
                    trade_batches.entry(symbol).or_default().push(trade);
                }
                ArcticQuery::AddBar(bar) => {
                    let symbol = get_arctic_symbol(&bar.instrument_id, "bars");
                    bar_batches.entry(symbol).or_default().push(bar);
                }
                // Handle other types...
                _ => {}
            }
        }
        
        // Write batches to ArcticDB
        for (symbol, quotes) in quote_batches {
            if let Err(e) = Self::write_quotes_to_arctic(client, &symbol, quotes).await {
                tracing::error!("Failed to write quotes to Arctic: {}", e);
            }
        }
        
        // Similar for trades and bars...
    }
    
    async fn write_quotes_to_arctic(
        client: &ArcticClient,
        symbol: &str,
        quotes: Vec<QuoteTick>,
    ) -> Result<(), anyhow::Error> {
        // Convert QuoteTicks to DataFrame
        let df = Self::quotes_to_dataframe(quotes)?;
        
        // Write to ArcticDB with append mode
        client.write(symbol, df).append().await?;
        
        Ok(())
    }
    
    fn quotes_to_dataframe(quotes: Vec<QuoteTick>) -> Result<DataFrame, anyhow::Error> {
        // Convert quotes to columnar format for ArcticDB
        let timestamps: Vec<i64> = quotes.iter().map(|q| q.ts_event).collect();
        let bid_prices: Vec<f64> = quotes.iter().map(|q| q.bid_price.raw as f64).collect();
        let ask_prices: Vec<f64> = quotes.iter().map(|q| q.ask_price.raw as f64).collect();
        let bid_sizes: Vec<f64> = quotes.iter().map(|q| q.bid_size.raw as f64).collect();
        let ask_sizes: Vec<f64> = quotes.iter().map(|q| q.ask_size.raw as f64).collect();
        
        // Create DataFrame (using polars or similar)
        let df = DataFrame::new(vec![
            Column::new("timestamp".into(), timestamps),
            Column::new("bid_price".into(), bid_prices),
            Column::new("ask_price".into(), ask_prices),
            Column::new("bid_size".into(), bid_sizes),
            Column::new("ask_size".into(), ask_sizes),
        ])?;
        
        Ok(df)
    }
}
```

### 4. CacheDatabaseAdapter Implementation

```rust
#[async_trait::async_trait]
impl CacheDatabaseAdapter for HybridCacheDatabaseAdaptor {
    // Relational data methods route to PostgreSQL
    async fn load_currencies(&self) -> anyhow::Result<HashMap<Ustr, Currency>> {
        // Delegate to PostgreSQL implementation
        PostgresCacheDatabase::load_currencies(&self.pg_pool).await
    }
    
    async fn load_instruments(&self) -> anyhow::Result<HashMap<InstrumentId, InstrumentAny>> {
        // Delegate to PostgreSQL implementation
        PostgresCacheDatabase::load_instruments(&self.pg_pool).await
    }
    
    // Time series data methods route to ArcticDB
    fn load_quotes(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<QuoteTick>> {
        let symbol = get_arctic_symbol(instrument_id, "quotes");
        
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                let df = self.arctic_client.read(&symbol).await?;
                Self::dataframe_to_quotes(df)
            })
        })
    }
    
    fn load_trades(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<TradeTick>> {
        let symbol = get_arctic_symbol(instrument_id, "trades");
        
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                let df = self.arctic_client.read(&symbol).await?;
                Self::dataframe_to_trades(df)
            })
        })
    }
    
    fn load_bars(&self, instrument_id: &InstrumentId) -> anyhow::Result<Vec<Bar>> {
        let symbol = get_arctic_symbol(instrument_id, "bars");
        
        tokio::task::block_in_place(|| {
            get_runtime().block_on(async {
                let df = self.arctic_client.read(&symbol).await?;
                Self::dataframe_to_bars(df)
            })
        })
    }
    
    // Add methods route to appropriate backend
    fn add_currency(&self, currency: &Currency) -> anyhow::Result<()> {
        let query = PostgresQuery::AddCurrency(*currency);
        self.pg_tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send PostgreSQL query: {e}"))
    }
    
    fn add_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        let query = ArcticQuery::AddQuote(quote.clone());
        self.arctic_tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send ArcticDB query: {e}"))
    }
    
    fn add_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        let query = ArcticQuery::AddTrade(trade.clone());
        self.arctic_tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send ArcticDB query: {e}"))
    }
    
    fn add_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        let query = ArcticQuery::AddBar(bar.clone());
        self.arctic_tx.send(query).map_err(|e| anyhow::anyhow!("Failed to send ArcticDB query: {e}"))
    }
}
```

## Dependencies and Integration

### Cargo.toml Dependencies
```toml
[dependencies]
# Existing dependencies
sqlx = { version = "0.8.3", features = ["postgres", "runtime-tokio", "json"] }
tokio = { version = "1.44.1", features = ["full"] }

# New ArcticDB dependencies
arcticdb = "0.8.0"  # Or latest version
polars = { version = "0.35.0", features = ["lazy"] }  # For DataFrame operations
aws-sdk-s3 = "0.35.0"  # If using S3 backend
```

### Configuration Integration
```rust
// Add to existing configuration structures
#[derive(Debug, Clone)]
pub enum CacheBackend {
    PostgresOnly,
    ArcticOnly,
    Hybrid(HybridCacheConfig),
}
```

## Performance Optimizations

### 1. Batch Writing
- Buffer time series data for batch writes to ArcticDB
- Configurable batch size and interval
- Separate batches per instrument for parallel writes

### 2. Connection Pooling
- Maintain connection pools for both backends
- Implement connection health checks
- Automatic reconnection logic

### 3. Async Query Routing
- Non-blocking query routing to appropriate backend
- Parallel execution of independent queries
- Query result caching for frequently accessed data

### 4. Memory Management
- Stream large result sets instead of loading entirely into memory
- Configurable result set size limits
- Lazy loading for historical data

## Migration Strategy

### Phase 1: Development
1. Implement `HybridCacheDatabaseAdaptor` alongside existing PostgreSQL adapter
2. Add configuration option to choose cache backend
3. Comprehensive testing with both backends

### Phase 2: Deployment
1. Deploy with PostgreSQL as primary, ArcticDB as secondary
2. Implement data migration tools for existing time series data
3. Gradual rollout with feature flags

### Phase 3: Optimization
1. Performance benchmarking and optimization
2. Advanced ArcticDB features (compression, indexing)
3. Query optimization for hybrid workloads

## Testing Strategy

### Unit Tests
- Test each backend independently
- Test query routing logic
- Test data conversion functions

### Integration Tests
- End-to-end tests with both backends
- Performance tests comparing with PostgreSQL-only
- Failure scenario testing (connection losses, partial writes)

### Load Tests
- High-frequency data ingestion tests
- Concurrent read/write operations
- Memory usage and performance profiling

## Benefits

1. **Performance**: ArcticDB's columnar storage for time series data
2. **Scalability**: S3-backed storage scales virtually unlimited
3. **Cost Efficiency**: Separate storage tiers for different data types
4. **Query Performance**: Optimized backends for their respective data types
5. **Cloud Native**: Direct S3 integration for cloud deployments
6. **Compression**: Excellent compression ratios for time series data

This implementation provides a robust, scalable solution that leverages the best of both database technologies while maintaining compatibility with the existing NautilusTrader architecture.

## Database Initialization

### Modified CLI Commands

You'll need to extend the existing `nautilus database` CLI to support hybrid initialization:

```rust
// Add to crates/cli/src/opt.rs
#[derive(Parser, Debug, Clone)]
pub enum DatabaseCommand {
    /// Initializes a new Postgres database with the latest schema.
    Init(DatabaseConfig),
    /// Initializes hybrid cache with PostgreSQL + ArcticDB
    InitHybrid(HybridDatabaseConfig),
    /// Drops roles, privileges and deletes all data from the database.
    Drop(DatabaseConfig),
    /// Drops both PostgreSQL and ArcticDB data
    DropHybrid(HybridDatabaseConfig),
}

#[derive(Parser, Debug, Clone)]
pub struct HybridDatabaseConfig {
    // PostgreSQL settings
    #[arg(long)]
    pub pg_host: Option<String>,
    #[arg(long)]
    pub pg_port: Option<u16>,
    #[arg(long)]
    pub pg_username: Option<String>,
    #[arg(long)]
    pub pg_database: Option<String>,
    #[arg(long)]
    pub pg_password: Option<String>,
    
    // ArcticDB settings
    #[arg(long)]
    pub arctic_uri: Option<String>,
    #[arg(long)]
    pub arctic_library: Option<String>,
    #[arg(long)]
    pub arctic_aws_access_key: Option<String>,
    #[arg(long)]
    pub arctic_aws_secret_key: Option<String>,
    #[arg(long)]
    pub arctic_aws_region: Option<String>,
    
    #[arg(long)]
    pub schema: Option<String>,
}
```

### Modified Schema Files

Create new schema files that separate relational and time series data:

**schema/sql/hybrid_tables.sql** (PostgreSQL - Relational Data Only):
```sql
-- Remove time series tables: quote, trade, bar, signal, custom
-- Keep: general, trader, account, client, strategy, currency, instrument, 
--       order, order_event, position, account_event
```

**schema/arctic/init_arctic.py** (ArcticDB Initialization):
```python
import arcticdb as adb
from pathlib import Path

def init_arctic_library(config: HybridCacheConfig):
    """Initialize ArcticDB library with proper schemas"""
    
    # Connect to ArcticDB
    ac = adb.Arctic(config.arctic_uri)
    
    # Create or get library
    try:
        lib = ac.create_library(config.arctic_library)
    except:
        lib = ac[config.arctic_library]
    
    # Define schemas for different data types
    quote_schema = {
        'timestamp': 'datetime64[ns]',
        'bid_price': 'float64',
        'ask_price': 'float64', 
        'bid_size': 'float64',
        'ask_size': 'float64'
    }
    
    trade_schema = {
        'timestamp': 'datetime64[ns]',
        'price': 'float64',
        'quantity': 'float64',
        'aggressor_side': 'string'
    }
    
    bar_schema = {
        'timestamp': 'datetime64[ns]',
        'open': 'float64',
        'high': 'float64', 
        'low': 'float64',
        'close': 'float64',
        'volume': 'float64'
    }
    
    # Create index for performance
    for schema_name, schema in [('quotes', quote_schema), ('trades', trade_schema), ('bars', bar_schema)]:
        # ArcticDB automatically handles indexing on timestamp
        print(f"Schema {schema_name} ready for {config.arctic_library}")
    
    return lib
```

### Hybrid Database Initialization Function

```rust
// Add to crates/cli/src/database/postgres.rs or new hybrid.rs

use arcticdb_python::PyArcticDB; // Hypothetical Python binding

pub async fn run_hybrid_database_command(opt: HybridDatabaseConfig) -> anyhow::Result<()> {
    match opt.command {
        DatabaseCommand::InitHybrid(config) => {
            // 1. Initialize PostgreSQL with hybrid schema
            let pg_connect_options = get_postgres_connect_options(
                config.pg_host,
                config.pg_port, 
                config.pg_username,
                config.pg_password,
                config.pg_database,
            );
            let pg = connect_pg(pg_connect_options.clone().into()).await?;
            
            // Use modified schema without time series tables
            let hybrid_schema_path = config.schema.unwrap_or_else(|| "schema/sql/hybrid_tables.sql".to_string());
            init_postgres_hybrid(&pg, pg_connect_options.database, pg_connect_options.password, Some(hybrid_schema_path)).await?;
            
            // 2. Initialize ArcticDB
            init_arctic_db(&config).await?;
            
            log::info!("Hybrid cache database initialized successfully");
        }
        DatabaseCommand::DropHybrid(config) => {
            // Drop both PostgreSQL and ArcticDB data
            drop_hybrid_databases(&config).await?;
        }
        _ => {} // Handle other commands
    }
    Ok(())
}

async fn init_arctic_db(config: &HybridDatabaseConfig) -> anyhow::Result<()> {
    // Call Python initialization script or use Rust Arctic bindings when available
    let arctic_uri = config.arctic_uri.as_ref().ok_or_else(|| anyhow::anyhow!("Arctic URI required"))?;
    let library = config.arctic_library.as_ref().ok_or_else(|| anyhow::anyhow!("Arctic library name required"))?;
    
    // For now, shell out to Python script
    let script_path = "schema/arctic/init_arctic.py";
    let output = std::process::Command::new("python")
        .arg(script_path)
        .arg("--uri").arg(arctic_uri)
        .arg("--library").arg(library)
        .output()?;
        
    if !output.status.success() {
        return Err(anyhow::anyhow!("Failed to initialize ArcticDB: {}", String::from_utf8_lossy(&output.stderr)));
    }
    
    log::info!("ArcticDB library '{}' initialized", library);
    Ok(())
}
```

### Usage Examples

```bash
# Initialize hybrid cache with local PostgreSQL and S3-backed ArcticDB
nautilus database init-hybrid \
    --pg-host localhost \
    --pg-port 5432 \
    --pg-username postgres \
    --pg-password pass \
    --pg-database nautilus \
    --arctic-uri "s3://my-bucket/nautilus-data?region=us-east-1" \
    --arctic-library nautilus_timeseries \
    --arctic-aws-region us-east-1

# Or use environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432  
export POSTGRES_USERNAME=postgres
export POSTGRES_PASSWORD=pass
export POSTGRES_DATABASE=nautilus
export ARCTIC_URI="s3://my-bucket/nautilus-data?region=us-east-1"
export ARCTIC_LIBRARY=nautilus_timeseries
export ARCTIC_AWS_REGION=us-east-1

nautilus database init-hybrid
```

### Environment Variables Support

Add to your `.env` file:
```bash
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=pass
POSTGRES_DATABASE=nautilus

# ArcticDB Configuration  
ARCTIC_URI=s3://my-trading-bucket/nautilus-arctic?region=us-east-1
ARCTIC_LIBRARY=nautilus_timeseries
ARCTIC_AWS_ACCESS_KEY=your_access_key
ARCTIC_AWS_SECRET_KEY=your_secret_key
ARCTIC_AWS_REGION=us-east-1

# Cache Configuration
CACHE_BACKEND=hybrid
CACHE_BUFFER_INTERVAL_MS=100
CACHE_BATCH_SIZE=1000
```

This extended implementation provides a robust, scalable solution that leverages the best of both database technologies while maintaining compatibility with the existing NautilusTrader architecture and extending the familiar CLI interface that follows the existing `nautilus database init` pattern.