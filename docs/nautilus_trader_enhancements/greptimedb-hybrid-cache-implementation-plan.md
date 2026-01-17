# Com

plete Hybrid Cache Implementation with GreptimeDB

## Overview

Complete the `ArcticDbPostgresHybridCacheAdapter` implementation (renamed to `GreptimePostgresHybridCacheAdapter`) as a native Nautilus Trader module. This involves:

1. Setting up GreptimeDB as a standalone process
2. Integrating GreptimeDB Rust client library
3. Completing all PostgreSQL methods (currently stubbed)
4. Completing all GreptimeDB time-series methods
5. Implementing all load methods to read from both backends
6. Adding PyO3 Python bindings for Nautilus Trader integration
7. Testing and integration

## Architecture

The hybrid cache routes data based on type:

- **PostgreSQL** (`fincept_terminal` schema): Relational data (currencies, instruments, accounts, orders, positions, snapshots, general cache)
- **GreptimeDB** (standalone process): Time-series data (quotes, trades, bars, signals, custom data)

### GreptimeDB Deployment Model

- **Standalone Process**: GreptimeDB runs as a separate binary on the same server
- **gRPC Interface**: Communication via gRPC for low-latency, high-performance access
- **Scalability**: Can be migrated to a cluster when resource needs grow
- **Resource Monitoring**: Monitor server usage to determine when to scale out

## Implementation Phases

### Phase 1: GreptimeDB Setup and Rust Client Integration

**Goal**: Set up GreptimeDB and integrate Rust client library**Tasks**:

1. **Install GreptimeDB**:

- Download GreptimeDB standalone binary
- Create deployment script for local development
- Configure GreptimeDB for time-series data storage
- Set up connection configuration (host, port, database name)

2. **Add GreptimeDB Rust Client Dependency**:

- File: `apps/hydra-naut-trader/crates/infrastructure/Cargo.toml` or `apps/hydra-naut-trader/crates/persistence/Cargo.toml`
- Add `greptimedb-rs` crate dependency
- Configure gRPC connection settings

3. **Create GreptimeDB Client Wrapper**:

- File: `apps/hydra-naut-trader/crates/infrastructure/src/sql/greptime_client.rs` (new)
- Implement connection management (gRPC client)
- Implement write operations (INSERT statements via gRPC)
- Implement read operations (SELECT queries via gRPC)
- Handle symbol/table naming with user/exchange isolation
- Support batch writes for performance

4. **Data Schema Design**:

- Define GreptimeDB table schemas for:
    - Quotes: `timestamp`, `instrument_id`, `bid_price`, `ask_price`, `bid_size`, `ask_size`
    - Trades: `timestamp`, `instrument_id`, `price`, `quantity`, `aggressor_side`, `trade_id`
    - Bars: `timestamp`, `instrument_id`, `bar_type`, `open`, `high`, `low`, `close`, `volume`
    - Signals: `timestamp`, `signal_name`, `signal_value`, `metadata`
- Use GreptimeDB's automatic schema generation where possible
- Add indexes for performance (timestamp, instrument_id)

**Files to create/modify**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/greptime_client.rs` (new)
- `apps/hydra-naut-trader/crates/infrastructure/src/sql/mod.rs` (add module)
- `apps/hydra-naut-trader/crates/infrastructure/Cargo.toml` (add dependency)
- `scripts/setup_greptime.sh` (new - deployment script)

**Dependencies to add**:

```toml
greptimedb-rs = "0.4"  # Or latest version
tonic = "0.11"  # gRPC runtime (if not already present)
prost = "0.12"  # Protocol buffers (if needed)
```



### Phase 2: Update Hybrid Cache Adapter Structure

**Goal**: Rename and restructure the adapter for GreptimeDB**Tasks**:

1. **Rename adapter and configuration**:

- Rename `ArcticDbPostgresHybridCacheAdapter` → `GreptimePostgresHybridCacheAdapter`
- Rename `ArcticDbPostgresHybridCacheConfig` → `GreptimePostgresHybridCacheConfig`
- Update all references throughout the codebase

2. **Update configuration structure**:

- Replace ArcticDB URI with GreptimeDB connection settings:
    - `greptime_host: String` (default: "localhost")
    - `greptime_port: u16` (default: 4001 for gRPC)
    - `greptime_database: String` (default: "nautilus_timeseries")
    - `greptime_username: Option<String>`
    - `greptime_password: Option<String>`
- Remove ArcticDB-specific fields (S3, AWS credentials)

3. **Update query enum**:

- Rename `ArcticDbPostgresHybridQuery` → `GreptimePostgresHybridQuery`
- Keep same structure but update comments

**Files to modify**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/arcticdb_postgres_hybrid_cache.rs` (rename and update)
- Consider renaming file to `greptime_postgres_hybrid_cache.rs`

### Phase 3: Complete PostgreSQL Methods

**Goal**: Implement all stubbed PostgreSQL operations**Tasks**:

1. **General cache storage** (`add_general_to_postgres`):

- Store key-value pairs in `nautilus_general` table
- Handle user context if needed

2. **Account operations** (`add_account_to_postgres`, `load_accounts`, `load_account`):

- Store accounts in `nautilus_accounts` table
- Map `AccountAny` to database schema
- Load and reconstruct `AccountAny` from database
- Reference: `apps/hydra-naut-trader/crates/infrastructure/src/sql/cache.rs` for patterns

3. **Order operations** (`add_order_to_postgres`, `update_order_in_postgres`, `load_orders`, `load_order`):

- Store orders in `nautilus_orders` table
- Store order events in `nautilus_order_events` table
- Handle order snapshots
- Map `OrderAny` and `OrderEventAny` to database schema

4. **Position operations** (`load_positions`, `load_position`):

- Store positions in `nautilus_positions` table
- Handle position snapshots
- Map `Position` to database schema

5. **Instrument operations** (`load_instruments`, `load_instrument`):

- Load from `nautilus_instruments` table
- Reconstruct `InstrumentAny` from database rows
- Handle different instrument types (Spot, Future, Option, etc.)

6. **Synthetic instruments** (`load_synthetics`, `load_synthetic`):

- Store/load synthetic instruments if needed
- May be optional depending on requirements

**Reference implementation**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/cache.rs` - existing PostgreSQL cache implementation
- `apps/hydra-naut-trader/crates/infrastructure/src/sql/queries.rs` - database query helpers

### Phase 4: Complete GreptimeDB Time-Series Methods

**Goal**: Implement all time-series data operations using GreptimeDB**Tasks**:

1. **Quote operations** (`add_quote_hybrid`, `load_quotes`):

- Write quotes to GreptimeDB table: `user_{user_id}_quotes_exchange_{exchange_id}`
- Use INSERT statements via gRPC client
- Batch writes for performance (collect multiple quotes, send in single request)
- Read quotes with time-range filtering using SQL queries
- Convert between `QuoteTick` and GreptimeDB row format

2. **Trade operations** (`add_trade_hybrid`, `load_trades`):

- Write trades to GreptimeDB: `user_{user_id}_trades_exchange_{exchange_id}`
- Batch writes
- Read trades with time-range filtering

3. **Bar operations** (`add_bar_hybrid`, `load_bars`):

- Write bars to GreptimeDB: `user_{user_id}_bars_exchange_{exchange_id}`
- Handle different bar types and aggregations
- Read bars with time-range filtering

4. **Signal operations** (`add_signal_hybrid`, `load_signals`):

- Write signals to GreptimeDB: `user_{user_id}_signals`
- Read signals by name with time filtering

5. **Custom data operations** (`add_custom_data_hybrid`, `load_custom_data`):

- Write custom data to GreptimeDB
- Read custom data by type

**Implementation Pattern**:

```rust
// Example: Writing quotes to GreptimeDB
async fn add_quote_to_greptime(
    client: &GreptimeClient,
    user_id: Uuid,
    exchange_id: i32,
    quote: QuoteTick,
) -> anyhow::Result<()> {
    let table_name = format!("user_{}_quotes_exchange_{}", user_id, exchange_id);
    
    // Convert QuoteTick to GreptimeDB row format
    let row = GreptimeRow {
        timestamp: quote.ts_event.as_i64(),
        instrument_id: quote.instrument_id.to_string(),
        bid_price: quote.bid_price.as_f64(),
        ask_price: quote.ask_price.as_f64(),
        bid_size: quote.bid_size.as_f64(),
        ask_size: quote.ask_size.as_f64(),
    };
    
    // Insert via gRPC
    client.insert(&table_name, row).await?;
    Ok(())
}
```

**Files to modify**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/greptime_postgres_hybrid_cache.rs` (lines 432-614, update for GreptimeDB)

### Phase 5: Complete Load Methods

**Goal**: Implement all `load_*` methods to read from both backends**Tasks**:

1. **Time-series load methods**:

- `load_quotes`: Read from GreptimeDB using SQL query with time range
- `load_trades`: Read from GreptimeDB
- `load_bars`: Read from GreptimeDB
- `load_signals`: Read from GreptimeDB
- `load_custom_data`: Read from GreptimeDB

2. **Relational load methods**:

- `load_instruments`: Read from PostgreSQL, reconstruct `InstrumentAny`
- `load_accounts`: Read from PostgreSQL, reconstruct `AccountAny`
- `load_orders`: Read from PostgreSQL, reconstruct `OrderAny`
- `load_positions`: Read from PostgreSQL, reconstruct `Position`
- `load_currencies`: Already implemented, verify completeness

3. **Index and snapshot methods**:

- `load_index_order_position`: Read from PostgreSQL
- `load_index_order_client`: Read from PostgreSQL
- `load_order_snapshot`: Read from PostgreSQL
- `load_position_snapshot`: Read from PostgreSQL

4. **Actor and strategy methods**:

- `load_actor`: Read from PostgreSQL `nautilus_general` table
- `load_strategy`: Read from PostgreSQL `nautilus_general` table

**GreptimeDB Query Pattern**:

```rust
// Example: Loading quotes from GreptimeDB
async fn load_quotes_from_greptime(
    client: &GreptimeClient,
    user_id: Uuid,
    exchange_id: i32,
    instrument_id: &InstrumentId,
    start_time: Option<i64>,
    end_time: Option<i64>,
) -> anyhow::Result<Vec<QuoteTick>> {
    let table_name = format!("user_{}_quotes_exchange_{}", user_id, exchange_id);
    
    let mut query = format!(
        "SELECT timestamp, instrument_id, bid_price, ask_price, bid_size, ask_size FROM {} WHERE instrument_id = '{}'",
        table_name, instrument_id
    );
    
    if let Some(start) = start_time {
        query.push_str(&format!(" AND timestamp >= {}", start));
    }
    if let Some(end) = end_time {
        query.push_str(&format!(" AND timestamp <= {}", end));
    }
    
    query.push_str(" ORDER BY timestamp ASC");
    
    let rows = client.query(&query).await?;
    
    // Convert rows to QuoteTick
    let quotes: Vec<QuoteTick> = rows.into_iter()
        .map(|row| convert_row_to_quote_tick(row))
        .collect();
    
    Ok(quotes)
}
```

**Files to modify**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/greptime_postgres_hybrid_cache.rs` (lines 654-876)

### Phase 6: Complete Add/Update Methods

**Goal**: Implement all remaining add/update operations**Tasks**:

1. **Add methods**:

- `add_synthetic`: Store synthetic instruments
- `add_position`: Store positions
- `add_order_book`: Store order book snapshots (may use GreptimeDB or PostgreSQL)
- `add_custom_data`: Route to GreptimeDB

2. **Update methods**:

- `update_account`: Update account state
- `update_order`: Already routed, verify implementation
- `update_position`: Update position state
- `update_actor`: Update actor state
- `update_strategy`: Update strategy state

3. **Snapshot methods**:

- `snapshot_order_state`: Create order snapshots
- `snapshot_position_state`: Create position snapshots

4. **Index methods**:

- `index_venue_order_id`: Create venue order ID index
- `index_order_position`: Create order-position index

5. **Delete methods**:

- `delete_actor`: Delete actor data
- `delete_strategy`: Delete strategy data

**Files to modify**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/greptime_postgres_hybrid_cache.rs` (lines 753-876)

### Phase 7: Flush and Connection Management

**Goal**: Complete connection lifecycle and flush operations**Tasks**:

1. **Flush implementation**:

- Flush PostgreSQL buffer
- Flush GreptimeDB buffer (ensure all pending writes are committed)
- Handle batch write completion

2. **Connection management**:

- Properly initialize GreptimeDB gRPC client in `connect()`
- Handle connection errors and reconnection
- Implement connection health checks
- Clean shutdown in `close()`

3. **Batch write optimization**:

- Buffer time-series data for batch writes
- Configurable batch size and interval
- Separate batches per table for parallel writes

**Files to modify**:

- `apps/hydra-naut-trader/crates/infrastructure/src/sql/greptime_postgres_hybrid_cache.rs` (lines 219-251, 649-652)

### Phase 8: PyO3 Python Bindings

**Goal**: Expose the hybrid cache adapter to Python/Nautilus Trader**Tasks**:

1. **Add PyO3 attributes**:

- Ensure `#[pyo3::pyclass]` is properly configured
- Add Python module registration

2. **Create Python bindings file**:

- File: `apps/hydra-naut-trader/crates/infrastructure/src/python/sql/hybrid_cache.rs` (new)
- Implement `#[pymethods]` for all public methods
- Handle Python <-> Rust type conversions
- Reference: `apps/hydra-naut-trader/crates/infrastructure/src/python/sql/cache.rs`

3. **Register in Python module**:

- File: `apps/hydra-naut-trader/crates/infrastructure/src/python/mod.rs`