#!/usr/bin/env python3
"""
Backtest Service
Runs backtests for trading strategies using Nautilus Trader.
Called from Rust API with JSON configuration file.
"""

import json
import os
import re
import sys
from pathlib import Path

# Try to import requests, but make it optional
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    print("Warning: requests library not available. API calls will fail.", file=sys.stderr)
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Optional, List, Callable

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.model.identifiers import Venue, InstrumentId, TraderId, Symbol, TradeId
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import AccountType, OmsType, AggregationSource, BookType
from nautilus_trader.model.objects import Money, Quantity, Price
from nautilus_trader.model.currencies import Currency, USD
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.enums import PriceType, BarAggregation
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.config import LoggingConfig, CacheConfig, DatabaseConfig
from nautilus_trader.data.aggregation import BarBuilder

# Hybrid cache is automatically configured through the kernel when using type="postgres"

# Import required services
from nautilus_trader.services.strategy_generator import DynamicStrategy, DynamicStrategyConfig
from nautilus_trader.services.report_generator import generate_reports
from nautilus_trader.services.tick_generator import generate_synthetic_ticks_from_bars
from nautilus_trader.services.data_preparation import load_data_from_catalog
from nautilus_trader.services.backtest_service_impl import create_currency_pair_from_id, create_indicators, parse_rules_to_strategy_logic

# Import DynamicBacktestStrategy
try:
    from nautilus_trader.services.dynamic_backtest_strategy import DynamicBacktestStrategy
    HAS_DYNAMIC_STRATEGY = True
except ImportError:
    HAS_DYNAMIC_STRATEGY = False
    print("Warning: DynamicBacktestStrategy not available", file=sys.stderr)


def get_catalog_entry_from_api(api_url: str, catalog_entry_id: str) -> Dict[str, Any]:
    """Query the API to get catalog entry details."""
    if not HAS_REQUESTS:
        raise ImportError("requests library is required to query catalog entries from API")
    
    try:
        # Query the data catalog API endpoint - it returns all entries, we filter by catalog_entry_id
        response = requests.get(f"{api_url}/api/data-catalog/query", timeout=10)
        response.raise_for_status()
        entries = response.json()
        
        # Find the entry with matching catalog_entry_id
        entry = next((e for e in entries if e.get("catalog_entry_id") == catalog_entry_id), None)
        
        if not entry:
            raise ValueError(f"Catalog entry {catalog_entry_id} not found")
        
        return entry
    except Exception as e:
        print(f"Error querying catalog entry from API: {e}", file=sys.stderr)
        raise


def create_currency_pair_from_instrument_id(instrument_id_str: str, bars: List[Bar] = None) -> CurrencyPair:
    """
    Create a CurrencyPair instrument from an instrument ID string.
    Example: "XDG/USD.KRAKEN" -> CurrencyPair
    """
    instrument_id = InstrumentId.from_str(instrument_id_str)
    
    # Parse base and quote from instrument ID (e.g., "XDG/USD.KRAKEN")
    parts = instrument_id.value.split('.')
    if len(parts) < 2:
        raise ValueError(f"Invalid instrument ID format: {instrument_id_str}")
    
    symbol_parts = parts[0].split('/')
    if len(symbol_parts) != 2:
        raise ValueError(f"Invalid symbol format: {parts[0]}")
    
    base_code = symbol_parts[0]
    quote_code = symbol_parts[1]
    
    # Get currencies (simplified - in production you'd look these up properly)
    from nautilus_trader.model.currencies import Currency
    # from_str with strict=False will create and register unknown currencies (like XDG)
    base_currency = Currency.from_str(base_code, strict=False)
    quote_currency = Currency.from_str(quote_code, strict=False)
    
    # Explicitly register currencies to ensure they're in Rust CURRENCY_MAP
    # This is important for PyO3 CurrencyPair.from_dict() which needs currencies registered
    Currency.register(base_currency, overwrite=True)
    Currency.register(quote_currency, overwrite=True)
    
    # Determine size precision from bars if available
    size_precision = 0
    if bars and len(bars) > 0 and bars[0].volume:
        size_precision = bars[0].volume.precision
    
    # Create size increment based on precision
    if size_precision == 0:
        size_increment = Quantity.from_int(1)
        min_quantity = Quantity.from_int(1)
        max_quantity = Quantity.from_int(10000000)
    else:
        size_increment_str = f"0.{'0' * (size_precision - 1)}1"
        size_increment = Quantity.from_str(size_increment_str)
        min_quantity = Quantity.from_str(size_increment_str)
        max_quantity_str = "10000000" + ("." + "0" * size_precision) if size_precision > 0 else "10000000"
        max_quantity = Quantity.from_str(max_quantity_str)
    
    # Determine price precision from bars if available
    # The chart shows 6 decimal places for crypto prices < 1, so we need to detect or use a heuristic
    price_precision = 8  # Default
    if bars and len(bars) > 0:
        # Check all bars to find maximum actual decimal places
        max_decimal_places = 0
        max_precision_attr = 0
        sample_price = None
        
        for bar in bars:
            if bar.close:
                precision_attr = bar.close.precision
                if precision_attr > max_precision_attr:
                    max_precision_attr = precision_attr
                
                # Get sample price for heuristic
                if sample_price is None:
                    sample_price = float(bar.close.as_double())
                
                # Count actual decimal places in stored value
                # Use as_double() like the chart does, then format and count significant decimal places
                price_val = float(bar.close.as_double())
                # Format with enough precision to capture all significant digits (up to 10 decimal places)
                # Then count actual decimal places, stopping at trailing zeros that are likely float artifacts
                price_str = f"{price_val:.10f}".rstrip('0').rstrip('.')
                if '.' in price_str:
                    decimal_places = len(price_str.split('.')[1])
                    if decimal_places > max_decimal_places:
                        max_decimal_places = decimal_places
        
        # Heuristic: If price < 1 and precision_attr is 4, the actual data has up to 8 decimal places
        # The chart displays 6 for readability, but Parquet data preserves up to 8
        # Price objects round to 4, so we should use 8 for crypto prices < 1 to preserve all precision
        if sample_price and sample_price < 1.0 and max_precision_attr == 4:
            price_precision = 8
        elif max_decimal_places > 0:
            price_precision = min(max_decimal_places, 16)  # Cap at 16 to avoid "precision greater than max 16" error
    
    # Create price increment
    if price_precision == 0:
        price_increment = Price.from_str("1")
    else:
        price_increment_str = f"0.{'0' * (price_precision - 1)}1"
        price_increment = Price.from_str(price_increment_str)
    
    # Create Symbol from raw symbol string
    raw_symbol = Symbol(parts[0])
    
    # Create CurrencyPair
    currency_pair = CurrencyPair(
        instrument_id=instrument_id,
        raw_symbol=raw_symbol,
        base_currency=base_currency,
        quote_currency=quote_currency,
        price_precision=price_precision,
        size_precision=size_precision,
        price_increment=price_increment,
        size_increment=size_increment,
        lot_size=size_increment,  # For crypto, lot_size = size_increment
        max_quantity=max_quantity,
        min_quantity=min_quantity,
        max_price=Price.from_str("1000000"),
        min_price=Price.from_str("0.00000001"),
        margin_init=Decimal("0"),
        margin_maint=Decimal("0"),
        maker_fee=Decimal("0.001"),
        taker_fee=Decimal("0.001"),
        ts_event=0,
        ts_init=0,
    )
    
    return currency_pair


def load_bars_from_catalog(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> List[Bar]:
    """Load bars from catalog for the given instrument and date range."""
    print(f"Querying catalog for instrument: {instrument_id}", file=sys.stderr)
    query_kwargs = {"instrument_ids": [instrument_id]}
    
    # Query bars
    try:
        bars = catalog.bars(**query_kwargs)
        bars_list = list(bars)
        print(f"Found {len(bars_list)} total bars in catalog for {instrument_id}", file=sys.stderr)
        
    except Exception as e:
        print(f"Error querying catalog: {e}", file=sys.stderr)
        # Try querying without instrument filter to see what's available
        try:
            all_bars = catalog.bars()
            all_bars_list = list(all_bars)
            print(f"Total bars in catalog (all instruments): {len(all_bars_list)}", file=sys.stderr)
            if all_bars_list:
                # Show sample instrument IDs from bars
                sample_instruments = set()
                for bar in all_bars_list[:100]:  # Sample first 100
                    if hasattr(bar, 'bar_type') and hasattr(bar.bar_type, 'instrument_id'):
                        sample_instruments.add(str(bar.bar_type.instrument_id))
                print(f"Sample instrument IDs in catalog: {list(sample_instruments)[:10]}", file=sys.stderr)
        except Exception as e2:
            print(f"Error querying all bars: {e2}", file=sys.stderr)
        raise ValueError(f"Failed to query bars from catalog: {e}")
    
    if not bars_list:
        # Try to see what instruments are available
        try:
            instruments = catalog.instruments()
            instrument_ids = [str(inst.id) for inst in instruments]
            print(f"Available instruments in catalog: {instrument_ids[:20]}", file=sys.stderr)
        except Exception as e:
            print(f"Error getting instruments: {e}", file=sys.stderr)
        raise ValueError(f"No bars found in catalog for instrument {instrument_id}")
    
    # Show date range of available bars
    if bars_list:
        first_bar_ts = bars_list[0].ts_init / 1e9
        last_bar_ts = bars_list[-1].ts_init / 1e9
        first_bar_dt = datetime.fromtimestamp(first_bar_ts, tz=timezone.utc)
        last_bar_dt = datetime.fromtimestamp(last_bar_ts, tz=timezone.utc)
        print(f"Available bars date range: {first_bar_dt} to {last_bar_dt}", file=sys.stderr)
        if start_date:
            print(f"Requested start date: {start_date}", file=sys.stderr)
        if end_date:
            print(f"Requested end date: {end_date}", file=sys.stderr)
    
    # Filter by date range if provided
    if start_date or end_date:
        filtered_bars = []
        skipped_before = 0
        skipped_after = 0
        
        # Ensure dates are timezone-aware (UTC)
        if start_date and start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date and end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)
        
        for bar in bars_list:
            bar_ts = bar.ts_init / 1e9
            bar_dt = datetime.fromtimestamp(bar_ts, tz=timezone.utc)
            
            if start_date and bar_dt < start_date:
                skipped_before += 1
                continue
            if end_date and bar_dt > end_date:
                skipped_after += 1
                continue
            
            filtered_bars.append(bar)
        
        print(f"Date filtering: {skipped_before} bars before start, {skipped_after} bars after end, {len(filtered_bars)} bars in range", file=sys.stderr)
        bars_list = filtered_bars
    
    # Sort by timestamp
    bars_list.sort(key=lambda bar: bar.ts_init)
    
    return bars_list


def aggregate_bars_to_timeframe(
    base_bars: List[Bar],
    instrument: CurrencyPair,
    target_timeframe: str,
) -> List[Bar]:
    """
    Aggregate base bars (e.g., 1m) to a higher timeframe (e.g., 5m).
    
    Args:
        base_bars: List of base timeframe bars (e.g., 1m)
        instrument: The instrument for the bars
        target_timeframe: Target timeframe string (e.g., "5m", "1h", "1d")
        
    Returns:
        List of aggregated bars
    """
    if not base_bars:
        return []
    
    # Parse timeframe to BarSpecification
    match = re.match(r'^(\d+)([mhd])$', target_timeframe.lower())
    if not match:
        raise ValueError(f"Invalid timeframe format: {target_timeframe}")
    
    value = int(match.group(1))
    unit = match.group(2)
    
    if unit == 'm':
        aggregation = BarAggregation.MINUTE
        period_nanos = value * 60 * 1_000_000_000
    elif unit == 'h':
        aggregation = BarAggregation.HOUR
        period_nanos = value * 3600 * 1_000_000_000
    elif unit == 'd':
        aggregation = BarAggregation.DAY
        period_nanos = value * 86400 * 1_000_000_000
    else:
        raise ValueError(f"Unsupported timeframe unit: {unit}")
    
    bar_spec = BarSpecification(value, aggregation, PriceType.LAST)
    bar_type = BarType(instrument.id, bar_spec)
    
    # Create BarBuilder for the target timeframe
    builder = BarBuilder(instrument, bar_type)
    aggregated_bars = []
    current_period_start = None
    
    # Aggregate each base bar
    for i, base_bar in enumerate(base_bars):
        # Calculate which period this bar belongs to
        period_start = (base_bar.ts_event // period_nanos) * period_nanos
        
        # If we've moved to a new period, build the previous bar
        if current_period_start is not None and period_start != current_period_start:
            if builder.initialized:
                # Build the aggregated bar for the previous period
                aggregated_bar = builder.build_now()
                if aggregated_bar:
                    aggregated_bars.append(aggregated_bar)
                # Reset builder for new period
                builder = BarBuilder(instrument, bar_type)
        
        # Update builder with current base bar
        builder.update_bar(base_bar, base_bar.volume, base_bar.ts_event)
        current_period_start = period_start
    
    # Build final bar if builder has data
    if builder.initialized:
        final_bar = builder.build_now()
        if final_bar:
            aggregated_bars.append(final_bar)
    
    # Post-process aggregated bars to ensure prices use instrument's precision
    # BarBuilder may preserve precision from base bars (4), but we need instrument precision (8)
    corrected_bars = []
    for bar in aggregated_bars:
        # Recreate Price objects using instrument.make_price() to ensure correct precision
        open_price = instrument.make_price(float(bar.open.as_double()))
        high_price = instrument.make_price(float(bar.high.as_double()))
        low_price = instrument.make_price(float(bar.low.as_double()))
        close_price = instrument.make_price(float(bar.close.as_double()))
        
        # Create new bar with corrected prices
        corrected_bar = Bar(
            bar_type=bar.bar_type,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=bar.volume,
            ts_event=bar.ts_event,
            ts_init=bar.ts_init,
        )
        corrected_bars.append(corrected_bar)
    
    return corrected_bars


def load_and_aggregate_bars(
    catalog: ParquetDataCatalog,
    instrument_id: str,
    instrument: CurrencyPair,
    required_timeframes: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> Dict[str, List[Bar]]:
    """
    Load base bars (1m) from catalog and aggregate to required timeframes.
    
    Args:
        catalog: ParquetDataCatalog instance
        instrument_id: Instrument ID string
        instrument: CurrencyPair instrument
        required_timeframes: List of required timeframes (e.g., ["1m", "5m"])
        start_date: Optional start date filter
        end_date: Optional end date filter
        
    Returns:
        Dictionary mapping timeframe to list of bars (e.g., {"1m": [...], "5m": [...]})
    """
    # Load base bars (1m) from catalog
    print(f"Loading base bars (1m) for {instrument_id}...", file=sys.stderr)
    base_bars = load_bars_from_catalog(catalog, instrument_id, start_date, end_date)
    
    if not base_bars:
        raise ValueError(f"No base bars found for {instrument_id} in date range")
    
    print(f"Loaded {len(base_bars)} base bars", file=sys.stderr)
    
    # Group bars by timeframe
    bars_by_timeframe: Dict[str, List[Bar]] = {}
    
    # Filter base bars to 1m only (if they exist)
    # Check what timeframes are in the loaded bars
    base_timeframes = set()
    for bar in base_bars[:100]:  # Sample first 100
        if hasattr(bar, 'bar_type') and hasattr(bar.bar_type, 'spec'):
            spec = bar.bar_type.spec
            if spec.aggregation == BarAggregation.MINUTE:
                base_timeframes.add(f"{spec.step}m")
    
    # Find the smallest timeframe (should be 1m)
    smallest_timeframe = min(required_timeframes, key=lambda tf: int(re.match(r'^(\d+)', tf).group(1)) if re.match(r'^(\d+)', tf) else 999)
    
    # Use 1m bars as base if available, otherwise use whatever is smallest
    # Also correct base bars' precision to match instrument
    def correct_bar_precision(bar: Bar) -> Bar:
        """Correct a bar's price precision to match the instrument."""
        open_price = instrument.make_price(float(bar.open.as_double()))
        high_price = instrument.make_price(float(bar.high.as_double()))
        low_price = instrument.make_price(float(bar.low.as_double()))
        close_price = instrument.make_price(float(bar.close.as_double()))
        return Bar(
            bar_type=bar.bar_type,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=bar.volume,
            ts_event=bar.ts_event,
            ts_init=bar.ts_init,
        )
    
    if "1m" in base_timeframes or smallest_timeframe == "1m":
        # Filter to 1m bars only and correct their precision
        one_minute_bars_raw = [b for b in base_bars if hasattr(b, 'bar_type') and 
                          hasattr(b.bar_type, 'spec') and
                          b.bar_type.spec.aggregation == BarAggregation.MINUTE and
                          b.bar_type.spec.step == 1]
        if one_minute_bars_raw:
            # Correct precision of 1m bars
            one_minute_bars = [correct_bar_precision(b) for b in one_minute_bars_raw]
            bars_by_timeframe["1m"] = one_minute_bars
            base_bars_for_agg = one_minute_bars
        else:
            # Use all bars as base and correct their precision
            corrected_base_bars = [correct_bar_precision(b) for b in base_bars]
            bars_by_timeframe[smallest_timeframe] = corrected_base_bars
            base_bars_for_agg = corrected_base_bars
    else:
        # Use all loaded bars as base and correct their precision
        corrected_base_bars = [correct_bar_precision(b) for b in base_bars]
        bars_by_timeframe[smallest_timeframe] = corrected_base_bars
        base_bars_for_agg = corrected_base_bars
    
    # Aggregate to other required timeframes
    for timeframe in required_timeframes:
        if timeframe not in bars_by_timeframe:
            print(f"Aggregating bars to {timeframe}...", file=sys.stderr)
            aggregated = aggregate_bars_to_timeframe(base_bars_for_agg, instrument, timeframe)
            bars_by_timeframe[timeframe] = aggregated
            print(f"Generated {len(aggregated)} {timeframe} bars", file=sys.stderr)
    
    return bars_by_timeframe


# Import DynamicBacktestStrategy
try:
    from nautilus_trader.services.dynamic_backtest_strategy import DynamicBacktestStrategy
    HAS_DYNAMIC_STRATEGY = True
except ImportError:
    HAS_DYNAMIC_STRATEGY = False
    print("Warning: DynamicBacktestStrategy not available", file=sys.stderr)

# Module-level variable to store user_id per process
# Each backtest runs in its own process, so this is safe for multi-user
_current_user_id: str | None = None

# Module-level variable to store trading_mode per process
# Each backtest/paper/live run runs in its own process, so this is safe for multi-user
_current_trading_mode: str | None = None

def set_current_user_id(user_id: str) -> None:
    """Set the current user_id for this process."""
    global _current_user_id
    _current_user_id = user_id

def get_current_user_id() -> str | None:
    """Get the current user_id for this process."""
    return _current_user_id

def set_current_trading_mode(trading_mode: str) -> None:
    """Set the current trading_mode for this process (backtest, paper, or live)."""
    global _current_trading_mode
    _current_trading_mode = trading_mode

def get_current_trading_mode() -> str | None:
    """Get the current trading_mode for this process."""
    return _current_trading_mode


def run_backtest(
    strategy_config: Dict[str, Any],
    strategy_data: list,
    backtest_config: Dict[str, Any],
    catalog_entry_id: str,
    strategy_run_id: str,
    database_url: str,
    api_url: str,
    user_id: str | None = None,
) -> Dict[str, Any]:
    """
    Run a backtest for a trading strategy.
    
    Args:
        strategy_config: Strategy configuration
        strategy_data: List of indicator configurations
        backtest_config: Backtest date range and portfolio config
        catalog_entry_id: Catalog entry ID for data
        strategy_run_id: Strategy run ID for database updates
        database_url: PostgreSQL connection URL
        api_url: API URL for updates
        
    Returns:
        Dictionary with report file paths and metadata
    """
    try:
        # 0. Extract and set user_id FIRST (before any cache initialization)
        # This ensures multi-user support - each backtest runs in its own process
        user_id = None
        if isinstance(strategy_config, dict):
            user_id = strategy_config.get("user_id")
        
        if not user_id:
            raise ValueError("user_id is required in strategy config")
        
        # Set user_id in module-level variable (per-process, safe for multi-user)
        set_current_user_id(str(user_id))
        # Set trading_mode to "backtest" for this process
        set_current_trading_mode("backtest")
        # Directly check the module variable instead of calling the function
        from nautilus_trader.services import backtest_service as bs_module
        direct_check = bs_module._current_user_id
        verified_user_id = get_current_user_id()
        print(f"Set current user_id={user_id} for this backtest process", file=sys.stderr)
        if verified_user_id != str(user_id) or direct_check != str(user_id):
            print(f"WARNING: user_id verification failed! Set: {user_id}, Direct: {direct_check}, Got: {verified_user_id}", file=sys.stderr)
        
        # 1. Get catalog entry details from API
        print(f"Fetching catalog entry {catalog_entry_id} from API...", file=sys.stderr)
        catalog_entry = get_catalog_entry_from_api(api_url, catalog_entry_id)
        instrument_id_str = catalog_entry["instrument_id"]
        exchange = catalog_entry["exchange"]
        
        # 2. Set up logging
        logging_config = LoggingConfig(
            log_level="INFO",
            log_colors=False,
            use_pyo3=False,
        )
        
        # 3. Set up cache with hybrid database adapter (always enabled)
        cache_config = None
        try:
            # Use environment variables or database_url to configure
            postgres_host = os.getenv("POSTGRES_HOST", "localhost")
            postgres_port = int(os.getenv("POSTGRES_PORT", "5432"))
            postgres_username = os.getenv("POSTGRES_USERNAME")
            postgres_password = os.getenv("POSTGRES_PASSWORD")
            postgres_database = os.getenv("POSTGRES_DATABASE", "fincept_terminal")
            
            # Parse database_url if provided (format: postgresql://user:pass@host:port/db)
            if database_url:
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(database_url)
                    if parsed.hostname:
                        postgres_host = parsed.hostname
                    if parsed.port:
                        postgres_port = parsed.port
                    if parsed.username:
                        postgres_username = parsed.username
                    if parsed.password:
                        postgres_password = parsed.password
                    if parsed.path and len(parsed.path) > 1:
                        postgres_database = parsed.path[1:]  # Remove leading '/'
                except Exception as e:
                    print(f"Warning: Failed to parse database_url: {e}. Using environment variables.", file=sys.stderr)
            
            # Configure cache - always use hybrid type when called from hydra-terminal-api
            # Note: DatabaseConfig doesn't have a 'database' field, so we pass it separately
            # The factory function will read it from environment variables or use defaults
            cache_config = CacheConfig(
                database=DatabaseConfig(
                    type="hybrid",  # Always use hybrid cache (GreptimeDB + PostgreSQL) from hydra-terminal-api
                    host=postgres_host,
                    port=postgres_port,
                    username=postgres_username,
                    password=postgres_password,
                ),
                tick_capacity=10_000,
                bar_capacity=10_000,
                buffer_interval_ms=100,  # Batch writes every 100ms
            )
            
            # Store postgres_database in environment so factory function can access it
            # The factory function will use POSTGRES_DATABASE env var or default
            if postgres_database:
                os.environ["POSTGRES_DATABASE"] = postgres_database
            
            # Note: user_id is already set in environment at the start of the function
            # This ensures the cache uses the correct user_id from strategy config
            print("Hybrid cache configured (GreptimeDB + PostgreSQL)", file=sys.stderr)
            print(f"  User ID: {user_id}", file=sys.stderr)
            print(f"  PostgreSQL: {postgres_host}:{postgres_port}/{postgres_database}", file=sys.stderr)
            print(f"  GreptimeDB: {os.getenv('GREPTIME_HOST', 'localhost')}:{os.getenv('GREPTIME_PORT', '4000')}/{os.getenv('GREPTIME_DATABASE', 'nautilus_timeseries')}", file=sys.stderr)
        except Exception as e:
            print(f"ERROR: Failed to configure hybrid cache: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            cache_config = None
        
        # 4. Create backtest engine
        engine_config = BacktestEngineConfig(
            trader_id=TraderId(f"BACKTEST-{strategy_run_id[:8]}"),
            logging=logging_config,
            cache=cache_config,
            user_id=str(user_id),  # Pass user_id directly through config
        )
        engine = BacktestEngine(config=engine_config)
        
        # Verify cache is initialized
        if cache_config and cache_config.database:
            try:
                if hasattr(engine, 'cache') and engine.cache:
                    print(f"Cache initialized: {type(engine.cache)}", file=sys.stderr)
                    if hasattr(engine.cache, 'database') and engine.cache.database:
                        print(f"Cache database adapter: {type(engine.cache.database)}", file=sys.stderr)
                        print(f"Cache has backing: {engine.cache.has_backing}", file=sys.stderr)
                    else:
                        print("WARNING: Cache database adapter is None!", file=sys.stderr)
                else:
                    print("WARNING: Engine cache is None!", file=sys.stderr)
            except Exception as e:
                print(f"ERROR checking cache initialization: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
        
        # 4. Determine catalog path (from environment or default)
        catalog_path = os.getenv("CATALOG_PATH", "./data/catalog")
        print(f"Using catalog path: {catalog_path}", file=sys.stderr)
        if not os.path.exists(catalog_path):
            print(f"Warning: Catalog path does not exist: {catalog_path}", file=sys.stderr)
            # Try alternative paths
            alt_paths = [
                "../data/catalog",
                "../../data/catalog",
                "./data/parquet",
                "../data/parquet",
            ]
            for alt_path in alt_paths:
                if os.path.exists(alt_path):
                    print(f"Found alternative catalog path: {alt_path}", file=sys.stderr)
                    catalog_path = alt_path
                    break
            else:
                raise ValueError(f"Catalog path not found: {catalog_path}. Tried: {[catalog_path] + alt_paths}")
        
        catalog = ParquetDataCatalog(catalog_path)
        
        # 5. Parse date range
        start_date = None
        end_date = None
        
        def parse_date(date_str: str) -> datetime:
            """Parse a date string and ensure it's timezone-aware (UTC)."""
            if not date_str:
                return None
            
            # Try ISO format first (with T or space separator)
            try:
                # Handle Z suffix
                if date_str.endswith("Z"):
                    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                # Handle explicit timezone (+00:00, -05:00, etc.)
                elif "+" in date_str or (date_str.count("-") >= 3 and ":" in date_str.split()[-1] if " " in date_str else False):
                    dt = datetime.fromisoformat(date_str)
                # Try ISO format (might be naive)
                else:
                    try:
                        dt = datetime.fromisoformat(date_str)
                    except ValueError:
                        # Try space-separated format: "2025-01-01 00:00:00"
                        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                
                # Ensure timezone-aware (assume UTC if naive)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                
                return dt
            except (ValueError, AttributeError) as e:
                raise ValueError(f"Failed to parse date '{date_str}': {e}")
        
        if backtest_config.get("start_date"):
            start_date = parse_date(backtest_config["start_date"])
        
        if backtest_config.get("end_date"):
            end_date = parse_date(backtest_config["end_date"])
        
        # 6. Get required timeframes from strategy settings
        strategy_settings = strategy_config.get("strategy_settings", {})
        additional_aggs = strategy_settings.get("Additional_Aggregations", [])
        
        # Create DynamicBacktestStrategy to extract timeframes from rules
        # We'll do this temporarily just to get timeframes
        temp_strategy_parser = DynamicBacktestStrategy(
            strategy_config=strategy_config,
            strategy_data=[],
            bars=[],
        )
        required_timeframes = temp_strategy_parser.get_required_timeframes()
        print(f"Required timeframes: {required_timeframes}", file=sys.stderr)
        
        # 7. Create instrument (we need it for aggregation, so load a sample first)
        print(f"Creating instrument {instrument_id_str}...", file=sys.stderr)
        # Load a sample to create instrument - use more bars to find maximum precision
        sample_bars = load_bars_from_catalog(catalog, instrument_id_str, start_date, end_date)
        if not sample_bars:
            raise ValueError(f"No bars found for {instrument_id_str} in date range")
        # Use a larger sample (up to 1000 bars) to find maximum precision
        # This ensures we don't lose precision during aggregation
        sample_size = min(1000, len(sample_bars))
        instrument = create_currency_pair_from_instrument_id(instrument_id_str, sample_bars[:sample_size])
        
        # 8. Load and aggregate bars for all required timeframes
        print(f"Loading and aggregating bars for timeframes: {required_timeframes}...", file=sys.stderr)
        bars_by_timeframe = load_and_aggregate_bars(
            catalog,
            instrument_id_str,
            instrument,
            required_timeframes,
            start_date,
            end_date,
        )
        
        # Get all bars (flattened) for tick generation and strategy initialization
        all_bars = []
        for timeframe, bars in bars_by_timeframe.items():
            all_bars.extend(bars)
        all_bars.sort(key=lambda b: b.ts_init)
        print(f"Total bars across all timeframes: {len(all_bars)}", file=sys.stderr)
        
        # 9. Set up venue
        account_type_str = strategy_config.get("account_type", "CASH")
        account_type = AccountType[account_type_str] if account_type_str in AccountType.__members__ else AccountType.CASH
        
        position_lifecycle_str = strategy_config.get("position_lifecycle", "NETTING")
        oms_type = OmsType.NETTING if position_lifecycle_str == "NETTING" else OmsType.HEDGING
        
        venue = Venue(exchange)
        
        # Set up starting balances for multi-currency account
        base_currency = instrument.base_currency
        quote_currency = instrument.quote_currency
        
        # Get allocated funds from strategy_settings - REQUIRED, no fallbacks
        # This is critical for production trading, so we must fail if missing
        if "Allocated_Funds" not in strategy_settings:
            raise ValueError(
                "Allocated_Funds is required in strategy_settings. "
                "This value determines the starting capital for the backtest and must be explicitly set."
            )
        
        allocated_funds = strategy_settings["Allocated_Funds"]
        if not isinstance(allocated_funds, (int, float)) or allocated_funds <= 0:
            raise ValueError(
                f"Allocated_Funds must be a positive number, got: {allocated_funds} (type: {type(allocated_funds).__name__})"
            )
        
        # For CurrencyPair, we need a multi-currency account with balances in both currencies
        # This makes it a true multi-currency account, not single-currency
        # Use Allocated_Funds for the quote currency (e.g., USD), and 0 for base currency (e.g., XDG)
        starting_balances = [
            Money(float(allocated_funds), quote_currency),  # Use Allocated_Funds from strategy_settings
            Money(0.0, base_currency),  # Add base currency balance (XDG) to make it multi-currency
        ]
        
        # Set base_currency=None for multi-currency account
        # This allows CurrencyPair to be added to CASH accounts
        engine.add_venue(
            venue=venue,
            oms_type=oms_type,
            book_type=BookType.L1_MBP,
            account_type=account_type,
            base_currency=None,  # Multi-currency account (has balances in multiple currencies)
            starting_balances=starting_balances,
            trade_execution=True,
        )
        
        # 10. Add instrument (must be before adding data)
        engine.add_instrument(instrument)
        
        # 11. Add bars for all timeframes to engine
        print("Adding bars for all timeframes to engine...", file=sys.stderr)
        for timeframe, bars in bars_by_timeframe.items():
            print(f"Adding {len(bars)} {timeframe} bars", file=sys.stderr)
            engine.add_data(bars)
        
        # 12. Generate synthetic ticks from 1m bars (use smallest timeframe for ticks)
        smallest_timeframe = min(required_timeframes, key=lambda tf: int(re.match(r'^(\d+)', tf).group(1)) if re.match(r'^(\d+)', tf) else 999)
        tick_bars = bars_by_timeframe.get(smallest_timeframe, all_bars[:1000])
        print(f"Generating synthetic ticks from {smallest_timeframe} bars...", file=sys.stderr)
        ticks = generate_synthetic_ticks_from_bars(tick_bars, instrument)
        
        print(f"Generated {len(ticks)} ticks", file=sys.stderr)
        engine.add_data(ticks)
        
        # 13. Create strategy using DynamicBacktestStrategy class
        print("Creating strategy...", file=sys.stderr)
        
        # Use 1m bars (or smallest) for strategy initialization
        strategy_bars = bars_by_timeframe.get("1m", bars_by_timeframe.get(smallest_timeframe, []))
        
        strategy_parser = DynamicBacktestStrategy(
            strategy_config=strategy_config,
            strategy_data=strategy_data,
            bars=strategy_bars,
        )
        strategy = strategy_parser.create_strategy(
            instrument_id=instrument.id,
        )
        
        engine.add_strategy(strategy)
        
        # 14. Run backtest
        print("Running backtest...", file=sys.stderr)
        engine.run()
        print("Backtest completed", file=sys.stderr)
        
        # 14.5. Flush cache to ensure all data is persisted
        if cache_config and cache_config.database:
            try:
                print("Flushing cache to database...", file=sys.stderr)
                if hasattr(engine, 'cache') and engine.cache:
                    print(f"  Cache type: {type(engine.cache)}", file=sys.stderr)
                    if hasattr(engine.cache, 'database') and engine.cache.database:
                        print(f"  Database adapter type: {type(engine.cache.database)}", file=sys.stderr)
                        # Try flush (CacheHybridAdapter and PostgresCacheAdapter both have this)
                        if hasattr(engine.cache.database, 'flush'):
                            print("  Calling flush() and waiting for completion...", file=sys.stderr)
                            engine.cache.database.flush()
                            print("Cache flushed successfully (using flush)", file=sys.stderr)
                            # Give a moment for any final commits
                            import time
                            time.sleep(0.5)
                            print("  Flush completed, proceeding to close...", file=sys.stderr)
                        # Fallback to flush_db (PostgresCacheAdapter)
                        elif hasattr(engine.cache.database, 'flush_db'):
                            engine.cache.database.flush_db()
                            print("Cache flushed successfully (using flush_db)", file=sys.stderr)
                        else:
                            print(f"WARNING: Cache database adapter has no flush method! Available methods: {dir(engine.cache.database)}", file=sys.stderr)
                    else:
                        print("WARNING: Cache database adapter is None!", file=sys.stderr)
                else:
                    print("WARNING: Engine cache is None!", file=sys.stderr)
            except Exception as e:
                print(f"ERROR: Failed to flush cache: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
        
        # 15. Generate reports
        print("Generating reports...", file=sys.stderr)
        reports = generate_reports(engine, strategy_run_id, catalog_path)
        
        # Read performance stats file contents and include in metadata
        reports_dir = os.path.join(os.getcwd(), "logs", "backtests", "reports", strategy_run_id)
        
        # Read performance_stats_general.txt content
        if reports.get("performance_stats_general"):
            general_stats_path = reports["performance_stats_general"]
            try:
                if os.path.exists(general_stats_path):
                    with open(general_stats_path, 'r') as f:
                        reports["performance_stats_general_content"] = f.read()
            except Exception as e:
                print(f"Warning: Failed to read performance_stats_general: {e}", file=sys.stderr)
        
        # Read performance_stats_pnls.txt content
        if reports.get("performance_stats_pnls"):
            pnls_stats_path = reports["performance_stats_pnls"]
            try:
                if os.path.exists(pnls_stats_path):
                    with open(pnls_stats_path, 'r') as f:
                        reports["performance_stats_pnls_content"] = f.read()
            except Exception as e:
                print(f"Warning: Failed to read performance_stats_pnls: {e}", file=sys.stderr)
        
        # Read performance_stats_returns.txt content
        if reports.get("performance_stats_returns"):
            returns_stats_path = reports["performance_stats_returns"]
            try:
                if os.path.exists(returns_stats_path):
                    with open(returns_stats_path, 'r') as f:
                        reports["performance_stats_returns_content"] = f.read()
            except Exception as e:
                print(f"Warning: Failed to read performance_stats_returns: {e}", file=sys.stderr)
        
        
        # Clean up
        engine.reset()
        engine.dispose()
        
        # Ensure all report values are strings (file paths), not DataFrames or other objects
        # Also check if any path is suspiciously long (might be file content instead of path)
        # File paths should never be more than 500 characters - anything longer is suspicious
        # EXCEPT for *_content fields which intentionally contain file content
        MAX_PATH_LENGTH = 500
        from pathlib import Path as PathLib
        
        for key, value in list(reports.items()):
            if value is not None:
                # Skip length/content checks for *_content fields (they intentionally contain file content)
                is_content_field = key.endswith('_content')
                
                # Check for Path objects explicitly first
                if isinstance(value, PathLib):
                    value_str = str(value)
                    print(f"WARNING: reports['{key}'] is a Path object. Converting to string: {value_str[:100]}", file=sys.stderr)
                    reports[key] = value_str
                    value = value_str  # Update for length check below
                
                if not isinstance(value, str):
                    # Convert non-string to string
                    value_str = str(value)
                    print(f"WARNING: reports['{key}'] is not a string, it's a {type(value).__name__}. Converting to string.", file=sys.stderr)
                    reports[key] = value_str
                    value = value_str  # Update for length check below
                
                # Now check length (value is guaranteed to be a string at this point)
                # Skip this check for content fields
                if not is_content_field and len(value) > MAX_PATH_LENGTH:
                    # If a path is longer than MAX_PATH_LENGTH chars, it's definitely not a file path
                    print(f"ERROR: reports['{key}'] is {len(value)} chars long - this is too long for a file path! First 200 chars: {value[:200]}", file=sys.stderr)
                    # Check if it looks like file content (contains newlines, CSV headers, etc.)
                    if '\n' in value or value.startswith('order_id,') or value.startswith('client_order_id,') or ',' in value[:50]:
                        print(f"ERROR: reports['{key}'] appears to contain file content instead of a file path! Setting to None.", file=sys.stderr)
                    # Don't include it in the result - set to None to prevent huge JSON
                    reports[key] = None
        
        # Log summary of cleaned reports
        report_count = sum(1 for v in reports.values() if v is not None and isinstance(v, str))
        print(f"Reports dictionary cleaned: {report_count} valid file paths, {len(reports) - report_count} None values", file=sys.stderr)
        
        return reports
        
    except Exception as e:
        print(f"Error running backtest: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise


def main():
    """Main entry point for backtest service."""
    if len(sys.argv) < 2:
        error_result = {"error": "No config file provided"}
        print(json.dumps(error_result), file=sys.stderr)
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    try:
        # Read configuration
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Extract configuration
        strategy_config = config.get("strategy", {})
        strategy_data = config.get("strategy_data", [])
        backtest_config = config.get("backtest_config", {})
        catalog_entry_id = config.get("catalog_entry_id")
        strategy_run_id = config.get("strategy_run_id")
        database_url = config.get("database_url")
        api_url = config.get("api_url", "http://127.0.0.1:3000")
        
        # Extract user_id from strategy config
        user_id = None
        if isinstance(strategy_config, dict):
            user_id = strategy_config.get("user_id")
        
        if not catalog_entry_id:
            raise ValueError("catalog_entry_id is required")
        if not strategy_run_id:
            raise ValueError("strategy_run_id is required")
        if not user_id:
            raise ValueError("user_id is required in strategy config")
        
        # Note: user_id will be set in environment by run_backtest() at the start
        # This ensures multi-user support - each backtest process has its own user_id
        
        # Run backtest
        result = run_backtest(
            strategy_config=strategy_config,
            strategy_data=strategy_data,
            backtest_config=backtest_config,
            catalog_entry_id=catalog_entry_id,
            strategy_run_id=strategy_run_id,
            database_url=database_url,
            api_url=api_url,
            user_id=user_id,
        )
        
        # Output result as JSON to stdout (logs already go to stderr)
        # Flush stderr first to ensure logs are written
        sys.stderr.flush()
        
        # Try to serialize to JSON, with error handling
        # First, ensure all values are JSON-serializable and not objects that might expand
        # CRITICAL: Convert everything to plain Python types (str, int, float, bool, None)
        # to prevent any object serialization issues
        json_safe_result = {}
        for key, value in result.items():
            if value is None:
                json_safe_result[key] = None
            elif isinstance(value, bool):
                json_safe_result[key] = value
            elif isinstance(value, (int, float)):
                json_safe_result[key] = value
            elif isinstance(value, str):
                # For strings, verify length and check for suspicious content
                actual_len = len(value)
                # Test JSON serialization of just this field
                try:
                    json_test = json.dumps({key: value})
                    json_len = len(json_test) - len('{"' + key + '":""}')  # Approximate field size
                    print(f"  {key}: str length={actual_len}, JSON field size≈{json_len} bytes", file=sys.stderr)
                    # Skip size checks for *_content fields (they intentionally contain file content)
                    is_content_field = key.endswith('_content')
                    
                    if not is_content_field and (actual_len > 500 or json_len > 10000):  # More than 500 chars or 10KB JSON is suspicious
                        print(f"  WARNING: {key} is suspiciously large (str={actual_len} chars, JSON≈{json_len} bytes)! First 200 chars: {value[:200]}", file=sys.stderr)
                        # Check if it looks like file content
                        if '\n' in value or value.startswith(('order_id,', 'client_order_id,', 'timestamp,')):
                            print(f"  ERROR: {key} appears to contain file content! Setting to None.", file=sys.stderr)
                            json_safe_result[key] = None
                        else:
                            # It's a long path - keep it but log warning
                            json_safe_result[key] = value
                    else:
                        # Content fields or normal-sized fields - keep them
                        json_safe_result[key] = value
                except Exception as e:
                    print(f"  ERROR: Failed to test JSON serialization for {key}: {e}. Setting to None.", file=sys.stderr)
                    json_safe_result[key] = None
            else:
                # For any other type, convert to string explicitly
                value_str = str(value)
                print(f"WARNING: Converting {key} from {type(value).__name__} to string. Length: {len(value_str)}", file=sys.stderr)
                if len(value_str) > 500:
                    print(f"  ERROR: Converted {key} string is too long ({len(value_str)} chars)! Setting to None.", file=sys.stderr)
                    json_safe_result[key] = None
                else:
                    json_safe_result[key] = value_str
        print("=== END FIELD CHECK ===", file=sys.stderr)
        sys.stderr.flush()
        
        try:
            # Test JSON serialization
            test_json = json.dumps(json_safe_result)
            json_size = len(test_json)
            sys.stderr.flush()
            
            if json_size > 10 * 1024 * 1024:  # More than 10MB
                print(f"ERROR: JSON size ({json_size} bytes = {json_size / (1024*1024):.2f} MB) is suspiciously large! Something is wrong.", file=sys.stderr)
                print(f"ERROR: Dictionary had {len(json_safe_result)} fields, total string size was small but JSON is huge - this indicates a serialization bug.", file=sys.stderr)
                sys.stderr.flush()
                # Don't output the huge JSON - return an error instead
                error_result = {
                    "error": f"JSON output too large ({json_size} bytes = {json_size / (1024*1024):.2f} MB). This indicates a bug - report values may contain file content instead of paths.",
                    "strategy_run_id": json_safe_result.get("strategy_run_id"),
                    "status": "error"
                }
                print(json.dumps(error_result))
                sys.stdout.flush()
                sys.exit(1)
            
            # JSON is small enough - write to a metadata file instead of stdout
            # This avoids issues with stdout containing other output
            metadata_file = os.path.join(os.getcwd(), "logs", "backtests", "reports", strategy_run_id, "metadata.json")
            os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
            
            with open(metadata_file, 'w') as f:
                f.write(test_json)
            
            print(f"Report metadata written to: {metadata_file}", file=sys.stderr)
            print(f"JSON is acceptable size ({json_size} bytes). Metadata file created.", file=sys.stderr)
            sys.stderr.flush()
            
            # Output minimal success message to stdout (for backwards compatibility)
            success_result = {
                "status": "completed",
                "strategy_run_id": json_safe_result.get("strategy_run_id"),
                "metadata_file": metadata_file
            }
            print(json.dumps(success_result))
            sys.stdout.flush()
        except Exception as json_error:
            # If JSON serialization fails, log the error and try to return a minimal error response
            print(f"Error serializing result to JSON: {json_error}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            # Try to create a minimal result with just the error
            error_result = {
                "error": f"Failed to serialize results to JSON: {str(json_error)}",
                "strategy_run_id": result.get("strategy_run_id") if isinstance(result, dict) else None,
                "status": "error"
            }
            print(json.dumps(error_result))
            sys.stdout.flush()
            sys.exit(1)
        
    except Exception as e:
        import traceback
        error_traceback = traceback.format_exc()
        error_result = {
            "error": str(e),
            "traceback": error_traceback,
            "strategy_run_id": config.get("strategy_run_id") if 'config' in locals() else None
        }
        # Print to stderr for logging
        print(f"Backtest error: {e}", file=sys.stderr)
        print(f"Traceback: {error_traceback}", file=sys.stderr)
        # Flush stderr first
        sys.stderr.flush()
        print(json.dumps(error_result), file=sys.stdout)
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()
