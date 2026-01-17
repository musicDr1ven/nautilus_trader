#!/usr/bin/env python3
"""
Dynamic Strategy Generator
Creates a Nautilus Trader strategy from dynamic configuration.
"""

from decimal import Decimal
from typing import Dict, Any, Optional, Callable, List
import sys
import re
import json
import os
import time

from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.model.identifiers import InstrumentId, AccountId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.enums import OrderSide, OrderType, PriceType, BarAggregation
from nautilus_trader.trading.strategy import Strategy


class DynamicStrategyConfig(StrategyConfig, frozen=True, kw_only=True):
    """
    Configuration for DynamicStrategy instances.
    
    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the strategy.
    indicators : Dict[str, Any]
        Dictionary of indicator instances keyed by their names.
    trade_size : Decimal
        Trade size as percentage (e.g., 10 for 10%).
    buy_logic : Optional[Callable]
        Callable function that returns True when buy conditions are met.
    sell_logic : Optional[Callable]
        Callable function that returns True when sell conditions are met.
    enable_long : bool
        Whether long positions are enabled.
    enable_short : bool
        Whether short positions are enabled.
    long_buy_order_type : str
        Order type for long buy orders (MARKET, LIMIT, etc.).
    long_sell_order_type : str
        Order type for long sell orders (MARKET, LIMIT, etc.).
    short_buy_order_type : str
        Order type for short buy orders (MARKET, LIMIT, etc.).
    short_sell_order_type : str
        Order type for short sell orders (MARKET, LIMIT, etc.).
    """
    
    instrument_id: InstrumentId
    indicators: Dict[str, Any]
    trade_size: Decimal
    buy_logic: Optional[Callable] = None
    sell_logic: Optional[Callable] = None
    enable_long: bool = True
    enable_short: bool = False
    long_buy_order_type: str = "MARKET"
    long_sell_order_type: str = "MARKET"
    short_buy_order_type: str = "MARKET"
    short_sell_order_type: str = "MARKET"
    required_timeframes: Optional[List[str]] = None  # List of timeframes like ["1m", "5m"]
    rules_config: Optional[Dict[str, Any]] = None  # Rules config for extracting indicator timeframes
    strategy_data: Optional[List[Dict[str, Any]]] = None  # Original indicator data from database
    allow_multiple_trades: bool = False  # If False, prevent opening new positions when one is already open


class DynamicStrategy(Strategy):
    """
    A dynamic trading strategy that executes buy/sell logic based on configurable rules.
    """
    
    def __init__(self, config: DynamicStrategyConfig) -> None:
        super().__init__(config)
        
        self.instrument: Optional[Instrument] = None
        
        # Create instance attributes for each indicator (like self.fast_ema, self.slow_ema)
        # This allows Nautilus Trader to properly track and update indicators
        self.indicators = {}  # Keep dictionary for easy lookup
        for indicator_name, indicator_instance in config.indicators.items():
            # Create instance attribute using setattr (e.g., self.fast_ema = indicator_instance)
            setattr(self, indicator_name, indicator_instance)
            # Also store in dictionary for easy lookup
            self.indicators[indicator_name] = indicator_instance
            print(f"[INDICATOR_INIT] Created instance attribute self.{indicator_name} = {type(indicator_instance).__name__}", file=sys.stderr)
        
        self.buy_logic = config.buy_logic
        self.sell_logic = config.sell_logic
        self.trade_size = config.trade_size
        self.enable_long = config.enable_long
        self.enable_short = config.enable_short
        self.long_buy_order_type = config.long_buy_order_type
        self.long_sell_order_type = config.long_sell_order_type
        self.short_buy_order_type = config.short_buy_order_type
        self.short_sell_order_type = config.short_sell_order_type
        self.rules_config = config.rules_config or {}
        self.strategy_data = config.strategy_data or []  # Store original database configuration
        self.allow_multiple_trades = config.allow_multiple_trades
        
        # Create mapping from api_name to original indicator data from database
        # This allows us to look up exact configuration without parsing names
        self.indicator_data_map: Dict[str, Dict[str, Any]] = {}
        for ind_data in self.strategy_data:
            api_name = ind_data.get("api_name")
            if api_name:
                self.indicator_data_map[api_name] = ind_data
                print(f"[INDICATOR_INIT] Mapped api_name '{api_name}' to database config: indicator_name={ind_data.get('indicator_name')}, config={ind_data.get('indicator_config')}", file=sys.stderr)
        
        # Parse required timeframes and create bar types
        self.bar_types: Dict[str, BarType] = {}
        self.timeframe_to_bar_type: Dict[str, BarType] = {}
        
        timeframes = config.required_timeframes or ["1m"]
        for timeframe in timeframes:
            bar_spec = self._parse_timeframe_to_bar_spec(timeframe)
            bar_type = BarType(config.instrument_id, bar_spec)
            self.bar_types[timeframe] = bar_type
            self.timeframe_to_bar_type[timeframe] = bar_type
        
        # No primary timeframe - strategy can evaluate logic on any timeframe
        # Use the first timeframe as default bar_type for backward compatibility
        default_timeframe = timeframes[0] if timeframes else "1m"
        self.bar_type = self.bar_types.get(default_timeframe, self.bar_types.get("1m"))
        
        # Map indicators to their timeframes (will be populated in on_start)
        self.indicator_to_timeframe: Dict[str, str] = {}
    
    def _parse_timeframe_to_bar_spec(self, timeframe: str) -> BarSpecification:
        """
        Parse timeframe string (e.g., "5m", "1h", "1d") to BarSpecification.
        
        Args:
            timeframe: Timeframe string like "5m", "1h", "1d"
            
        Returns:
            BarSpecification object
        """
        # Parse format: number + unit (e.g., "5m", "1h", "1d")
        match = re.match(r'^(\d+)([mhd])$', timeframe.lower())
        if not match:
            # Default to 1 minute if parsing fails
            return BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
        
        value = int(match.group(1))
        unit = match.group(2)
        
        if unit == 'm':
            aggregation = BarAggregation.MINUTE
        elif unit == 'h':
            aggregation = BarAggregation.HOUR
        elif unit == 'd':
            aggregation = BarAggregation.DAY
        else:
            aggregation = BarAggregation.MINUTE
        
        return BarSpecification(value, aggregation, PriceType.LAST)
    
    def _extract_indicator_values(self, indicator_name: str, indicator_instance) -> tuple[float | None, dict | None]:
        """
        Extract indicator values (single or multi-value).
        Returns: (single_value, multi_value_dict)
        """
        # Get indicator config from database
        ind_config = self.indicator_data_map.get(indicator_name, {})
        indicator_type = ind_config.get("indicator_name", "UNKNOWN")
        
        # Single-value indicators
        if indicator_type in ["EMA", "SMA", "RSI", "ATR", "KAMA", "ForceIndex", "ZigZag"]:
            try:
                if hasattr(indicator_instance, 'value'):
                    value = float(getattr(indicator_instance, 'value', None) or 0)
                    return (value, None)
                elif hasattr(indicator_instance, 'get_value'):
                    value = float(indicator_instance.get_value())
                    return (value, None)
            except (TypeError, ValueError, AttributeError):
                return (None, None)
        
        # Multi-value indicators
        elif indicator_type == "MACD":
            try:
                # MACDComplete provides macd, signal, and hist properties
                macd_value = float(getattr(indicator_instance, 'macd', 0))
                signal_value = float(getattr(indicator_instance, 'signal', 0))
                hist_value = float(getattr(indicator_instance, 'hist', 0))
                
                return (None, {
                    "macd": macd_value,
                    "signal": signal_value,
                    "hist": hist_value
                })
            except (TypeError, ValueError, AttributeError):
                return (None, None)
        
        elif indicator_type == "Bollinger Bands":
            try:
                return (None, {
                    "upper": float(getattr(indicator_instance, 'upper', 0)),
                    "middle": float(getattr(indicator_instance, 'middle', 0)),
                    "lower": float(getattr(indicator_instance, 'lower', 0))
                })
            except (TypeError, ValueError, AttributeError):
                return (None, None)
        
        elif indicator_type == "SpeedMonitor":
            try:
                return (None, {
                    "ema50": float(getattr(indicator_instance, 'ema50', 0)),
                    "ema100": float(getattr(indicator_instance, 'ema100', 0)),
                    "ema200": float(getattr(indicator_instance, 'ema200', 0))
                })
            except (TypeError, ValueError, AttributeError):
                return (None, None)
        
        elif indicator_type == "Uptrick: Volatility Adjusted Trail":
            try:
                return (None, {
                    "upper": float(getattr(indicator_instance, 'upper', 0)),
                    "lower": float(getattr(indicator_instance, 'lower', 0)),
                    "state": int(getattr(indicator_instance, 'state', 0))
                })
            except (TypeError, ValueError, AttributeError):
                return (None, None)
        
        # Add more indicator types as needed
        else:
            return (None, None)
    
    def _extract_indicator_timeframes_from_rules(self) -> Dict[str, str]:
        """
        Extract indicator-to-timeframe mappings from rules_config.
        Looks for indicator_value references like "slow_ema/default:5m" -> {"slow_ema": "5m"}
        
        The api_name (e.g., "slow_ema") is extracted from the reference format and used as a key.
        Do NOT parse the api_name itself - it's just a key from the strategy_indicators table.
        All indicator details (type, parameters) come from the strategy_data, not from parsing.
        
        Returns:
            Dictionary mapping api_name to timeframe string
        """
        indicator_timeframes = {}
        
        def extract_from_value(value):
            """Recursively extract indicator timeframes from rule values."""
            if isinstance(value, dict):
                # Check for indicator_value type
                if value.get("type") == "indicator_value":
                    indicator_value = value.get("value", "")
                    # Extract api_name and timeframe from format like "slow_ema/default:5m"
                    # api_name is just a key - do not parse it further
                    if ":" in indicator_value:
                        # Split by ":" to get timeframe
                        parts = indicator_value.split(":")
                        if len(parts) >= 2:
                            timeframe = parts[-1]  # Last part is timeframe (e.g., "5m")
                            # Extract api_name (everything before the last ":", then before "/" if present)
                            indicator_ref = ":".join(parts[:-1])
                            api_name = indicator_ref.split("/")[0]  # api_name is just a key, not parsed
                            indicator_timeframes[api_name] = timeframe
                            print(f"[INDICATOR_REG] Found indicator api_name '{api_name}' -> timeframe '{timeframe}' from rule value '{indicator_value}'", file=sys.stderr)
                # Recursively check nested structures
                for v in value.values():
                    extract_from_value(v)
            elif isinstance(value, list):
                for item in value:
                    extract_from_value(item)
        
        # Extract from rules_config (long and short buy/sell logic)
        if self.rules_config:
            print(f"[INDICATOR_REG] Extracting timeframes from rules_config: {self.rules_config}", file=sys.stderr)
            for direction in ["long", "short"]:
                if direction in self.rules_config:
                    direction_config = self.rules_config[direction]
                    for side in ["buy", "sell"]:
                        if side in direction_config:
                            extract_from_value(direction_config[side])
        else:
            print(f"[INDICATOR_REG] WARNING: rules_config is None or empty!", file=sys.stderr)
        
        # Do NOT parse indicator names - api_name is just a key from the database
        # All timeframe info should come from rules_config extraction above
        # All indicator details (type, parameters) come from strategy_data
        
        print(f"[INDICATOR_REG] Final indicator-to-timeframe mapping: {indicator_timeframes}", file=sys.stderr)
        return indicator_timeframes
    
    def on_start(self) -> None:
        """Actions to be performed on strategy start."""
        print(f"[INDICATOR_REG] ===== Strategy on_start() called =====", file=sys.stderr)
        print(f"[INDICATOR_REG] rules_config: {self.rules_config}", file=sys.stderr)
        print(f"[INDICATOR_REG] indicators keys: {list(self.indicators.keys())}", file=sys.stderr)
        
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        
        # Get venue from instrument and construct AccountId
        # AccountId format is typically "{VENUE}-001"
        venue = self.instrument.id.venue
        self.account_id = AccountId(f"{venue}-001")
        
        # Extract indicator-to-timeframe mappings
        self.indicator_to_timeframe = self._extract_indicator_timeframes_from_rules()
        
        # Get distinct timeframes being used in rules_config
        distinct_timeframes = set(self.indicator_to_timeframe.values())
        
        # Add any assigned indicators that aren't in the mapping to the distinct timeframes
        # This ensures indicators like MACD that are assigned but not used in rules still get a timeframe
        for indicator_name in self.indicators.keys():
            if indicator_name not in self.indicator_to_timeframe:
                # Assign to the first distinct timeframe (or first available if none)
                if distinct_timeframes:
                    # Use the first distinct timeframe from rules
                    assigned_timeframe = list(distinct_timeframes)[0]
                else:
                    # Fallback to first available timeframe from required_timeframes
                    assigned_timeframe = list(self.bar_types.keys())[0] if self.bar_types else "1m"
                self.indicator_to_timeframe[indicator_name] = assigned_timeframe
                print(f"[INDICATOR_REG] Assigned unused indicator '{indicator_name}' to timeframe '{assigned_timeframe}' (from rules_config timeframes)", file=sys.stderr)
        
        print(f"[INDICATOR_REG] Indicator-to-timeframe mappings: {self.indicator_to_timeframe}", file=sys.stderr)
        print(f"[INDICATOR_REG] Available indicators: {list(self.indicators.keys())}", file=sys.stderr)
        print(f"[INDICATOR_REG] Available bar types: {list(self.bar_types.keys())}", file=sys.stderr)
        
        # Register indicators to their specific bar types based on timeframe
        # Use instance attributes directly (like self.fast_ema) to match Nautilus Trader pattern
        # Use exact api_name from database (no parsing!)
        # Note: Indicators don't have a public update() method - Nautilus Trader handles updates automatically
        for indicator_name, indicator in self.indicators.items():
            # Get the instance attribute (e.g., self.fast_ema) instead of using the dict value
            # This ensures Nautilus Trader can properly track the indicator
            indicator_instance = getattr(self, indicator_name, None)
            if indicator_instance is None:
                print(f"[INDICATOR_REG] WARNING: Instance attribute self.{indicator_name} not found, using dict value", file=sys.stderr)
                indicator_instance = indicator
            
            # Get exact api_name from database configuration (no parsing!)
            # indicator_name is the api_name from the database (e.g., "fast_ema", "slow_ema")
            api_name = indicator_name
            
            # Get timeframe for this indicator from rules_config mapping
            # The mapping was created by extracting from rules like "fast_ema/default:5m"
            timeframe = self.indicator_to_timeframe.get(api_name)
            
            # Get original database config for this indicator
            db_config = self.indicator_data_map.get(api_name, {})
            indicator_type = db_config.get("indicator_name", "UNKNOWN")
            indicator_config = db_config.get("indicator_config", {})
            
            print(f"[INDICATOR_REG] Processing indicator '{api_name}' from database: type={indicator_type}, config={indicator_config}, timeframe: {timeframe}", file=sys.stderr)
            
            # Check if this is a custom indicator wrapper (like MACDComplete) that has an internal indicator
            # Custom indicators that wrap Nautilus indicators need special handling
            internal_indicator = None
            is_wrapper_indicator = False
            if hasattr(indicator_instance, '_macd') and hasattr(indicator_instance, 'macd'):
                # MACDComplete - register the internal _macd indicator
                internal_indicator = indicator_instance._macd
                is_wrapper_indicator = True
            elif hasattr(indicator_instance, '_ema1') and hasattr(indicator_instance, 'ema50'):
                # SpeedMonitor - register the first EMA (they all get updated together)
                internal_indicator = indicator_instance._ema1
                is_wrapper_indicator = True
            elif hasattr(indicator_instance, '_ema') and hasattr(indicator_instance, 'upper'):
                # UptrickVolatilityTrail - register the internal EMA
                internal_indicator = indicator_instance._ema
                is_wrapper_indicator = True
            
            # Use internal indicator if available, otherwise use the indicator instance directly
            indicator_to_register = internal_indicator if internal_indicator is not None else indicator_instance
            
            # Track wrapper indicators that need manual updates even when internal indicator is registered
            if is_wrapper_indicator:
                if not hasattr(self, '_wrapper_indicators'):
                    self._wrapper_indicators = {}
                self._wrapper_indicators[api_name] = indicator_instance
            
            if timeframe and timeframe in self.bar_types:
                # Register to specific bar type using instance attribute
                # Nautilus Trader will automatically update the indicator when matching bars arrive
                bar_type = self.bar_types[timeframe]
                try:
                    self.register_indicator_for_bars(bar_type, indicator_to_register)
                    print(f"[INDICATOR_REG] ✓ Registered indicator '{api_name}' (type={indicator_type}) to {timeframe} bars ({str(bar_type)})", file=sys.stderr)
                except TypeError as e:
                    # If registration fails (e.g., custom indicator), we'll update it manually in on_bar
                    print(f"[INDICATOR_REG] ⚠ WARNING: Could not register '{api_name}' automatically (type={indicator_type}): {e}. Will update manually in on_bar.", file=sys.stderr)
                    # Store a flag to update this indicator manually
                    if not hasattr(self, '_manual_indicators'):
                        self._manual_indicators = {}
                    self._manual_indicators[api_name] = indicator_instance
            else:
                # Fallback: register to all bar types if timeframe not found
                print(f"[INDICATOR_REG] ⚠ WARNING: Could not determine timeframe for indicator '{api_name}' (type={indicator_type}), available mappings: {self.indicator_to_timeframe}, registering to all bar types", file=sys.stderr)
                for bar_type in self.bar_types.values():
                    try:
                        self.register_indicator_for_bars(bar_type, indicator_to_register)
                        print(f"[INDICATOR_REG]   Registered '{api_name}' to {str(bar_type)}", file=sys.stderr)
                    except TypeError as e:
                        print(f"[INDICATOR_REG] ⚠ WARNING: Could not register '{api_name}' to {str(bar_type)}: {e}. Will update manually in on_bar.", file=sys.stderr)
                        if not hasattr(self, '_manual_indicators'):
                            self._manual_indicators = {}
                        self._manual_indicators[api_name] = indicator_instance
        
        # Subscribe to all required bar types
        for bar_type in self.bar_types.values():
            self.subscribe_bars(bar_type)
        
        self.subscribe_trade_ticks(self.config.instrument_id)
        
        # Initialize debug counters
        self._bar_count = 0
        self._buy_trigger_count = 0
        self._sell_trigger_count = 0
        self._buy_logic_true_count = 0
        self._sell_logic_true_count = 0
        self._5m_bar_count = 0
        
    
    def on_stop(self) -> None:
        """Actions to be performed on strategy stop."""
        # Print summary
        print(f"\n[SUMMARY] Strategy stopped:", file=sys.stderr)
        print(f"  Total bars processed: {getattr(self, '_bar_count', 0)}", file=sys.stderr)
        print(f"  5m bars processed: {getattr(self, '_5m_bar_count', 0)}", file=sys.stderr)
        print(f"  Buy logic TRUE count: {getattr(self, '_buy_logic_true_count', 0)}", file=sys.stderr)
        print(f"  Sell logic TRUE count: {getattr(self, '_sell_logic_true_count', 0)}", file=sys.stderr)
        print(f"  Buy orders placed: {getattr(self, '_buy_trigger_count', 0)}", file=sys.stderr)
        print(f"  Sell orders placed: {getattr(self, '_sell_trigger_count', 0)}", file=sys.stderr)
    
    def on_bar(self, bar: Bar) -> None:
        """
        Actions to be performed when the strategy receives a bar.
        Evaluates buy/sell logic on every bar arrival, allowing conditions
        to check indicators across multiple timeframes.
        """
        if bar.is_single_price():
            return
        
        # Identify which timeframe this bar is from (for logging/debugging)
        bar_timeframe = None
        for timeframe, bar_type in self.bar_types.items():
            if bar.bar_type == bar_type:
                bar_timeframe = timeframe
                break
        
        # Update wrapper indicators (custom indicators that wrap internal indicators)
        # These need to be updated to calculate derived values (e.g., signal, histogram for MACDComplete)
        if hasattr(self, '_wrapper_indicators'):
            for indicator_name, indicator_instance in self._wrapper_indicators.items():
                # Only update if this indicator is for the current timeframe
                indicator_timeframe = self.indicator_to_timeframe.get(indicator_name)
                if indicator_timeframe == bar_timeframe or indicator_timeframe is None:
                    try:
                        if hasattr(indicator_instance, 'handle_bar'):
                            indicator_instance.handle_bar(bar)
                    except Exception:
                        # Silently fail - don't interrupt strategy execution
                        pass
        
        # Update manual indicators (custom indicators that couldn't be registered automatically)
        if hasattr(self, '_manual_indicators'):
            for indicator_name, indicator_instance in self._manual_indicators.items():
                # Only update if this indicator is for the current timeframe
                indicator_timeframe = self.indicator_to_timeframe.get(indicator_name)
                if indicator_timeframe == bar_timeframe or indicator_timeframe is None:
                    try:
                        if hasattr(indicator_instance, 'handle_bar'):
                            indicator_instance.handle_bar(bar)
                    except Exception:
                        # Silently fail - don't interrupt strategy execution
                        pass
        
        # Track bar counts for summary (initialize early)
        if not hasattr(self, '_bar_count'):
            self._bar_count = 0
            self._buy_trigger_count = 0
            self._sell_trigger_count = 0
            self._buy_logic_true_count = 0
            self._sell_logic_true_count = 0
        self._bar_count += 1
        
        # Indicators are automatically updated by Nautilus Trader after register_indicator_for_bars
        # Automatically capture indicator values for storage in GreptimeDB
        for indicator_name, indicator_instance in self.indicators.items():
            # Check if indicator is initialized and ready
            if not (hasattr(indicator_instance, 'initialized') and indicator_instance.initialized):
                continue
            
            # Get the timeframe/bar_type for this indicator
            indicator_timeframe = self.indicator_to_timeframe.get(indicator_name)
            if not indicator_timeframe or indicator_timeframe != bar_timeframe:
                continue
            
            # This indicator is associated with the current bar's timeframe
            # Extract indicator values
            single_value, multi_value_dict = self._extract_indicator_values(indicator_name, indicator_instance)
            
            # Only store if we have valid values
            if single_value is None and multi_value_dict is None:
                continue
            
            # Get the bar_type for this indicator's timeframe
            indicator_bar_type = self.bar_types.get(indicator_timeframe)
            if not indicator_bar_type:
                continue
            
            # Get indicator type from database config
            ind_config = self.indicator_data_map.get(indicator_name, {})
            indicator_type = ind_config.get("indicator_name", "UNKNOWN")
            
            # Store indicator data in cache database adapter
            # The cache has a .database property that gives access to the database adapter
            try:
                database_adapter = getattr(self.cache, 'database', None)
                if database_adapter and hasattr(database_adapter, 'add_indicator'):
                    database_adapter.add_indicator(
                        instrument_id=self.config.instrument_id,
                        indicator_name=indicator_name,
                        indicator_type=indicator_type,
                        bar_type=indicator_bar_type,
                        timestamp=int(bar.ts_event),
                        value=single_value,
                        values_dict=multi_value_dict,
                    )
            except Exception:
                # Silently fail - don't interrupt strategy execution
                pass
        
        # Check indicator initialization status on first few bars
        if not hasattr(self, '_indicator_status_checked'):
            self._indicator_status_checked = {}
        
        # Log indicator status periodically (every 1000 bars for 5m timeframe)
        if bar_timeframe == "5m" and self._bar_count % 1000 == 0:
            for api_name, indicator_instance in self.indicators.items():
                if api_name not in self._indicator_status_checked or self._indicator_status_checked[api_name] < 5:
                    initialized = getattr(indicator_instance, 'initialized', False)
                    value = None
                    if initialized:
                        try:
                            value = float(getattr(indicator_instance, 'value', None) or 0)
                        except:
                            pass
                    db_config = self.indicator_data_map.get(api_name, {})
                    indicator_type = db_config.get("indicator_name", "UNKNOWN")
                    period = db_config.get("indicator_config", {}).get("period", "?")
                    print(f"[INDICATOR_STATUS] {api_name} (type={indicator_type}, period={period}): initialized={initialized}, value={value}", file=sys.stderr)
                    self._indicator_status_checked[api_name] = self._indicator_status_checked.get(api_name, 0) + 1
        
        # Check if we have valid logic functions
        if not self.buy_logic or not self.sell_logic:
            return
        
        # Determine which timeframes are used by the indicators
        # We should ONLY evaluate logic on bars from timeframes that have indicators
        indicator_timeframes = set(self.indicator_to_timeframe.values())
        
        # Only evaluate logic if this bar is from a timeframe that has indicators
        # This prevents evaluating logic on 1m bars when all indicators are on 5m bars
        if indicator_timeframes and bar_timeframe not in indicator_timeframes:
            # This bar's timeframe doesn't have any indicators, skip evaluation
            return
        
        # Evaluate buy/sell logic on bar arrival
        # This allows rules to check conditions across multiple timeframes
        # For example: buy when 1m indicator > X AND 5m indicator > Y
        # The logic can check any indicator's current value regardless of which bar triggered
        
        # Only log indicator values on 5m bars and only every 10th 5m bar
        if bar_timeframe == "5m":
            if not hasattr(self, '_5m_bar_count'):
                self._5m_bar_count = 0
            self._5m_bar_count += 1
            
            # Log indicator values every 10th 5m bar
            if self._5m_bar_count % 10 == 0:
                indicator_values = {}
                for name, indicator in self.indicators.items():
                    if hasattr(indicator, 'initialized') and indicator.initialized:
                        if hasattr(indicator, 'value'):
                            try:
                                indicator_values[name] = float(indicator.value)
                            except (TypeError, ValueError, AttributeError):
                                indicator_values[name] = None
                        elif hasattr(indicator, 'get_value'):
                            try:
                                indicator_values[name] = float(indicator.get_value())
                            except (TypeError, ValueError, AttributeError):
                                indicator_values[name] = None
        # Check position state before evaluating buy logic (if allow_multiple_trades is False)
        is_flat = self.portfolio.is_flat(self.config.instrument_id)
        is_long = self.portfolio.is_net_long(self.config.instrument_id)
        is_short = self.portfolio.is_net_short(self.config.instrument_id)
        
        # Get remaining USD balance for logging
        remaining_usd = None
        try:
            account = self.cache.account(self.account_id)
            if account and self.instrument:
                balance = account.balance(self.instrument.quote_currency)
                if balance:
                    remaining_usd = float(balance.free.as_decimal())
        except Exception:
            pass
        
        # If allow_multiple_trades is False and we have an open position, skip buy logic evaluation
        should_buy = False
        if self.allow_multiple_trades or is_flat:
            # Evaluate buy logic only if allow_multiple_trades is True OR position is flat
            try:
                should_buy = self.buy_logic()
                if should_buy:
                    self._buy_logic_true_count += 1
            except Exception as e:
                print(f"[ERROR] Error evaluating buy logic: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()
                return
        else:
            # Position is open and allow_multiple_trades is False - skip buy logic
            # Log remaining balance periodically (every 100th 5m bar)
            if bar_timeframe == "5m" and hasattr(self, '_5m_bar_count') and self._5m_bar_count % 100 == 0:
                if remaining_usd is not None:
                    print(f"[BALANCE] Position open, remaining USD: {remaining_usd:.2f} (5m bar #{self._5m_bar_count})", file=sys.stderr)
        
        # Evaluate sell logic
        # Get indicator values for logging
        fast_ema_val = None
        slow_ema_val = None
        try:
            if "fast_ema" in self.indicators:
                fast_ema = self.indicators["fast_ema"]
                if hasattr(fast_ema, 'initialized') and fast_ema.initialized:
                    if hasattr(fast_ema, 'value'):
                        fast_ema_val = float(fast_ema.value)
                    elif hasattr(fast_ema, 'get_value'):
                        fast_ema_val = float(fast_ema.get_value())
            if "slow_ema" in self.indicators:
                slow_ema = self.indicators["slow_ema"]
                if hasattr(slow_ema, 'initialized') and slow_ema.initialized:
                    if hasattr(slow_ema, 'value'):
                        slow_ema_val = float(slow_ema.value)
                    elif hasattr(slow_ema, 'get_value'):
                        slow_ema_val = float(slow_ema.get_value())
        except Exception:
            pass
        
        try:
            should_sell = self.sell_logic()
            if should_sell:
                self._sell_logic_true_count += 1
        except Exception as e:
            print(f"[ERROR] Error evaluating sell logic: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            return
        
        # Execute trades based on logic
        # Position state already checked above
        
        if should_buy and self.enable_long:
            # Safety check: if allow_multiple_trades is False and position is not flat, skip
            if not self.allow_multiple_trades and not is_flat:
                # Already logged above, just skip order placement
                pass
            elif is_flat:
                self._buy_trigger_count += 1
                print(f"[TRADE] BUY signal on {bar_timeframe} bar at {bar.close} (trigger #{self._buy_trigger_count})", file=sys.stderr)
                self._place_buy_order(OrderSide.BUY, self.long_buy_order_type)
            elif is_short:
                # Close short position and go long
                self._buy_trigger_count += 1
                print(f"[TRADE] BUY signal (close short) on {bar_timeframe} bar at {bar.close} (trigger #{self._buy_trigger_count})", file=sys.stderr)
                self.close_all_positions(self.config.instrument_id)
                self._place_buy_order(OrderSide.BUY, self.long_buy_order_type)
        
        if should_sell and self.enable_long:
            if is_long:
                self._sell_trigger_count += 1
                print(f"[TRADE] SELL signal on {bar_timeframe} bar at {bar.close} (trigger #{self._sell_trigger_count})", file=sys.stderr)
                self._place_sell_order(OrderSide.SELL, self.long_sell_order_type)
        
        if should_sell and self.enable_short:
            if is_flat:
                # For short, selling opens a short position
                self._place_sell_order(OrderSide.SELL, self.short_sell_order_type)
            elif is_long:
                # Close long position and go short
                self.close_all_positions(self.config.instrument_id)
                self._place_sell_order(OrderSide.SELL, self.short_sell_order_type)
        
        if should_buy and self.enable_short:
            # Safety check: if allow_multiple_trades is False and position is not flat, skip
            if not self.allow_multiple_trades and not is_flat:
                # Already logged above, just skip order placement
                pass
            elif is_short:
                self._place_sell_order(OrderSide.SELL, self.short_sell_order_type)
            elif is_flat:
                # For short, buying closes a short position
                self._place_buy_order(OrderSide.BUY, self.short_buy_order_type)
    
    def _place_buy_order(self, side: OrderSide, order_type_str: str) -> None:
        """Place a buy order."""
        if self.instrument is None:
            return
        
        # Get available balance
        account = self.cache.account(self.account_id)
        if account is None:
            return
        
        balance = account.balance(self.instrument.quote_currency)
        if balance is None:
            return
        
        available = balance.free
        if available.as_decimal() <= 0:
            return
        
        # Calculate trade value (trade_size is a percentage, e.g., 10 for 10%)
        trade_value = float(available.as_decimal()) * (float(self.trade_size) / 100.0)
        
        # Get current price from last bar or trade tick
        last_price = None
        # Try to get price from the most recent bar of any subscribed type
        # Sort by timeframe (1m, 5m, 15m, etc.) - shorter timeframes first
        def timeframe_to_minutes(tf: str) -> int:
            """Convert timeframe string to minutes for sorting."""
            if tf.endswith('m'):
                return int(tf[:-1])
            elif tf.endswith('h'):
                return int(tf[:-1]) * 60
            elif tf.endswith('d'):
                return int(tf[:-1]) * 1440
            return 0
        
        sorted_timeframes = sorted(self.bar_types.keys(), key=timeframe_to_minutes)
        for timeframe in sorted_timeframes:
            bar_type = self.bar_types[timeframe]
            last_bar = self.cache.bar(bar_type)
            if last_bar:
                last_price = float(last_bar.close.as_double())
                break
        
        if last_price is None:
            # Fallback: try to get from trade ticks
            last_tick = self.cache.trade_tick(self.instrument.id)
            if last_tick:
                last_price = float(last_tick.price.as_double())
        
        if last_price is None or last_price <= 0:
            self.log.warning(f"Could not determine last price for {self.instrument.id}, skipping buy order.")
            return
        
        # Calculate quantity
        quantity_value = trade_value / last_price
        
        # Create quantity using instrument's make_qty to respect size_precision
        quantity = self.instrument.make_qty(quantity_value)
        
        # Validate quantity meets instrument constraints
        if quantity.as_decimal() <= 0:
            return
        
        # Check minimum quantity constraint
        if hasattr(self.instrument, 'min_quantity') and self.instrument.min_quantity:
            if quantity < self.instrument.min_quantity:
                self.log.warning(f"Quantity {quantity} is below minimum {self.instrument.min_quantity}, skipping order.")
                return
        
        # Check maximum quantity constraint
        if hasattr(self.instrument, 'max_quantity') and self.instrument.max_quantity:
            if quantity > self.instrument.max_quantity:
                # Clamp to max quantity
                quantity = self.instrument.max_quantity
                self.log.warning(f"Quantity clamped to maximum {self.instrument.max_quantity}.")
        
        # Create order based on order type using order_factory
        order_type = OrderType[order_type_str]

        if order_type == OrderType.MARKET:
            order = self.order_factory.market(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=quantity,
            )
        elif order_type == OrderType.LIMIT:
            # For limit orders, use current price
            limit_price = self.instrument.make_price(last_price)
            order = self.order_factory.limit(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=quantity,
                price=limit_price,
            )
        else:
            # Default to market order
            order = self.order_factory.market(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=quantity,
            )
        
        # IMPORTANT: Set account_id on the order after creation
        # Nautilus doesn't set account_id until submit_order(), but we need it for the cache
        # Try to set it directly on the order object
        try:
            if hasattr(order, 'account_id'):
                # If account_id is a property/setter, try to set it
                try:
                    order.account_id = self.account_id
                except (AttributeError, TypeError):
                    # If direct assignment doesn't work, try using setattr
                    setattr(order, 'account_id', self.account_id)
        except Exception:
            # If we can't set it, the order will get account_id when submit_order() is called
            # But the cache might be called before that, so we'll need to handle it in Rust
            pass
        
        # Submit order with error handling for assertion errors
        try:
            self.submit_order(order)
        except AssertionError as e:
            # Log assertion errors from order book but don't crash the backtest
            error_msg = str(e)
            print(f"[ORDER_ERROR] AssertionError submitting BUY {order_type_str} order: quantity={quantity}, price={last_price}, error={error_msg}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            # Continue execution - don't crash the backtest
        except Exception as e:
            # Log other errors
            error_msg = str(e)
            print(f"[ORDER_ERROR] Error submitting BUY {order_type_str} order: quantity={quantity}, price={last_price}, error={error_msg}", file=sys.stderr)
            import traceback
            traceback.print_exc()
    
    def _place_sell_order(self, side: OrderSide, order_type_str: str) -> None:
        """Place a sell order."""
        if self.instrument is None:
            return
        
        # Get position from cache by finding open positions for this instrument
        # cache.position() expects PositionId, so we need to find it by instrument
        position = None
        positions_open = self.cache.positions_open()
        for pos in positions_open:
            if pos.instrument_id == self.config.instrument_id:
                position = pos
                break
        
        if position is not None:
            # Check if position has quantity (not flat)
            # quantity is a property, not a method (see https://nautilustrader.io/docs/latest/concepts/positions/)
            pos_quantity = position.quantity
            if pos_quantity.as_decimal() != 0:
                # Use position quantity
                quantity = pos_quantity
            else:
                # Position is flat, fall through to balance calculation
                position = None
        
        if position is None:
            # Get available balance
            account = self.cache.account(self.account_id)
            if account is None:
                return
            
            balance = account.balance(self.instrument.base_currency)
            if balance is None:
                return
            
            available = balance.free
            if available.as_decimal() <= 0:
                return
            
            # Calculate trade value (trade_size is a percentage, e.g., 10 for 10%)
            trade_value = float(available.as_decimal()) * (float(self.trade_size) / 100.0)
            
            # Get current price
            last_price = float(self.cache.last_price(self.instrument.id))
            if last_price <= 0:
                return
            
            # Calculate quantity
            quantity_value = trade_value / last_price
            
            # Create quantity using instrument's make_qty
            quantity = self.instrument.make_qty(quantity_value)
        else:
            pass
        
        # Validate quantity meets instrument constraints
        if quantity.as_decimal() <= 0:
            return
        
        # Check minimum quantity constraint
        if hasattr(self.instrument, 'min_quantity') and self.instrument.min_quantity:
            if quantity < self.instrument.min_quantity:
                self.log.warning(f"Quantity {quantity} is below minimum {self.instrument.min_quantity}, skipping order.")
                return
        
        # Check maximum quantity constraint
        if hasattr(self.instrument, 'max_quantity') and self.instrument.max_quantity:
            if quantity > self.instrument.max_quantity:
                # Clamp to max quantity
                quantity = self.instrument.max_quantity
                self.log.warning(f"Quantity clamped to maximum {self.instrument.max_quantity}.")
        
        # Get current price for limit orders
        last_price = None
        # Sort by timeframe (1m, 5m, 15m, etc.) - shorter timeframes first
        def timeframe_to_minutes(tf: str) -> int:
            """Convert timeframe string to minutes for sorting."""
            if tf.endswith('m'):
                return int(tf[:-1])
            elif tf.endswith('h'):
                return int(tf[:-1]) * 60
            elif tf.endswith('d'):
                return int(tf[:-1]) * 1440
            return 0
        
        sorted_timeframes = sorted(self.bar_types.keys(), key=timeframe_to_minutes)
        for timeframe in sorted_timeframes:
            bar_type = self.bar_types[timeframe]
            last_bar = self.cache.bar(bar_type)
            if last_bar:
                last_price = float(last_bar.close.as_double())
                break
        
        if last_price is None:
            # Fallback: try to get from trade ticks
            last_tick = self.cache.trade_tick(self.instrument.id)
            if last_tick:
                last_price = float(last_tick.price.as_double())
        
        if last_price is None or last_price <= 0:
            self.log.warning(f"Could not determine last price for {self.instrument.id}, skipping sell order.")
            return
        
        # Create order based on order type using order_factory
        order_type = OrderType[order_type_str]

        if order_type == OrderType.MARKET:
            order = self.order_factory.market(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=quantity,
            )
        elif order_type == OrderType.LIMIT:
            # For limit orders, use current price
            limit_price = self.instrument.make_price(last_price)
            order = self.order_factory.limit(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=quantity,
                price=limit_price,
            )
        else:
            # Default to market order
            order = self.order_factory.market(
                instrument_id=self.instrument.id,
                order_side=side,
                quantity=quantity,
            )
        
        # IMPORTANT: Set account_id on the order after creation
        # Nautilus doesn't set account_id until submit_order(), but we need it for the cache
        # Try to set it directly on the order object
        try:
            if hasattr(order, 'account_id'):
                # If account_id is a property/setter, try to set it
                try:
                    order.account_id = self.account_id
                except (AttributeError, TypeError):
                    # If direct assignment doesn't work, try using setattr
                    setattr(order, 'account_id', self.account_id)
        except Exception:
            # If we can't set it, the order will get account_id when submit_order() is called
            # But the cache might be called before that, so we'll need to handle it in Rust
            pass
        
        # Submit order with error handling for assertion errors
        try:
            self.submit_order(order)
        except AssertionError as e:
            # Log assertion errors from order book but don't crash the backtest
            error_msg = str(e)
            print(f"[ORDER_ERROR] AssertionError submitting SELL {order_type_str} order: quantity={quantity}, error={error_msg}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            # Continue execution - don't crash the backtest
        except Exception as e:
            # Log other errors
            error_msg = str(e)
            print(f"[ORDER_ERROR] Error submitting SELL {order_type_str} order: quantity={quantity}, error={error_msg}", file=sys.stderr)
            import traceback
            traceback.print_exc()
    
    def _parse_order_type(self, order_type_str: str) -> OrderType:
        """Parse order type string to OrderType enum."""
        order_type_str = order_type_str.upper()
        if order_type_str == "MARKET":
            return OrderType.MARKET
        elif order_type_str == "LIMIT":
            return OrderType.LIMIT
        else:
            return OrderType.MARKET

