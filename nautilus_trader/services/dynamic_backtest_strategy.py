#!/usr/bin/env python3
"""
Dynamic Backtest Strategy
Handles parsing and configuration of dynamic trading strategies for backtesting.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
import sys

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.data import Bar


class DynamicBacktestStrategy:
    """
    Handles parsing and configuration of dynamic trading strategies for backtesting.
    """
    
    def __init__(
        self,
        strategy_config: Dict[str, Any],
        strategy_data: List[Dict[str, Any]],
        bars: List[Bar],
    ):
        """
        Initialize the dynamic backtest strategy parser.
        
        Args:
            strategy_config: Strategy configuration dictionary
            strategy_data: List of indicator configurations
            bars: List of bars for indicator initialization
        """
        self.strategy_config = strategy_config
        self.strategy_data = strategy_data
        self.bars = bars
        self.strategy_settings = strategy_config.get("strategy_settings", {})
        self.rules_config = strategy_config.get("rules_config", {})
        
    def parse_rules_config(self) -> Tuple[Optional[Dict], Optional[Dict], Optional[Dict], Optional[Dict]]:
        """
        Parse rules_config to extract buy and sell logic.
        rules_config structure:
        {
            "long": {
                "buy": {...},
                "sell": {...}
            },
            "short": {
                "buy": {...},
                "sell": {...}
            }
        }
        
        Returns:
            Tuple of (long_buy_logic, long_sell_logic, short_buy_logic, short_sell_logic)
        """
        long_buy_logic = None
        long_sell_logic = None
        short_buy_logic = None
        short_sell_logic = None
        
        if "long" in self.rules_config:
            long_config = self.rules_config["long"]
            long_buy_logic = long_config.get("buy")
            long_sell_logic = long_config.get("sell")
        
        if "short" in self.rules_config:
            short_config = self.rules_config["short"]
            short_buy_logic = short_config.get("buy")
            short_sell_logic = short_config.get("sell")
        
        return long_buy_logic, long_sell_logic, short_buy_logic, short_sell_logic
    
    def get_enable_flags(self) -> Tuple[bool, bool]:
        """
        Get enable_long and enable_short from strategy_settings.
        
        Returns:
            Tuple of (enable_long, enable_short)
        """
        enable_long = self.strategy_settings.get("ENABLE_LONG", False)
        enable_short = self.strategy_settings.get("ENABLE_SHORT", False)
        return enable_long, enable_short
    
    def get_order_types(self) -> Dict[str, str]:
        """
        Get order types from strategy_settings.
        
        Returns:
            Dictionary with order type keys
        """
        return {
            "long_buy_order_type": self.strategy_settings.get("LONG_BUY_ORDER_TYPE", "MARKET"),
            "long_sell_order_type": self.strategy_settings.get("LONG_SELL_ORDER_TYPE", "MARKET"),
            "short_buy_order_type": self.strategy_settings.get("SHORT_BUY_ORDER_TYPE", "MARKET"),
            "short_sell_order_type": self.strategy_settings.get("SHORT_SELL_ORDER_TYPE", "MARKET"),
        }
    
    def get_trade_size(self) -> float:
        """
        Get trade size from strategy_settings.
        trade_size is stored as a percentage (e.g., 10 for 10%).
        DynamicStrategy will divide by 100 when calculating trade_value.
        
        Returns:
            Trade size as percentage (e.g., 10 for 10%)
        """
        return self.strategy_settings.get("Trade_Size_Percent") or self.strategy_settings.get("trade_size", 10)
    
    def extract_timeframes_from_rules(self) -> set:
        """
        Extract timeframes from rules_config indicator values.
        Example: "slow_ema/default:5m" -> "5m"
        
        Returns:
            Set of timeframe strings (e.g., {"5m", "1m"})
        """
        timeframes = set()
        
        def extract_from_value(value):
            """Recursively extract timeframes from rule values."""
            if isinstance(value, dict):
                # Check for indicator_value type
                if value.get("type") == "indicator_value":
                    indicator_value = value.get("value", "")
                    # Extract timeframe from format like "slow_ema/default:5m"
                    if ":" in indicator_value:
                        timeframe = indicator_value.split(":")[-1]
                        timeframes.add(timeframe)
                # Recursively check nested structures
                for v in value.values():
                    extract_from_value(v)
            elif isinstance(value, list):
                for item in value:
                    extract_from_value(item)
        
        # Extract from long and short rules
        long_buy, long_sell, short_buy, short_sell = self.parse_rules_config()
        for rule in [long_buy, long_sell, short_buy, short_sell]:
            if rule:
                extract_from_value(rule)
        
        return timeframes
    
    def get_required_timeframes(self) -> List[str]:
        """
        Get all required timeframes from Additional_Aggregations and rules_config.
        
        Returns:
            List of timeframe strings (e.g., ["1m", "5m"])
        """
        timeframes = set()
        
        # Get from Additional_Aggregations
        additional_aggs = self.strategy_settings.get("Additional_Aggregations", [])
        if isinstance(additional_aggs, list):
            timeframes.update(additional_aggs)
        
        # Extract from rules_config indicator values
        rule_timeframes = self.extract_timeframes_from_rules()
        timeframes.update(rule_timeframes)
        
        # If no timeframes found, default to 1m
        if not timeframes:
            timeframes.add("1m")
        
        # Sort and return as list
        return sorted(list(timeframes))
    
    def create_strategy(self, instrument_id: InstrumentId) -> Any:
        """
        Create and configure the trading strategy.
        
        Args:
            instrument_id: The instrument ID for the strategy
            
        Returns:
            Configured strategy instance
        """
        # Parse rules_config to get long/short buy/sell logic
        long_buy_logic, long_sell_logic, short_buy_logic, short_sell_logic = self.parse_rules_config()
        
        # Get enable flags from strategy_settings
        enable_long, enable_short = self.get_enable_flags()
        
        # Get order types
        order_types = self.get_order_types()
        
        # Get trade size
        trade_size = self.get_trade_size()
        
        print(f"Strategy settings: enable_long={enable_long}, enable_short={enable_short}", file=sys.stderr)
        print(f"Order types: long_buy={order_types['long_buy_order_type']}, long_sell={order_types['long_sell_order_type']}, "
              f"short_buy={order_types['short_buy_order_type']}, short_sell={order_types['short_sell_order_type']}", file=sys.stderr)
        
        # Import required services
        from nautilus_trader.services.strategy_generator import DynamicStrategy, DynamicStrategyConfig
        from nautilus_trader.services.backtest_service_impl import create_indicators, parse_rules_to_strategy_logic
        
        # Create indicators
        indicators = create_indicators(self.strategy_data, self.bars)
        
        # Parse rules to strategy logic
        buy_logic, sell_logic = parse_rules_to_strategy_logic(
            self.rules_config,
            indicators,
            enable_long=enable_long,
            enable_short=enable_short,
        )
        
        # Get required timeframes
        required_timeframes = self.get_required_timeframes()
        print(f"Required timeframes: {required_timeframes}", file=sys.stderr)
        
        # Create strategy config
        # Note: trade_size is passed as-is (percentage value like 10 for 10%)
        # DynamicStrategy._place_buy_order and _place_sell_order will divide by 100.0
        # when calculating: trade_value = available * (trade_size / 100.0)
        strategy_config_obj = DynamicStrategyConfig(
            instrument_id=instrument_id,
            indicators=indicators,
            buy_logic=buy_logic,
            sell_logic=sell_logic,
            trade_size=trade_size,  # Percentage value (e.g., 10 for 10%)
            enable_long=enable_long,
            enable_short=enable_short,
            long_buy_order_type=order_types["long_buy_order_type"],
            long_sell_order_type=order_types["long_sell_order_type"],
            short_buy_order_type=order_types["short_buy_order_type"],
            short_sell_order_type=order_types["short_sell_order_type"],
            required_timeframes=required_timeframes,  # Pass timeframes to strategy
            rules_config=self.rules_config,  # Pass rules_config for indicator timeframe extraction
            strategy_data=self.strategy_data,  # Pass original database configuration
            allow_multiple_trades=False,  # Default to False - can be read from strategy_settings later
        )
        
        strategy = DynamicStrategy(config=strategy_config_obj)
        return strategy

