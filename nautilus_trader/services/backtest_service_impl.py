#!/usr/bin/env python3
"""
Backtest Service Implementation
Helper functions for backtest service.
"""

from typing import Dict, Any, List, Optional, Callable
import sys
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.data import Bar
from nautilus_trader.model.enums import PriceType
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.indicators.average.sma import SimpleMovingAverage
from nautilus_trader.indicators.average.ama import AdaptiveMovingAverage
from nautilus_trader.indicators.rsi import RelativeStrengthIndex
from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence
from nautilus_trader.indicators.atr import AverageTrueRange
from nautilus_trader.indicators.bollinger_bands import BollingerBands
from nautilus_trader.indicators.custom.force_index import ForceIndex
from nautilus_trader.indicators.custom.macd_complete import MACDComplete
from nautilus_trader.indicators.custom.speed_monitor import SpeedMonitor
from nautilus_trader.indicators.custom.uptrick_volatility_trail import UptrickVolatilityTrail
from nautilus_trader.indicators.custom.zigzag import ZigZag


def create_currency_pair_from_id(instrument_id_str: str, bars: List[Bar] = None) -> CurrencyPair:
    """
    Create a CurrencyPair from instrument ID string.
    This is a placeholder - actual implementation is in backtest_service.py
    """
    raise NotImplementedError("Use create_currency_pair_from_instrument_id in backtest_service.py")


def create_indicators(strategy_data: List[Dict[str, Any]], bars: List[Bar]) -> Dict[str, Any]:
    """
    Create indicator instances from strategy data.
    
    Args:
        strategy_data: List of indicator configurations with:
            - indicator_name: e.g., "EMA", "SMA"
            - indicator_config: JSON config with period, price_type, etc.
            - api_name: Optional reference name (e.g., "fast_ema", "slow_ema")
        bars: List of bars for indicator initialization (not used currently)
    
    Returns:
        Dictionary of indicator instances keyed by api_name or indicator_name
    """
    indicators = {}
    
    for ind_data in strategy_data:
        indicator_name = ind_data.get("indicator_name", "").upper()
        indicator_config = ind_data.get("indicator_config", {})
        api_name = ind_data.get("api_name")
        
        if not indicator_name:
            print(f"Warning: Skipping indicator with no indicator_name", file=sys.stderr)
            continue
        
        # Get period from config (default to 10 if not specified)
        period = indicator_config.get("period") or indicator_config.get("length") or 10
        if not isinstance(period, int):
            try:
                period = int(period)
            except (ValueError, TypeError):
                period = 10
        
        # Get price_type from config (default to LAST)
        price_type_str = indicator_config.get("price_type", "LAST")
        try:
            price_type = PriceType[price_type_str.upper()]
        except (KeyError, AttributeError):
            price_type = PriceType.LAST
        
        # Create indicator based on type
        indicator = None
        if indicator_name == "EMA" or indicator_name == "EXPONENTIALMOVINGAVERAGE":
            indicator = ExponentialMovingAverage(period, price_type=price_type)
        elif indicator_name == "SMA" or indicator_name == "SIMPLEMOVINGAVERAGE":
            indicator = SimpleMovingAverage(period, price_type=price_type)
        elif indicator_name == "KAMA":
            # KAMA uses AdaptiveMovingAverage with period_er, fast_period, slow_period
            fast_period = indicator_config.get("fast_period", 2)
            slow_period = indicator_config.get("slow_period", 30)
            if not isinstance(fast_period, int):
                try:
                    fast_period = int(fast_period)
                except (ValueError, TypeError):
                    fast_period = 2
            if not isinstance(slow_period, int):
                try:
                    slow_period = int(slow_period)
                except (ValueError, TypeError):
                    slow_period = 30
            indicator = AdaptiveMovingAverage(period, fast_period, slow_period, price_type=price_type)
        elif indicator_name == "RSI":
            indicator = RelativeStrengthIndex(period)
        elif indicator_name == "MACD":
            # MACD requires fast_period, slow_period, signal_period
            # Use MACDComplete by default to get all three values (MACD, signal, histogram)
            fast_period = indicator_config.get("fast_period", 12)
            slow_period = indicator_config.get("slow_period", 26)
            signal_period = indicator_config.get("signal_period", 9)
            if not isinstance(fast_period, int):
                try:
                    fast_period = int(fast_period)
                except (ValueError, TypeError):
                    fast_period = 12
            if not isinstance(slow_period, int):
                try:
                    slow_period = int(slow_period)
                except (ValueError, TypeError):
                    slow_period = 26
            if not isinstance(signal_period, int):
                try:
                    signal_period = int(signal_period)
                except (ValueError, TypeError):
                    signal_period = 9
            # Use MACDComplete which provides MACD line, signal line, and histogram
            indicator = MACDComplete(fast_period=fast_period, slow_period=slow_period, signal_period=signal_period, price_type=price_type)
        elif indicator_name == "ATR":
            indicator = AverageTrueRange(period)
        elif indicator_name == "BOLLINGER BANDS" or indicator_name == "BOLLINGER":
            # Bollinger Bands requires period and std_dev (k parameter)
            std_dev = indicator_config.get("std_dev", 2.0)
            if not isinstance(std_dev, (int, float)):
                try:
                    std_dev = float(std_dev)
                except (ValueError, TypeError):
                    std_dev = 2.0
            indicator = BollingerBands(period, std_dev)
        elif indicator_name == "FORCEINDEX" or indicator_name == "FORCE INDEX":
            source_path = indicator_config.get("sourcePath", "close")
            volume_path = indicator_config.get("volumePath", "volume")
            indicator = ForceIndex(source_path=source_path, volume_path=volume_path)
        elif indicator_name == "SPEEDMONITOR" or indicator_name == "SPEED MONITOR":
            ema_level1 = indicator_config.get("emaLevel1", 50)
            ema_level2 = indicator_config.get("emaLevel2", 100)
            ema_level3 = indicator_config.get("emaLevel3", 200)
            if not isinstance(ema_level1, int):
                try:
                    ema_level1 = int(ema_level1)
                except (ValueError, TypeError):
                    ema_level1 = 50
            if not isinstance(ema_level2, int):
                try:
                    ema_level2 = int(ema_level2)
                except (ValueError, TypeError):
                    ema_level2 = 100
            if not isinstance(ema_level3, int):
                try:
                    ema_level3 = int(ema_level3)
                except (ValueError, TypeError):
                    ema_level3 = 200
            indicator = SpeedMonitor(ema_level1=ema_level1, ema_level2=ema_level2, ema_level3=ema_level3, price_type=price_type)
        elif indicator_name == "UPTRICK: VOLATILITY ADJUSTED TRAIL" or indicator_name == "UPTRICK":
            ema_len = indicator_config.get("emaLen", 8)
            atr_len = indicator_config.get("atrLen", 14)
            base_mult = indicator_config.get("baseMult", 2.0)
            sens_exp = indicator_config.get("sensExp", 1.00)
            pers_len = indicator_config.get("persLen", 20)
            pers_gain = indicator_config.get("persGain", 0.30)
            mult_min = indicator_config.get("multMin", 1.0)
            mult_max = indicator_config.get("multMax", 4.0)
            confirm_n = indicator_config.get("confirmN", 1)
            # Convert to proper types
            if not isinstance(ema_len, int):
                ema_len = int(ema_len) if ema_len else 8
            if not isinstance(atr_len, int):
                atr_len = int(atr_len) if atr_len else 14
            if not isinstance(base_mult, (int, float)):
                base_mult = float(base_mult) if base_mult else 2.0
            if not isinstance(sens_exp, (int, float)):
                sens_exp = float(sens_exp) if sens_exp else 1.00
            if not isinstance(pers_len, int):
                pers_len = int(pers_len) if pers_len else 20
            if not isinstance(pers_gain, (int, float)):
                pers_gain = float(pers_gain) if pers_gain else 0.30
            if not isinstance(mult_min, (int, float)):
                mult_min = float(mult_min) if mult_min else 1.0
            if not isinstance(mult_max, (int, float)):
                mult_max = float(mult_max) if mult_max else 4.0
            if not isinstance(confirm_n, int):
                confirm_n = int(confirm_n) if confirm_n else 1
            indicator = UptrickVolatilityTrail(
                ema_len=ema_len, atr_len=atr_len, base_mult=base_mult,
                sens_exp=sens_exp, pers_len=pers_len, pers_gain=pers_gain,
                mult_min=mult_min, mult_max=mult_max, confirm_n=confirm_n,
                price_type=price_type
            )
        elif indicator_name == "ZIGZAG":
            depth = indicator_config.get("depth", 12)
            deviation = indicator_config.get("deviation", 5.0)
            backstep = indicator_config.get("backstep", 2)
            if not isinstance(depth, int):
                depth = int(depth) if depth else 12
            if not isinstance(deviation, (int, float)):
                deviation = float(deviation) if deviation else 5.0
            if not isinstance(backstep, int):
                backstep = int(backstep) if backstep else 2
            indicator = ZigZag(depth=depth, deviation=deviation, backstep=backstep)
        else:
            print(f"Warning: Unknown indicator type '{indicator_name}', skipping", file=sys.stderr)
            continue
        
        if indicator:
            # Use api_name as key if available, otherwise use indicator_name
            key = api_name if api_name else indicator_name.lower()
            indicators[key] = indicator
            print(f"Created {indicator_name}(period={period}, price_type={price_type_str}) as '{key}'", file=sys.stderr)
    
    return indicators


def parse_rules_to_strategy_logic(
    rules_config: Dict[str, Any],
    indicators: Dict[str, Any],
    enable_long: bool = True,
    enable_short: bool = False
) -> tuple[Optional[Callable], Optional[Callable]]:
    """
    Parse rules_config to create callable buy/sell logic functions.
    
    Rules structure:
    {
        "long": {
            "buy": {
                "type": "logic_group",
                "rules": [...],
                "value": "and"  # or "or"
            },
            "sell": {...}
        },
        "short": {...}
    }
    
    Condition structure:
    {
        "type": "condition",
        "op": "gte",  # gte, lte, gt, lt, eq, ne
        "subject": {"type": "indicator_value", "value": "fast_ema/default:5m"},
        "object": {"type": "indicator_value", "value": "slow_ema/default:5m"}
    }
    
    Args:
        rules_config: Rules configuration dictionary
        indicators: Dictionary of indicator instances keyed by api_name
        enable_long: Whether long positions are enabled
        enable_short: Whether short positions are enabled
    
    Returns:
        Tuple of (buy_logic, sell_logic) callable functions
    """
    
    def get_indicator_value(indicator_ref: str, indicators_dict: Dict[str, Any]) -> Optional[float]:
        """
        Get current value from an indicator reference.
        Format: "api_name/default:timeframe" or just "api_name"
        
        The api_name is a key from the strategy_indicators table - do not parse it.
        Just extract it from the reference format and use it as a lookup key.
        
        Args:
            indicator_ref: Indicator reference string (e.g., "slow_ema/default:5m")
            indicators_dict: Dictionary of indicator instances keyed by api_name
        
        Returns:
            Indicator value as float if initialized, None if not initialized or not found
        """
        # Extract api_name from reference format (e.g., "slow_ema" from "slow_ema/default:5m")
        # api_name is just a key - do not parse it further
        api_name = indicator_ref.split("/")[0].split(":")[0]
        
        if api_name in indicators_dict:
            indicator = indicators_dict[api_name]
            # Check if indicator is initialized and has a value
            if hasattr(indicator, 'initialized') and indicator.initialized:
                if hasattr(indicator, 'value'):
                    try:
                        val = float(indicator.value)
                        # Only log occasionally to avoid spam
                        return val
                    except (TypeError, ValueError, AttributeError):
                        print(f"[INDICATOR_VALUE] Warning: Could not get value from indicator '{api_name}' (value attribute)", file=sys.stderr)
                        return None
                elif hasattr(indicator, 'get_value'):
                    try:
                        val = float(indicator.get_value())
                        # Only log occasionally to avoid spam
                        return val
                    except (TypeError, ValueError, AttributeError):
                        print(f"[INDICATOR_VALUE] Warning: Could not get value from indicator '{api_name}' (get_value method)", file=sys.stderr)
                        return None
            else:
                # Log first few times when indicator is not initialized
                if not hasattr(get_indicator_value, '_not_init_warned'):
                    get_indicator_value._not_init_warned = {}
                count = get_indicator_value._not_init_warned.get(api_name, 0)
                if count < 3:
                    print(f"[INDICATOR_VALUE] Warning: Indicator '{api_name}' not initialized yet (ref: '{indicator_ref}')", file=sys.stderr)
                    get_indicator_value._not_init_warned[api_name] = count + 1
                # Return None when not initialized - this will cause evaluate_condition to return False
                return None
        else:
            # This is a real error - log it
            print(f"[INDICATOR_VALUE] Error: Indicator '{api_name}' not found in indicators dict. Reference: '{indicator_ref}', Available: {list(indicators_dict.keys())}", file=sys.stderr)
        return None
    
    def evaluate_condition(condition: Dict[str, Any], indicators_dict: Dict[str, Any]) -> bool:
        """Evaluate a single condition."""
        op = condition.get("op", "").lower()
        subject = condition.get("subject", {})
        object_val = condition.get("object", {})
        
        # Get values
        subject_val = None
        object_val_num = None
        
        if subject.get("type") == "indicator_value":
            subject_val = get_indicator_value(subject.get("value", ""), indicators_dict)
        elif subject.get("type") == "constant":
            try:
                subject_val = float(subject.get("value", 0))
            except (ValueError, TypeError):
                subject_val = 0.0
        
        if object_val.get("type") == "indicator_value":
            object_val_num = get_indicator_value(object_val.get("value", ""), indicators_dict)
        elif object_val.get("type") == "constant":
            try:
                object_val_num = float(object_val.get("value", 0))
            except (ValueError, TypeError):
                object_val_num = 0.0
        
        if subject_val is None or object_val_num is None:
            # Log when we can't get values (but not too often)
            return False
        
        # Evaluate operator
        result = False
        if op == "gte":
            result = subject_val >= object_val_num
        elif op == "lte":
            result = subject_val <= object_val_num
        elif op == "gt":
            result = subject_val > object_val_num
        elif op == "lt":
            result = subject_val < object_val_num
        elif op == "eq" or op == "==":
            result = abs(subject_val - object_val_num) < 1e-9  # Float comparison
        elif op == "ne" or op == "!=":
            result = abs(subject_val - object_val_num) >= 1e-9
        else:
            print(f"[CONDITION] Warning: Unknown operator '{op}', returning False", file=sys.stderr)
            return False
        
        # Log condition evaluation when result is True (but only first few times to avoid spam)
        if result:
            if not hasattr(evaluate_condition, '_true_log_count'):
                evaluate_condition._true_log_count = {}
            condition_key = f"{subject.get('value', '?')} {op} {object_val.get('value', '?')}"
            count = evaluate_condition._true_log_count.get(condition_key, 0)
            if count < 3:  # Only log first 3 times
                print(f"[CONDITION] TRUE: {condition_key} -> {subject_val} {op} {object_val_num}", file=sys.stderr)
                evaluate_condition._true_log_count[condition_key] = count + 1
        
        # For sell conditions (lt/lte), log periodically to debug why they're not triggering
        if op in ["lt", "lte"] and not result:
            if not hasattr(evaluate_condition, '_sell_false_count'):
                evaluate_condition._sell_false_count = 0
            evaluate_condition._sell_false_count += 1
            if evaluate_condition._sell_false_count % 5000 == 0:
                condition_key = f"{subject.get('value', '?')} {op} {object_val.get('value', '?')}"
                print(f"[CONDITION] FALSE (sell): {condition_key} -> {subject_val} {op} {object_val_num} (count: {evaluate_condition._sell_false_count})", file=sys.stderr)
        
        return result
    
    def evaluate_logic_group(logic_group: Dict[str, Any], indicators_dict: Dict[str, Any]) -> bool:
        """Evaluate a logic_group (and/or combination of rules)."""
        if logic_group.get("type") != "logic_group":
            return False
        
        rules = logic_group.get("rules", [])
        logic_op = logic_group.get("value", "and").lower()
        
        if not rules:
            return False
        
        results = []
        for rule in rules:
            if rule.get("type") == "condition":
                results.append(evaluate_condition(rule, indicators_dict))
            elif rule.get("type") == "logic_group":
                results.append(evaluate_logic_group(rule, indicators_dict))
        
        if not results:
            return False
        
        # Apply logic operator
        if logic_op == "and":
            return all(results)
        elif logic_op == "or":
            return any(results)
        else:
            print(f"Warning: Unknown logic operator '{logic_op}', defaulting to 'and'", file=sys.stderr)
            return all(results)
    
    def create_buy_logic() -> Callable:
        """Create buy logic function combining long and short buy conditions."""
        long_buy = None
        short_buy = None
        
        if enable_long and "long" in rules_config:
            long_config = rules_config["long"]
            if "buy" in long_config:
                long_buy = long_config["buy"]
        
        if enable_short and "short" in rules_config:
            short_config = rules_config["short"]
            if "buy" in short_config:
                short_buy = short_config["buy"]
        
        # Create closure that captures indicators dict
        def buy_logic() -> bool:
            """Buy logic: long buy OR short buy"""
            # Use the captured indicators dict
            result = False
            if long_buy:
                long_result = evaluate_logic_group(long_buy, indicators)
                # Only log occasionally to avoid spam
                if long_result and not hasattr(buy_logic, '_true_logged'):
                    buy_logic._true_logged = True
                result = result or long_result
            if short_buy:
                short_result = evaluate_logic_group(short_buy, indicators)
                result = result or short_result
            return result
        
        return buy_logic
    
    def create_sell_logic() -> Callable:
        """Create sell logic function combining long and short sell conditions."""
        long_sell = None
        short_sell = None
        
        if enable_long and "long" in rules_config:
            long_config = rules_config["long"]
            if "sell" in long_config:
                long_sell = long_config["sell"]
        
        if enable_short and "short" in rules_config:
            short_config = rules_config["short"]
            if "sell" in short_config:
                short_sell = short_config["sell"]
        
        # Create closure that captures indicators dict
        def sell_logic() -> bool:
            """Sell logic: long sell OR short sell"""
            # Use the captured indicators dict
            result = False
            if long_sell:
                try:
                    long_result = evaluate_logic_group(long_sell, indicators)
                    result = result or long_result
                except Exception as e:
                    import traceback
                    traceback.print_exc()
            else:
                if not hasattr(sell_logic, '_no_config_warned'):
                    sell_logic._no_config_warned = True
            if short_sell:
                try:
                    short_result = evaluate_logic_group(short_sell, indicators)
                    result = result or short_result
                except Exception as e:
                    import traceback
                    traceback.print_exc()
            return result
        
        return sell_logic
    
    # Create buy and sell logic functions
    buy_logic = create_buy_logic()
    sell_logic = create_sell_logic()
    
    print(f"Created buy/sell logic functions from rules_config", file=sys.stderr)
    
    return buy_logic, sell_logic

