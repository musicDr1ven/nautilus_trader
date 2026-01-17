# -------------------------------------------------------------------------------------------------
# Uptrick: Volatility Adjusted Trail Indicator
# Volatility-adjusted trailing stop with dynamic multipliers
# -------------------------------------------------------------------------------------------------

from typing import Optional
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.indicators.atr import AverageTrueRange
from nautilus_trader.model.data import Bar, QuoteTick, TradeTick
from nautilus_trader.core.rust.model import PriceType


class UptrickVolatilityTrail:
    """
    Uptrick Volatility Adjusted Trail indicator.
    
    Parameters
    ----------
    ema_len : int
        Basis EMA length (default: 8)
    atr_len : int
        ATR length (default: 14)
    base_mult : float
        Base ATR multiplier (default: 2.0)
    sens_exp : float
        Volatility expansion sensitivity (default: 1.00)
    pers_len : int
        Trend persistence window (default: 20)
    pers_gain : float
        Persistence impact (0..1) (default: 0.30)
    mult_min : float
        Min effective multiplier (default: 1.0)
    mult_max : float
        Max effective multiplier (default: 4.0)
    confirm_n : int
        Bars above/below to confirm flip (default: 1)
    price_type : PriceType
        Price type for calculations
    """
    
    def __init__(
        self,
        ema_len: int = 8,
        atr_len: int = 14,
        base_mult: float = 2.0,
        sens_exp: float = 1.00,
        pers_len: int = 20,
        pers_gain: float = 0.30,
        mult_min: float = 1.0,
        mult_max: float = 4.0,
        confirm_n: int = 1,
        price_type: PriceType = PriceType.LAST,
    ):
        self.name = "UptrickVolatilityTrail"
        self.ema_len = ema_len
        self.atr_len = atr_len
        self.base_mult = base_mult
        self.sens_exp = sens_exp
        self.pers_len = pers_len
        self.pers_gain = pers_gain
        self.mult_min = mult_min
        self.mult_max = mult_max
        self.confirm_n = confirm_n
        self.price_type = price_type
        
        # Create internal indicators
        self._ema = ExponentialMovingAverage(ema_len, price_type=price_type)
        self._atr = AverageTrueRange(atr_len)
        
        # State tracking
        self.upper = 0.0
        self.lower = 0.0
        self.state = 0  # 0 = neutral, 1 = bullish, -1 = bearish
        
        self._price_history: list[float] = []
        self._atr_history: list[float] = []
        self._trend_state = 0  # 1 = up, -1 = down
        self._bars_above = 0
        self._bars_below = 0
        
        self.initialized = False
    
    def handle_bar(self, bar: Bar) -> None:
        """Update the indicator with the given bar."""
        if bar.is_single_price():
            return
        
        # Update internal indicators
        self._ema.handle_bar(bar)
        self._atr.handle_bar(bar)
        
        if self._ema.initialized and self._atr.initialized:
            close = float(bar.close.as_double())
            ema_val = float(self._ema.value)
            atr_val = float(self._atr.value)
            
            # Track price and ATR history
            self._price_history.append(close)
            self._atr_history.append(atr_val)
            if len(self._price_history) > self.pers_len:
                self._price_history.pop(0)
                self._atr_history.pop(0)
            
            # Calculate volatility expansion
            if len(self._atr_history) >= 2:
                current_atr = self._atr_history[-1]
                avg_atr = sum(self._atr_history[:-1]) / len(self._atr_history[:-1])
                volatility_ratio = current_atr / avg_atr if avg_atr > 0 else 1.0
                volatility_expansion = (volatility_ratio - 1.0) * self.sens_exp
            else:
                volatility_expansion = 0.0
            
            # Calculate trend persistence
            if len(self._price_history) >= self.pers_len:
                recent_prices = self._price_history[-self.pers_len:]
                price_trend = 1 if recent_prices[-1] > recent_prices[0] else -1
                persistence_factor = self.pers_gain if price_trend == self._trend_state else 0.0
            else:
                persistence_factor = 0.0
            
            # Calculate effective multiplier
            effective_mult = self.base_mult + volatility_expansion + persistence_factor
            effective_mult = max(self.mult_min, min(self.mult_max, effective_mult))
            
            # Calculate trailing stops
            upper_stop = ema_val + (atr_val * effective_mult)
            lower_stop = ema_val - (atr_val * effective_mult)
            
            # Update state based on price position
            if close > upper_stop:
                self._bars_above += 1
                self._bars_below = 0
            elif close < lower_stop:
                self._bars_below += 1
                self._bars_above = 0
            else:
                self._bars_above = 0
                self._bars_below = 0
            
            # Confirm trend flip
            if self._bars_above >= self.confirm_n:
                self._trend_state = 1
                self.state = 1
            elif self._bars_below >= self.confirm_n:
                self._trend_state = -1
                self.state = -1
            else:
                self.state = 0
            
            self.upper = upper_stop
            self.lower = lower_stop
            
            if not self.initialized:
                self.has_inputs = True
                self.initialized = True
    
    def handle_quote_tick(self, tick: QuoteTick) -> None:
        """Update the indicator with the given quote tick."""
        self._ema.handle_quote_tick(tick)
        self._atr.handle_quote_tick(tick)
    
    def handle_trade_tick(self, tick: TradeTick) -> None:
        """Update the indicator with the given trade tick."""
        self._ema.handle_trade_tick(tick)
        self._atr.handle_trade_tick(tick)
    
    def reset(self) -> None:
        """Reset the indicator."""
        self._ema.reset()
        self._atr.reset()
        self.upper = 0.0
        self.lower = 0.0
        self.state = 0
        self._price_history.clear()
        self._atr_history.clear()
        self._trend_state = 0
        self._bars_above = 0
        self._bars_below = 0
        self.has_inputs = False
        self.initialized = False
