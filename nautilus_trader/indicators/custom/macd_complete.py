# -------------------------------------------------------------------------------------------------
# MACD Complete Indicator
# Moving Average Convergence/Divergence with signal line and histogram
# -------------------------------------------------------------------------------------------------

from nautilus_trader.indicators.macd import MovingAverageConvergenceDivergence
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.data import Bar, QuoteTick, TradeTick
from nautilus_trader.core.rust.model import PriceType


class MACDComplete:
    """
    Complete MACD indicator with MACD line, signal line, and histogram.
    
    This indicator wraps Nautilus's MACD and adds signal line calculation.
    
    Parameters
    ----------
    fast_period : int
        Fast EMA period (default: 12)
    slow_period : int
        Slow EMA period (default: 26)
    signal_period : int
        Signal line EMA period (default: 9)
    price_type : PriceType
        Price type for calculations
    """
    
    def __init__(
        self,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        price_type: PriceType = PriceType.LAST,
    ):
        self.name = "MACDComplete"
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.price_type = price_type
        
        # Create internal MACD indicator (provides MACD line)
        self._macd = MovingAverageConvergenceDivergence(fast_period, slow_period, price_type=price_type)
        
        # Create signal line EMA (EMA of MACD line)
        self._signal_ema = ExponentialMovingAverage(signal_period, price_type=price_type)
        
        # Output values
        self.macd = 0.0  # MACD line (fast_ema - slow_ema)
        self.signal = 0.0  # Signal line (EMA of MACD line)
        self.hist = 0.0  # Histogram (MACD - Signal)
        
        self.initialized = False
        self.has_inputs = False
    
    def handle_bar(self, bar: Bar) -> None:
        """Update the indicator with the given bar."""
        if bar.is_single_price():
            return
        
        # Update MACD indicator
        self._macd.handle_bar(bar)
        
        # Once MACD is initialized, update signal line
        if self._macd.initialized:
            # Update signal EMA with MACD line value
            self._signal_ema.update_raw(float(self._macd.value))
            
            # Calculate all three values
            self.macd = float(self._macd.value)
            
            if self._signal_ema.initialized:
                self.signal = float(self._signal_ema.value)
                self.hist = self.macd - self.signal
                
                if not self.initialized:
                    self.has_inputs = True
                    self.initialized = True
    
    def handle_quote_tick(self, tick: QuoteTick) -> None:
        """Update the indicator with the given quote tick."""
        self._macd.handle_quote_tick(tick)
        
        if self._macd.initialized:
            self._signal_ema.update_raw(float(self._macd.value))
            
            self.macd = float(self._macd.value)
            if self._signal_ema.initialized:
                self.signal = float(self._signal_ema.value)
                self.hist = self.macd - self.signal
                
                if not self.initialized:
                    self.has_inputs = True
                    self.initialized = True
    
    def handle_trade_tick(self, tick: TradeTick) -> None:
        """Update the indicator with the given trade tick."""
        self._macd.handle_trade_tick(tick)
        
        if self._macd.initialized:
            self._signal_ema.update_raw(float(self._macd.value))
            
            self.macd = float(self._macd.value)
            if self._signal_ema.initialized:
                self.signal = float(self._signal_ema.value)
                self.hist = self.macd - self.signal
                
                if not self.initialized:
                    self.has_inputs = True
                    self.initialized = True
    
    def reset(self) -> None:
        """Reset the indicator."""
        self._macd.reset()
        self._signal_ema.reset()
        self.macd = 0.0
        self.signal = 0.0
        self.hist = 0.0
        self.has_inputs = False
        self.initialized = False
