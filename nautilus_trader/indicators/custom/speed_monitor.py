# -------------------------------------------------------------------------------------------------
# Speed Monitor Indicator
# Shows percentage distance from EMA50, EMA100, and EMA200
# -------------------------------------------------------------------------------------------------

from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.data import Bar, QuoteTick, TradeTick
from nautilus_trader.core.rust.model import PriceType


class SpeedMonitor:
    """
    Speed Monitor indicator showing percentage distance from multiple EMA levels.
    
    Parameters
    ----------
    ema_level1 : int
        First EMA level (default: 50)
    ema_level2 : int
        Second EMA level (default: 100)
    ema_level3 : int
        Third EMA level (default: 200)
    price_type : PriceType
        Price type for EMA calculations
    """
    
    def __init__(
        self,
        ema_level1: int = 50,
        ema_level2: int = 100,
        ema_level3: int = 200,
        price_type: PriceType = PriceType.LAST,
    ):
        self.name = "SpeedMonitor"
        self.ema_level1 = ema_level1
        self.ema_level2 = ema_level2
        self.ema_level3 = ema_level3
        self.price_type = price_type
        
        # Create internal EMA indicators
        self._ema1 = ExponentialMovingAverage(ema_level1, price_type=price_type)
        self._ema2 = ExponentialMovingAverage(ema_level2, price_type=price_type)
        self._ema3 = ExponentialMovingAverage(ema_level3, price_type=price_type)
        
        self.ema50 = 0.0
        self.ema100 = 0.0
        self.ema200 = 0.0
        self.initialized = False
        self.has_inputs = False
    
    def handle_bar(self, bar: Bar) -> None:
        """Update the indicator with the given bar."""
        if bar.is_single_price():
            return
        
        # Update internal EMAs
        self._ema1.handle_bar(bar)
        self._ema2.handle_bar(bar)
        self._ema3.handle_bar(bar)
        
        if self._ema1.initialized and self._ema2.initialized and self._ema3.initialized:
            close = float(bar.close.as_double())
            
            # Calculate percentage distance from each EMA
            ema1_val = float(self._ema1.value)
            ema2_val = float(self._ema2.value)
            ema3_val = float(self._ema3.value)
            
            self.ema50 = ((close - ema1_val) / ema1_val) * 100 if ema1_val != 0 else 0.0
            self.ema100 = ((close - ema2_val) / ema2_val) * 100 if ema2_val != 0 else 0.0
            self.ema200 = ((close - ema3_val) / ema3_val) * 100 if ema3_val != 0 else 0.0
            
            if not self.initialized:
                self.has_inputs = True
                self.initialized = True
    
    def handle_quote_tick(self, tick: QuoteTick) -> None:
        """Update the indicator with the given quote tick."""
        self._ema1.handle_quote_tick(tick)
        self._ema2.handle_quote_tick(tick)
        self._ema3.handle_quote_tick(tick)
    
    def handle_trade_tick(self, tick: TradeTick) -> None:
        """Update the indicator with the given trade tick."""
        self._ema1.handle_trade_tick(tick)
        self._ema2.handle_trade_tick(tick)
        self._ema3.handle_trade_tick(tick)
    
    def reset(self) -> None:
        """Reset the indicator."""
        self._ema1.reset()
        self._ema2.reset()
        self._ema3.reset()
        self.ema50 = 0.0
        self.ema100 = 0.0
        self.ema200 = 0.0
        self.has_inputs = False
        self.initialized = False
