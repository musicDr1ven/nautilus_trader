# -------------------------------------------------------------------------------------------------
# Force Index Indicator
# Measures the strength of price movement with volume
# -------------------------------------------------------------------------------------------------

from typing import Optional
from nautilus_trader.model.data import Bar, QuoteTick, TradeTick


class ForceIndex:
    """
    Force Index indicator that measures the strength of price movement with volume.
    
    Formula: (close - previous_close) * volume
    
    Parameters
    ----------
    source_path : str
        Source path for price (default: 'close')
    volume_path : str
        Volume path (default: 'volume')
    """
    
    def __init__(self, source_path: str = 'close', volume_path: str = 'volume'):
        self.name = "ForceIndex"
        self.source_path = source_path
        self.volume_path = volume_path
        self._previous_close: Optional[float] = None
        self.value = 0.0
        self.initialized = False
        self.has_inputs = False
    
    def handle_bar(self, bar: Bar) -> None:
        """Update the indicator with the given bar."""
        if bar.is_single_price():
            return
        
        close = float(bar.close.as_double())
        volume = float(bar.volume)
        
        if self._previous_close is not None:
            self.value = (close - self._previous_close) * volume
            if not self.initialized:
                self.has_inputs = True
                self.initialized = True
        
        self._previous_close = close
    
    def handle_quote_tick(self, tick: QuoteTick) -> None:
        """Update the indicator with the given quote tick."""
        # ForceIndex typically uses bars, but we can use last price
        price = float(tick.extract_price(self.price_type if hasattr(self, 'price_type') else None).as_double())
        # Note: QuoteTick doesn't have volume, so we can't calculate ForceIndex from it
        # This is a limitation - ForceIndex requires volume
        pass
    
    def handle_trade_tick(self, tick: TradeTick) -> None:
        """Update the indicator with the given trade tick."""
        # TradeTick has price and volume, but we need previous close
        # For now, ForceIndex is best used with bars
        pass
    
    def reset(self) -> None:
        """Reset the indicator."""
        self._previous_close = None
        self.value = 0.0
        self.has_inputs = False
        self.initialized = False
