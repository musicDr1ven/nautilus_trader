# -------------------------------------------------------------------------------------------------
# ZigZag Indicator
# Identifies pivot points and trend reversals
# -------------------------------------------------------------------------------------------------

from typing import Optional, List, Tuple
from nautilus_trader.model.data import Bar, QuoteTick, TradeTick


class ZigZag:
    """
    ZigZag indicator that identifies pivot points and trend reversals.
    
    Parameters
    ----------
    depth : int
        Depth parameter (default: 12)
    deviation : float
        Deviation percentage (default: 5.0)
    backstep : int
        Backstep parameter (default: 2)
    """
    
    def __init__(
        self,
        depth: int = 12,
        deviation: float = 5.0,
        backstep: int = 2,
    ):
        self.name = "ZigZag"
        self.depth = depth
        self.deviation = deviation
        self.backstep = backstep
        
        self.value = 0.0  # Current ZigZag value
        self._pivot_points: List[Tuple[float, float]] = []  # (price, timestamp)
        self._price_history: List[Tuple[float, float]] = []  # (price, timestamp)
        self._last_pivot_idx = -1
        self._last_pivot_type = 0  # 1 = high, -1 = low
        self.initialized = False
    
    def handle_bar(self, bar: Bar) -> None:
        """Update the indicator with the given bar."""
        if bar.is_single_price():
            return
        
        high = float(bar.high.as_double())
        low = float(bar.low.as_double())
        close = float(bar.close.as_double())
        timestamp = float(bar.ts_event)
        
        # Use close as the representative price
        self._price_history.append((close, timestamp))
        
        # Keep only recent history (depth * 2 to have enough data)
        if len(self._price_history) > self.depth * 2:
            self._price_history.pop(0)
        
        if len(self._price_history) < self.depth:
            return
        
        # Find pivot points
        pivot_price, pivot_type = self._find_pivot()
        
        if pivot_price is not None:
            # Check if this pivot is significant enough (deviation check)
            if self._is_significant_pivot(pivot_price, pivot_type):
                # Add pivot point
                self._pivot_points.append((pivot_price, timestamp))
                
                # Remove old pivots beyond backstep
                if len(self._pivot_points) > self.backstep + 1:
                    self._pivot_points.pop(0)
                
                self._last_pivot_idx = len(self._price_history) - 1
                self._last_pivot_type = pivot_type
                self.value = pivot_price
                
                if not self.initialized:
                    self.has_inputs = True
                    self.initialized = True
            else:
                # Use last known pivot or current price
                if self._pivot_points:
                    self.value = self._pivot_points[-1][0]
                else:
                    self.value = close
        else:
            # Use last known pivot or current price
            if self._pivot_points:
                self.value = self._pivot_points[-1][0]
            else:
                self.value = close
    
    def _find_pivot(self) -> Tuple[Optional[float], int]:
        """Find the most recent pivot point in the price history."""
        if len(self._price_history) < self.depth:
            return None, 0
        
        # Look for local high or low in the middle of the window
        mid_idx = len(self._price_history) - self.depth // 2 - 1
        if mid_idx < 0 or mid_idx >= len(self._price_history):
            return None, 0
        
        mid_price = self._price_history[mid_idx][0]
        
        # Check if it's a local high
        is_high = True
        is_low = True
        
        for i in range(max(0, mid_idx - self.depth // 2), min(len(self._price_history), mid_idx + self.depth // 2 + 1)):
            if i != mid_idx:
                price = self._price_history[i][0]
                if price > mid_price:
                    is_high = False
                if price < mid_price:
                    is_low = False
        
        if is_high:
            return mid_price, 1
        elif is_low:
            return mid_price, -1
        else:
            return None, 0
    
    def _is_significant_pivot(self, pivot_price: float, pivot_type: int) -> bool:
        """Check if the pivot is significant enough based on deviation."""
        if not self._pivot_points:
            return True  # First pivot is always significant
        
        last_pivot_price = self._pivot_points[-1][0]
        price_change = abs(pivot_price - last_pivot_price)
        price_change_pct = (price_change / last_pivot_price) * 100 if last_pivot_price > 0 else 0
        
        return price_change_pct >= self.deviation
    
    def handle_quote_tick(self, tick: QuoteTick) -> None:
        """Update the indicator with the given quote tick."""
        # ZigZag typically uses bars, but we can use last price
        pass
    
    def handle_trade_tick(self, tick: TradeTick) -> None:
        """Update the indicator with the given trade tick."""
        # ZigZag typically uses bars
        pass
    
    def reset(self) -> None:
        """Reset the indicator."""
        self.value = 0.0
        self._pivot_points.clear()
        self._price_history.clear()
        self._last_pivot_idx = -1
        self._last_pivot_type = 0
        self.has_inputs = False
        self.initialized = False
