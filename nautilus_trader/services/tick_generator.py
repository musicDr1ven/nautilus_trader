#!/usr/bin/env python3
"""
Tick Generator
Generates synthetic trade ticks from bars for backtesting.
"""

from typing import List
from nautilus_trader.model.data import Bar, TradeTick
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.instruments import Instrument


def generate_synthetic_ticks_from_bars(bars: List[Bar], instrument: Instrument) -> List[TradeTick]:
    """
    Generate synthetic trade ticks from bars.
    Creates 4 ticks per bar using the close price.
    
    Args:
        bars: List of bars to convert to ticks
        instrument: The instrument for the ticks
        
    Returns:
        List of TradeTick objects
    """
    ticks = []
    
    for bar in bars:
        # Create 4 ticks per bar
        volume_per_tick = max(1, int(float(bar.volume) / 4)) if float(bar.volume) > 0 else 1
        
        for i in range(4):
            # Use the close price for all ticks, but round it to match instrument's price_precision
            # This ensures the order book doesn't throw AssertionError for precision mismatches
            price_value = float(bar.close.as_double())
            price = instrument.make_price(price_value)
            
            # Calculate timestamps as integers
            ts_event = int(bar.ts_event) + (i * 250_000_000)
            ts_init = int(bar.ts_init) + (i * 250_000_000)
            
            # Alternate between BUYER and SELLER for synthetic ticks
            aggressor_side = AggressorSide.BUYER if i % 2 == 0 else AggressorSide.SELLER
            
            # Use instrument.make_qty to ensure quantity matches instrument's size_precision
            quantity = instrument.make_qty(volume_per_tick)
            
            tick = TradeTick(
                instrument_id=instrument.id,
                price=price,
                size=quantity,
                aggressor_side=aggressor_side,
                trade_id=TradeId(f"{bar.ts_event}_{i}"),
                ts_event=ts_event,
                ts_init=ts_init,
            )
            ticks.append(tick)
    
    return ticks

