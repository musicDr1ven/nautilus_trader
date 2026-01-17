# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from nautilus_trader.accounting.accounts.base import Account
from nautilus_trader.cache.config import CacheConfig
from nautilus_trader.cache.facade import CacheDatabaseFacade
from nautilus_trader.cache.transformers import transform_account_from_pyo3
from nautilus_trader.cache.transformers import transform_account_to_pyo3
from nautilus_trader.cache.transformers import transform_bar_to_pyo3
from nautilus_trader.cache.transformers import transform_currency_from_pyo3
from nautilus_trader.cache.transformers import transform_currency_to_pyo3
from nautilus_trader.cache.transformers import transform_custom_data_from_pyo3
from nautilus_trader.cache.transformers import transform_custom_data_to_pyo3
from nautilus_trader.cache.transformers import transform_data_type_to_pyo3
from nautilus_trader.cache.transformers import transform_instrument_from_pyo3
from nautilus_trader.cache.transformers import transform_instrument_to_pyo3
from nautilus_trader.cache.transformers import transform_order_event_to_pyo3
from nautilus_trader.cache.transformers import transform_order_from_pyo3
from nautilus_trader.cache.transformers import transform_order_to_pyo3
from nautilus_trader.cache.transformers import transform_order_to_snapshot_pyo3
from nautilus_trader.cache.transformers import transform_position_to_snapshot_pyo3
from nautilus_trader.cache.transformers import transform_quote_tick_to_pyo3
from nautilus_trader.cache.transformers import transform_signal_from_pyo3
from nautilus_trader.cache.transformers import transform_signal_to_pyo3
from nautilus_trader.cache.transformers import transform_trade_tick_from_pyo3
from nautilus_trader.cache.transformers import transform_trade_tick_to_pyo3
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.data import Data
from nautilus_trader.core.nautilus_pyo3 import PostgresCacheDatabase

# Try to import hybrid cache adapter if available
try:
    from nautilus_trader.core.nautilus_pyo3.infrastructure import GreptimePostgresHybridCacheAdapter
    HAS_HYBRID_CACHE = True
except ImportError:
    HAS_HYBRID_CACHE = False
    GreptimePostgresHybridCacheAdapter = None

from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.orders import Order
from nautilus_trader.model.position import Position
import sys


class CachePostgresAdapter(CacheDatabaseFacade):

    def __init__(
        self,
        config: CacheConfig | None = None,
    ) -> None:
        if config:
            config = CacheConfig()
        super().__init__(config)
        self._backing: PostgresCacheDatabase = PostgresCacheDatabase.connect()

    def close(self):
        """Close the database connection."""
        self._backing.close()

    def dispose(self):
        self._backing.close()

    def flush(self):
        self._backing.flush_db()

    def load(self):
        data = self._backing.load()
        return {key: bytes(value) for key, value in data.items()}

    def load_currencies(self) -> dict[str, Currency]:
        currencies = self._backing.load_currencies()
        return {currency.code: transform_currency_from_pyo3(currency) for currency in currencies}

    def load_currency(self, code: str) -> Currency | None:
        currency_pyo3 = self._backing.load_currency(code)
        if currency_pyo3:
            return transform_currency_from_pyo3(currency_pyo3)
        return None

    def load_instrument(self, instrument_id: InstrumentId) -> Instrument:
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        instrument_pyo3 = self._backing.load_instrument(instrument_id_pyo3)
        return transform_instrument_from_pyo3(instrument_pyo3)

    def load_order(self, client_order_id: ClientOrderId):
        order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        order_pyo3 = self._backing.load_order(order_id_pyo3)
        if order_pyo3:
            return transform_order_from_pyo3(order_pyo3)
        return None

    def load_orders(self):
        orders = self._backing.load_orders()
        return [transform_order_from_pyo3(order) for order in orders]

    def load_account(self, account_id: AccountId):
        account_id_pyo3 = nautilus_pyo3.AccountId.from_str(str(account_id))
        account_pyo3 = self._backing.load_account(account_id_pyo3)
        if account_pyo3:
            return transform_account_from_pyo3(account_pyo3)
        return None

    def load_signals(self, data_cls: type, name: str):
        signals_pyo3 = self._backing.load_signals(name)
        return [transform_signal_from_pyo3(data_cls, s) for s in signals_pyo3]

    def load_custom_data(self, data_type: DataType):
        data_type_pyo3 = transform_data_type_to_pyo3(data_type)
        data_pyo3 = self._backing.load_custom_data(data_type_pyo3)
        return [transform_custom_data_from_pyo3(d) for d in data_pyo3]

    def load_order_snapshot(
        self,
        client_order_id: ClientOrderId,
    ) -> nautilus_pyo3.OrderSnapshot | None:
        client_order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        snapshot_pyo3 = self._backing.load_order_snapshot(client_order_id_pyo3)
        return snapshot_pyo3

    def load_position_snapshot(
        self,
        position_id: PositionId,
    ) -> nautilus_pyo3.PositionSnapshot | None:
        position_id_pyo3 = nautilus_pyo3.PositionId.from_str(str(position_id))
        snapshot_pyo3 = self._backing.load_position_snapshot(position_id_pyo3)
        return snapshot_pyo3

    def load_quotes(self, instrument_id: InstrumentId) -> list[QuoteTick]:
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        quotes = self._backing.load_quotes(instrument_id_pyo3)
        return [QuoteTick.from_pyo3(quote_pyo3) for quote_pyo3 in quotes]

    def load_trades(self, instrument_id: InstrumentId) -> list[TradeTick]:
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        trades = self._backing.load_trades(instrument_id_pyo3)
        return [transform_trade_tick_from_pyo3(trade) for trade in trades]

    def load_bars(self, instrument_id: InstrumentId):
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        bars = self._backing.load_bars(instrument_id_pyo3)
        return [Bar.from_pyo3(bar_pyo3) for bar_pyo3 in bars]

    def add(self, key: str, value: bytes):
        self._backing.add(key, value)

    def add_currency(self, currency: Currency):
        currency_pyo3 = transform_currency_to_pyo3(currency)
        self._backing.add_currency(currency_pyo3)

    def add_instrument(self, instrument: Instrument):
        instrument_pyo3 = transform_instrument_to_pyo3(instrument)
        self._backing.add_instrument(instrument_pyo3)

    def add_order(self, order: Order, position_id: PositionId | None = None, client_id: ClientId | None = None):
        order_pyo3 = transform_order_to_pyo3(order)
        # Pass client_id to backing store (required by PyO3 binding)
        # position_id is not used by the backing store's add_order method
        self._backing.add_order(order_pyo3, client_id)

    def add_order_snapshot(self, order: Order) -> None:
        snapshot_pyo3 = transform_order_to_snapshot_pyo3(order)
        assert snapshot_pyo3
        self._backing.add_order_snapshot(snapshot_pyo3)

    def add_position_snapshot(
        self,
        position: Position,
        unrealized_pnl: Money | None = None,
    ) -> None:
        snapshot_pyo3 = transform_position_to_snapshot_pyo3(position, unrealized_pnl)
        assert snapshot_pyo3
        self._backing.add_position_snapshot(snapshot_pyo3)

    def add_account(self, account: Account):
        account_pyo3 = transform_account_to_pyo3(account)
        self._backing.add_account(account_pyo3)

    def add_position(self, position: Position) -> None:
        """Add a position to the cache database."""
        # Position is a Cython class, but PyO3 expects a PyO3 Position object
        # Convert Cython Position to PyO3 Position using to_dict() and from_dict()
        position_dict = position.to_dict()
        
        
        # Clean None values - Rust expects u64 for timestamp fields, not None
        # Fields that should be u64: ts_closed, duration_ns (convert None to 0)
        # Position.to_dict() converts 0 timestamps to None, but Rust expects u64
        # Optional string/identifier fields can remain None
        cleaned_dict = {}
        # Fields that are u64 and should be 0 if None
        u64_fields = ("ts_closed", "duration_ns", "ts_opened", "ts_last", "ts_init")
        # Optional fields that can be None (strings/identifiers)
        optional_fields = ("closing_order_id", "realized_pnl", "base_currency")
        
        for key, value in position_dict.items():
            if key == "commissions":
                # Rust expects commissions as HashMap<Currency, Money> (dict), but to_dict() returns a list
                # Convert list of Money strings to dict: {currency_code: money_string}
                if isinstance(value, list):
                    commissions_dict = {}
                    for money_str in value:
                        # Money strings are in format "AMOUNT CURRENCY" (e.g., "0.01 USD")
                        # Rust expects HashMap<Currency, Money> where:
                        # - Key: Currency code (string like "USD")
                        # - Value: Full Money string (e.g., "0.01 USD") because Money::from_str() expects "<amount> <currency>"
                        # Split from the right to handle amounts with spaces (e.g., "1 000.00 USD")
                        parts = money_str.rsplit(" ", 1)
                        if len(parts) == 2:
                            amount, currency_code = parts
                            # Store the full Money string as the value, not just the amount
                            commissions_dict[currency_code] = money_str
                    cleaned_dict[key] = commissions_dict
                else:
                    cleaned_dict[key] = value
            elif value is None:
                # Timestamp and duration fields should be 0, not None
                if key in u64_fields:
                    cleaned_dict[key] = 0
                # Optional string/identifier fields can remain None
                elif key in optional_fields:
                    cleaned_dict[key] = None
                # For other None values, try to infer type from field name
                elif key.endswith("_ns") or key.startswith("ts_"):
                    # Looks like a timestamp field
                    cleaned_dict[key] = 0
                else:
                    # Keep as None for now, but log it
                    cleaned_dict[key] = None
            else:
                cleaned_dict[key] = value
        
        # Add missing fields that Rust Position struct requires but Python to_dict() doesn't include
        # These fields are required by Rust's serde deserialization
        if "events" not in cleaned_dict:
            # events is a Vec<OrderFilled> - convert Python events to list of dicts
            # to_dict is a static method, so call it as OrderFilled.to_dict(event)
            from nautilus_trader.model.events.order import OrderFilled
            events_list = []
            for event in position.events:
                events_list.append(OrderFilled.to_dict(event))
            cleaned_dict["events"] = events_list
        
        if "trade_ids" not in cleaned_dict:
            # trade_ids is a Vec<TradeId> - convert to list of strings
            cleaned_dict["trade_ids"] = [str(tid) for tid in position.trade_ids]
        
        if "venue_order_ids" not in cleaned_dict:
            # venue_order_ids is a Vec<VenueOrderId> - convert to list of strings
            # venue_order_ids is a @property, not a method, so access without parentheses
            cleaned_dict["venue_order_ids"] = [str(vo_id) for vo_id in position.venue_order_ids]
        
        if "buy_qty" not in cleaned_dict:
            # buy_qty is a Quantity - convert to string
            # _buy_qty is a cdef attribute not accessible from Python, so calculate from events
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            buy_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.BUY:
                    buy_qty_total = buy_qty_total + event.last_qty
            cleaned_dict["buy_qty"] = str(buy_qty_total)
        
        if "sell_qty" not in cleaned_dict:
            # sell_qty is a Quantity - convert to string
            # _sell_qty is a cdef attribute not accessible from Python, so calculate from events
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            sell_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.SELL:
                    sell_qty_total = sell_qty_total + event.last_qty
            cleaned_dict["sell_qty"] = str(sell_qty_total)
        
        # Add other missing fields that Rust requires
        if "price_precision" not in cleaned_dict:
            cleaned_dict["price_precision"] = position.price_precision
        
        if "size_precision" not in cleaned_dict:
            cleaned_dict["size_precision"] = position.size_precision
        
        if "multiplier" not in cleaned_dict:
            cleaned_dict["multiplier"] = str(position.multiplier)
        
        if "is_inverse" not in cleaned_dict:
            cleaned_dict["is_inverse"] = position.is_inverse
        
        
        # Rust Position struct expects "id" field, but Python to_dict() uses "position_id"
        # Add "id" field mapped from "position_id" if it exists
        if "id" not in cleaned_dict and "position_id" in cleaned_dict:
            cleaned_dict["id"] = cleaned_dict["position_id"]
        
        pyo3_position = nautilus_pyo3.Position.from_dict(cleaned_dict)
        self._backing.add_position(pyo3_position)
    
    def update_position(self, position: Position) -> None:
        """Update a position in the cache database."""
        # Reuse the same conversion logic as add_position
        position_dict = position.to_dict()
        
        # Clean None values - Rust expects u64 for timestamp fields, not None
        cleaned_dict = {}
        u64_fields = ("ts_closed", "duration_ns", "ts_opened", "ts_last", "ts_init")
        optional_fields = ("closing_order_id", "realized_pnl", "base_currency")
        
        for key, value in position_dict.items():
            if key == "commissions":
                if isinstance(value, list):
                    commissions_dict = {}
                    for money_str in value:
                        parts = money_str.rsplit(" ", 1)
                        if len(parts) == 2:
                            amount, currency_code = parts
                            commissions_dict[currency_code] = money_str
                    cleaned_dict[key] = commissions_dict
                else:
                    cleaned_dict[key] = value
            elif value is None:
                if key in u64_fields:
                    cleaned_dict[key] = 0
                elif key in optional_fields:
                    cleaned_dict[key] = None
                elif key.endswith("_ns") or key.startswith("ts_"):
                    cleaned_dict[key] = 0
                else:
                    cleaned_dict[key] = None
            else:
                cleaned_dict[key] = value
        
        # Add missing fields
        if "events" not in cleaned_dict:
            from nautilus_trader.model.events.order import OrderFilled
            events_list = []
            for event in position.events:
                events_list.append(OrderFilled.to_dict(event))
            cleaned_dict["events"] = events_list
        
        if "trade_ids" not in cleaned_dict:
            cleaned_dict["trade_ids"] = [str(tid) for tid in position.trade_ids]
        
        if "venue_order_ids" not in cleaned_dict:
            cleaned_dict["venue_order_ids"] = [str(vo_id) for vo_id in position.venue_order_ids]
        
        if "buy_qty" not in cleaned_dict:
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            buy_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.BUY:
                    buy_qty_total = buy_qty_total + event.last_qty
            cleaned_dict["buy_qty"] = str(buy_qty_total)
        
        if "sell_qty" not in cleaned_dict:
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            sell_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.SELL:
                    sell_qty_total = sell_qty_total + event.last_qty
            cleaned_dict["sell_qty"] = str(sell_qty_total)
        
        if "price_precision" not in cleaned_dict:
            cleaned_dict["price_precision"] = position.price_precision
        
        if "size_precision" not in cleaned_dict:
            cleaned_dict["size_precision"] = position.size_precision
        
        if "multiplier" not in cleaned_dict:
            cleaned_dict["multiplier"] = str(position.multiplier)
        
        if "is_inverse" not in cleaned_dict:
            cleaned_dict["is_inverse"] = position.is_inverse
        
        if "id" not in cleaned_dict and "position_id" in cleaned_dict:
            cleaned_dict["id"] = cleaned_dict["position_id"]
        
        pyo3_position = nautilus_pyo3.Position.from_dict(cleaned_dict)
        self._backing.update_position(pyo3_position)

    def add_signal(self, signal: Data):
        signal_pyo3 = transform_signal_to_pyo3(signal)
        self._backing.add_signal(signal_pyo3)

    def add_custom_data(self, data: CustomData):
        data_pyo3 = transform_custom_data_to_pyo3(data)
        self._backing.add_custom_data(data_pyo3)

    def add_quote(self, quote: QuoteTick):
        quote_pyo3 = transform_quote_tick_to_pyo3(quote)
        self._backing.add_quote(quote_pyo3)

    def add_trade(self, trade: TradeTick):
        trade_pyo3 = transform_trade_tick_to_pyo3(trade)
        self._backing.add_trade(trade_pyo3)

    def add_bar(self, bar: Bar):
        bar_pyo3 = transform_bar_to_pyo3(bar)
        self._backing.add_bar(bar_pyo3)

    # Aliases for data engine compatibility
    def add_quote_tick(self, quote: QuoteTick):
        """Alias for add_quote - called by data engine."""
        self.add_quote(quote)

    def add_trade_tick(self, trade: TradeTick):
        """Alias for add_trade - called by data engine."""
        self.add_trade(trade)

    def add_quote_ticks(self, quotes: list[QuoteTick]):
        """Batch add quotes - called by data engine."""
        for quote in quotes:
            self.add_quote(quote)

    def add_trade_ticks(self, trades: list[TradeTick]):
        """Batch add trades - called by data engine."""
        for trade in trades:
            self.add_trade(trade)

    def add_bars(self, bars: list[Bar]):
        """Batch add bars - called by data engine."""
        for bar in bars:
            self.add_bar(bar)

    def update_order(self, order: Order):
        order_event_pyo3 = transform_order_event_to_pyo3(order.last_event)
        self._backing.update_order(order_event_pyo3)

    def update_account(self, account: Account):
        account_pyo3 = transform_account_to_pyo3(account)
        self._backing.update_account(account_pyo3)

    def index_venue_order_id(self, client_order_id: ClientOrderId, venue_order_id: VenueOrderId) -> None:
        """Index a venue order ID for a client order ID."""
        client_order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        venue_order_id_pyo3 = nautilus_pyo3.VenueOrderId.from_str(str(venue_order_id))
        self._backing.index_venue_order_id(client_order_id_pyo3, venue_order_id_pyo3)

    def index_order_position(self, client_order_id: ClientOrderId, position_id: PositionId) -> None:
        """Index a position ID for a client order ID."""
        client_order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        position_id_pyo3 = nautilus_pyo3.PositionId.from_str(str(position_id))
        self._backing.index_order_position(client_order_id_pyo3, position_id_pyo3)


class CacheHybridAdapter(CacheDatabaseFacade):
    """
    Cache database adapter for GreptimeDB-PostgreSQL hybrid storage.
    
    This adapter routes time-series data (quotes, trades, bars, signals) to GreptimeDB
    and relational data (currencies, instruments, accounts, orders, positions) to PostgreSQL.
    """

    def __init__(
        self,
        config: CacheConfig | None = None,
        user_id: str | None = None,
        postgres_host: str | None = None,
        postgres_port: int | None = None,
        postgres_username: str | None = None,
        postgres_password: str | None = None,
        postgres_database: str | None = None,
        greptime_host: str | None = None,
        greptime_port: int | None = None,
        greptime_database: str | None = None,
        greptime_username: str | None = None,
        greptime_password: str | None = None,
        buffer_interval_ms: int | None = None,
        batch_size: int | None = None,
        trading_mode: str | None = None,
    ) -> None:
        if not HAS_HYBRID_CACHE:
            raise ImportError(
                "GreptimePostgresHybridCacheAdapter is not available. "
                "Ensure the 'greptime' feature is enabled."
            )
        
        if config is None:
            config = CacheConfig()
        super().__init__(config)
        
        # Use user_id from config or get from backtest service module
        import uuid
        if user_id is None:
            # Try to get user_id from backtest service module (per-process variable)
            try:
                from nautilus_trader.services.backtest_service import get_current_user_id
                user_id = get_current_user_id()
            except (ImportError, AttributeError):
                pass
            
            if user_id is None:
                # Generate a deterministic UUID from instance_id if available
                user_id = str(uuid.uuid4())
                print(f"WARNING: No user_id provided. Generated random UUID: {user_id}", file=sys.stderr)
        
        # Get trading_mode from backtest service module (per-process variable) if not provided
        if trading_mode is None:
            try:
                from nautilus_trader.services.backtest_service import get_current_trading_mode
                trading_mode = get_current_trading_mode()
            except (ImportError, AttributeError):
                pass
        
        # Default trading_mode to "backtest" if still not set
        if trading_mode is None:
            trading_mode = "backtest"
        
        # Create hybrid cache adapter with connection parameters
        self._backing: GreptimePostgresHybridCacheAdapter = GreptimePostgresHybridCacheAdapter.connect(
            user_id=user_id,
            postgres_host=postgres_host,
            postgres_port=postgres_port,
            postgres_username=postgres_username,
            postgres_password=postgres_password,
            postgres_database=postgres_database,
            greptime_host=greptime_host,
            greptime_port=greptime_port,
            greptime_database=greptime_database,
            greptime_username=greptime_username,
            greptime_password=greptime_password,
            buffer_interval_ms=buffer_interval_ms,
            batch_size=batch_size,
            trading_mode=trading_mode,
        )

    def close(self):
        """Close the database connection."""
        self._backing.close()

    def dispose(self):
        self._backing.close()

    def flush(self):
        self._backing.flush_db()

    def load(self):
        data = self._backing.load()
        return {key: bytes(value) for key, value in data.items()}

    def load_currencies(self) -> dict[str, Currency]:
        import json
        currencies = self._backing.load_currencies()
        return {currency.code: transform_currency_from_pyo3(currency) for currency in currencies}

    def load_currency(self, code: str) -> Currency | None:
        currency_pyo3 = self._backing.load_currency(code)
        if currency_pyo3:
            return transform_currency_from_pyo3(currency_pyo3)
        return None

    def load_instruments(self) -> dict[InstrumentId, Instrument]:
        """Load all instruments from the hybrid cache."""
        instruments_list = self._backing.load_instruments()
        # instruments_list is a Vec<PyObject> from Rust (matching regular cache)
        result = {}
        for instrument_pyo3 in instruments_list:
            instrument = transform_instrument_from_pyo3(instrument_pyo3)
            # Use InstrumentId as key (not string) to match Cache.add_instrument() expectations
            result[instrument.id] = instrument
        return result
    
    def load_instrument(self, instrument_id: InstrumentId) -> Instrument:
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        instrument_pyo3 = self._backing.load_instrument(instrument_id_pyo3)
        return transform_instrument_from_pyo3(instrument_pyo3)

    def load_order(self, client_order_id: ClientOrderId):
        order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        order_pyo3 = self._backing.load_order(order_id_pyo3)
        if order_pyo3:
            return transform_order_from_pyo3(order_pyo3)
        return None

    def load_orders(self) -> dict[str, Order]:
        """Load all orders from the hybrid cache."""
        orders_dict = self._backing.load_orders()
        # orders_dict is a HashMap<String, PyObject> from Rust
        result = {}
        for order_id_str, order_pyo3 in orders_dict.items():
            order = transform_order_from_pyo3(order_pyo3)
            result[str(order.client_order_id)] = order
        return result

    def load_accounts(self) -> dict[str, Account]:
        """Load all accounts from the hybrid cache."""
        accounts_list = self._backing.load_accounts()
        # accounts_list is a Vec<PyObject> from Rust (matching regular cache)
        result = {}
        for account_pyo3 in accounts_list:
            account = transform_account_from_pyo3(account_pyo3)
            result[str(account.id)] = account
        return result
    
    def load_account(self, account_id: AccountId):
        account_id_pyo3 = nautilus_pyo3.AccountId.from_str(str(account_id))
        account_pyo3 = self._backing.load_account(account_id_pyo3)
        if account_pyo3:
            return transform_account_from_pyo3(account_pyo3)
        return None

    def load_signals(self, data_cls: type, name: str):
        signals_pyo3 = self._backing.load_signals(name)
        return [transform_signal_from_pyo3(data_cls, s) for s in signals_pyo3]

    def load_custom_data(self, data_type: DataType):
        data_type_pyo3 = transform_data_type_to_pyo3(data_type)
        data_pyo3 = self._backing.load_custom_data(data_type_pyo3)
        return [transform_custom_data_from_pyo3(d) for d in data_pyo3]

    def load_order_snapshot(
        self,
        client_order_id: ClientOrderId,
    ) -> nautilus_pyo3.OrderSnapshot | None:
        client_order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        snapshot_pyo3 = self._backing.load_order_snapshot(client_order_id_pyo3)
        return snapshot_pyo3

    def load_position_snapshot(
        self,
        position_id: PositionId,
    ) -> nautilus_pyo3.PositionSnapshot | None:
        position_id_pyo3 = nautilus_pyo3.PositionId.from_str(str(position_id))
        snapshot_pyo3 = self._backing.load_position_snapshot(position_id_pyo3)
        return snapshot_pyo3

    def load_quotes(self, instrument_id: InstrumentId) -> list[QuoteTick]:
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        quotes = self._backing.load_quotes(instrument_id_pyo3)
        return [QuoteTick.from_pyo3(quote_pyo3) for quote_pyo3 in quotes]

    def load_trades(self, instrument_id: InstrumentId) -> list[TradeTick]:
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        trades = self._backing.load_trades(instrument_id_pyo3)
        return [transform_trade_tick_from_pyo3(trade) for trade in trades]

    def load_bars(self, instrument_id: InstrumentId):
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument_id))
        bars = self._backing.load_bars(instrument_id_pyo3)
        return [Bar.from_pyo3(bar_pyo3) for bar_pyo3 in bars]

    def add(self, key: str, value: bytes):
        self._backing.add(key, value)

    def add_currency(self, currency: Currency):
        currency_pyo3 = transform_currency_to_pyo3(currency)
        self._backing.add_currency(currency_pyo3)

    def add_instrument(self, instrument: Instrument):
        # Register currencies before transforming to PyO3 (required for CurrencyPair.from_dict)
        from nautilus_trader.model.instruments.currency_pair import CurrencyPair
        if isinstance(instrument, CurrencyPair):
            # Register base and quote currencies to avoid "Unknown currency" error
            # This registers in both Python and Rust currency maps
            base_code = instrument.base_currency.code
            quote_code = instrument.quote_currency.code
            
            # Register in Python/Rust currency map (this updates Rust CURRENCY_MAP via currency_register)
            # This MUST happen before CurrencyPair.from_dict() is called, as it needs currencies in Rust map
            Currency.register(instrument.base_currency, overwrite=True)
            Currency.register(instrument.quote_currency, overwrite=True)
            
            # Verify currencies can be created from string (tests Rust map registration)
            # This ensures the registration actually worked in Rust
            try:
                test_base = Currency.from_str(base_code, strict=False)
                if test_base is None:
                    # Registration failed, try again
                    Currency.register(instrument.base_currency, overwrite=True)
            except Exception as e:
                # Registration might have failed, log and try again
                Currency.register(instrument.base_currency, overwrite=True)
            try:
                test_quote = Currency.from_str(quote_code, strict=False)
                if test_quote is None:
                    Currency.register(instrument.quote_currency, overwrite=True)
            except Exception as e:
                Currency.register(instrument.quote_currency, overwrite=True)
        
        instrument_pyo3 = transform_instrument_to_pyo3(instrument)
        self._backing.add_instrument(instrument_pyo3)

    def add_order(self, order: Order, position_id: PositionId | None = None, client_id: ClientId | None = None):
        order_pyo3 = transform_order_to_pyo3(order)
        # Pass client_id to backing store (required by PyO3 binding)
        # position_id is not used by the backing store's add_order method
        self._backing.add_order(order_pyo3, client_id)

    def add_order_snapshot(self, order: Order) -> None:
        snapshot_pyo3 = transform_order_to_snapshot_pyo3(order)
        assert snapshot_pyo3
        self._backing.add_order_snapshot(snapshot_pyo3)

    def add_position_snapshot(
        self,
        position: Position,
        unrealized_pnl: Money | None = None,
    ) -> None:
        snapshot_pyo3 = transform_position_to_snapshot_pyo3(position, unrealized_pnl)
        assert snapshot_pyo3
        self._backing.add_position_snapshot(snapshot_pyo3)

    def add_account(self, account: Account):
        account_pyo3 = transform_account_to_pyo3(account)
        self._backing.add_account(account_pyo3)

    def add_position(self, position: Position) -> None:
        """Add a position to the cache database."""
        # Position is a Cython class, but PyO3 expects a PyO3 Position object
        # Convert Cython Position to PyO3 Position using to_dict() and from_dict()
        position_dict = position.to_dict()
        
        
        # Clean None values - Rust expects u64 for timestamp fields, not None
        # Fields that should be u64: ts_closed, duration_ns (convert None to 0)
        # Position.to_dict() converts 0 timestamps to None, but Rust expects u64
        # Optional string/identifier fields can remain None
        cleaned_dict = {}
        # Fields that are u64 and should be 0 if None
        u64_fields = ("ts_closed", "duration_ns", "ts_opened", "ts_last", "ts_init")
        # Optional fields that can be None (strings/identifiers)
        optional_fields = ("closing_order_id", "realized_pnl", "base_currency")
        
        for key, value in position_dict.items():
            if key == "commissions":
                # Rust expects commissions as HashMap<Currency, Money> (dict), but to_dict() returns a list
                # Convert list of Money strings to dict: {currency_code: money_string}
                if isinstance(value, list):
                    commissions_dict = {}
                    for money_str in value:
                        # Money strings are in format "AMOUNT CURRENCY" (e.g., "0.01 USD")
                        # Rust expects HashMap<Currency, Money> where:
                        # - Key: Currency code (string like "USD")
                        # - Value: Full Money string (e.g., "0.01 USD") because Money::from_str() expects "<amount> <currency>"
                        # Split from the right to handle amounts with spaces (e.g., "1 000.00 USD")
                        parts = money_str.rsplit(" ", 1)
                        if len(parts) == 2:
                            amount, currency_code = parts
                            # Store the full Money string as the value, not just the amount
                            commissions_dict[currency_code] = money_str
                    cleaned_dict[key] = commissions_dict
                else:
                    cleaned_dict[key] = value
            elif value is None:
                # Timestamp and duration fields should be 0, not None
                if key in u64_fields:
                    cleaned_dict[key] = 0
                # Optional string/identifier fields can remain None
                elif key in optional_fields:
                    cleaned_dict[key] = None
                # For other None values, try to infer type from field name
                elif key.endswith("_ns") or key.startswith("ts_"):
                    # Looks like a timestamp field
                    cleaned_dict[key] = 0
                else:
                    # Keep as None for now, but log it
                    cleaned_dict[key] = None
            else:
                cleaned_dict[key] = value
        
        # Add missing fields that Rust Position struct requires but Python to_dict() doesn't include
        # These fields are required by Rust's serde deserialization
        if "events" not in cleaned_dict:
            # events is a Vec<OrderFilled> - convert Python events to list of dicts
            # to_dict is a static method, so call it as OrderFilled.to_dict(event)
            from nautilus_trader.model.events.order import OrderFilled
            events_list = []
            for event in position.events:
                events_list.append(OrderFilled.to_dict(event))
            cleaned_dict["events"] = events_list
        
        if "trade_ids" not in cleaned_dict:
            # trade_ids is a Vec<TradeId> - convert to list of strings
            cleaned_dict["trade_ids"] = [str(tid) for tid in position.trade_ids]
        
        if "venue_order_ids" not in cleaned_dict:
            # venue_order_ids is a Vec<VenueOrderId> - convert to list of strings
            # venue_order_ids is a @property, not a method, so access without parentheses
            cleaned_dict["venue_order_ids"] = [str(vo_id) for vo_id in position.venue_order_ids]
        
        if "buy_qty" not in cleaned_dict:
            # buy_qty is a Quantity - convert to string
            # _buy_qty is a cdef attribute not accessible from Python, so calculate from events
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            buy_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.BUY:
                    buy_qty_total = buy_qty_total + event.last_qty
            cleaned_dict["buy_qty"] = str(buy_qty_total)
        
        if "sell_qty" not in cleaned_dict:
            # sell_qty is a Quantity - convert to string
            # _sell_qty is a cdef attribute not accessible from Python, so calculate from events
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            sell_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.SELL:
                    sell_qty_total = sell_qty_total + event.last_qty
            cleaned_dict["sell_qty"] = str(sell_qty_total)
        
        # Add other missing fields that Rust requires
        if "price_precision" not in cleaned_dict:
            cleaned_dict["price_precision"] = position.price_precision
        
        if "size_precision" not in cleaned_dict:
            cleaned_dict["size_precision"] = position.size_precision
        
        if "multiplier" not in cleaned_dict:
            cleaned_dict["multiplier"] = str(position.multiplier)
        
        if "is_inverse" not in cleaned_dict:
            cleaned_dict["is_inverse"] = position.is_inverse
        
        
        # Rust Position struct expects "id" field, but Python to_dict() uses "position_id"
        # Add "id" field mapped from "position_id" if it exists
        if "id" not in cleaned_dict and "position_id" in cleaned_dict:
            cleaned_dict["id"] = cleaned_dict["position_id"]
        
        pyo3_position = nautilus_pyo3.Position.from_dict(cleaned_dict)
        self._backing.add_position(pyo3_position)
    
    def update_position(self, position: Position) -> None:
        """Update a position in the cache database."""
        # Reuse the same conversion logic as add_position
        position_dict = position.to_dict()
        
        # Clean None values - Rust expects u64 for timestamp fields, not None
        cleaned_dict = {}
        u64_fields = ("ts_closed", "duration_ns", "ts_opened", "ts_last", "ts_init")
        optional_fields = ("closing_order_id", "realized_pnl", "base_currency")
        
        for key, value in position_dict.items():
            if key == "commissions":
                if isinstance(value, list):
                    commissions_dict = {}
                    for money_str in value:
                        parts = money_str.rsplit(" ", 1)
                        if len(parts) == 2:
                            amount, currency_code = parts
                            commissions_dict[currency_code] = money_str
                    cleaned_dict[key] = commissions_dict
                else:
                    cleaned_dict[key] = value
            elif value is None:
                if key in u64_fields:
                    cleaned_dict[key] = 0
                elif key in optional_fields:
                    cleaned_dict[key] = None
                elif key.endswith("_ns") or key.startswith("ts_"):
                    cleaned_dict[key] = 0
                else:
                    cleaned_dict[key] = None
            else:
                cleaned_dict[key] = value
        
        # Add missing fields
        if "events" not in cleaned_dict:
            from nautilus_trader.model.events.order import OrderFilled
            events_list = []
            for event in position.events:
                events_list.append(OrderFilled.to_dict(event))
            cleaned_dict["events"] = events_list
        
        if "trade_ids" not in cleaned_dict:
            cleaned_dict["trade_ids"] = [str(tid) for tid in position.trade_ids]
        
        if "venue_order_ids" not in cleaned_dict:
            cleaned_dict["venue_order_ids"] = [str(vo_id) for vo_id in position.venue_order_ids]
        
        if "buy_qty" not in cleaned_dict:
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            buy_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.BUY:
                    buy_qty_total = buy_qty_total + event.last_qty
            cleaned_dict["buy_qty"] = str(buy_qty_total)
        
        if "sell_qty" not in cleaned_dict:
            from nautilus_trader.model.enums import OrderSide
            from nautilus_trader.model.objects import Quantity
            sell_qty_total = Quantity.zero(position.size_precision)
            for event in position.events:
                if event.order_side == OrderSide.SELL:
                    sell_qty_total = sell_qty_total + event.last_qty
            cleaned_dict["sell_qty"] = str(sell_qty_total)
        
        if "price_precision" not in cleaned_dict:
            cleaned_dict["price_precision"] = position.price_precision
        
        if "size_precision" not in cleaned_dict:
            cleaned_dict["size_precision"] = position.size_precision
        
        if "multiplier" not in cleaned_dict:
            cleaned_dict["multiplier"] = str(position.multiplier)
        
        if "is_inverse" not in cleaned_dict:
            cleaned_dict["is_inverse"] = position.is_inverse
        
        if "id" not in cleaned_dict and "position_id" in cleaned_dict:
            cleaned_dict["id"] = cleaned_dict["position_id"]
        
        pyo3_position = nautilus_pyo3.Position.from_dict(cleaned_dict)
        self._backing.update_position(pyo3_position)

    def add_signal(self, signal: Data):
        signal_pyo3 = transform_signal_to_pyo3(signal)
        self._backing.add_signal(signal_pyo3)

    def add_custom_data(self, data: CustomData):
        data_pyo3 = transform_custom_data_to_pyo3(data)
        self._backing.add_custom_data(data_pyo3)

    def add_quote(self, quote: QuoteTick):
        quote_pyo3 = transform_quote_tick_to_pyo3(quote)
        self._backing.add_quote(quote_pyo3)

    def add_trade(self, trade: TradeTick):
        trade_pyo3 = transform_trade_tick_to_pyo3(trade)
        self._backing.add_trade(trade_pyo3)

    def add_bar(self, bar: Bar):
        bar_pyo3 = transform_bar_to_pyo3(bar)
        self._backing.add_bar(bar_pyo3)

    def add_indicator(
        self,
        instrument_id: InstrumentId,
        indicator_name: str,
        indicator_type: str,
        bar_type: BarType,
        timestamp: int,
        value: float | None = None,
        values_dict: dict | None = None,
    ) -> None:
        """
        Add indicator data to GreptimeDB.
        
        Args:
            instrument_id: The instrument ID
            indicator_name: Name of the indicator (e.g., "fast_ema")
            indicator_type: Type of indicator (e.g., "EMA", "MACD", "Bollinger Bands")
            bar_type: BarType associated with this indicator
            timestamp: Timestamp in nanoseconds
            value: Single value for single-value indicators (None for multi-value)
            values_dict: Dictionary of values for multi-value indicators (None for single-value)
        """
        import json
        
        # Serialize values_dict to JSON string if provided
        values_json = None
        if values_dict is not None:
            values_json = json.dumps(values_dict)
        
        # Convert bar_type to string for Rust
        bar_type_str = str(bar_type)
        
        # Call Rust method
        try:
            self._backing.add_indicator(
                instrument_id=str(instrument_id),
                indicator_name=indicator_name,
                indicator_type=indicator_type,
                bar_type_str=bar_type_str,
                timestamp=timestamp,
                value=value,
                values_json=values_json,
            )
        except Exception as e:
            raise

    # Aliases for data engine compatibility
    def add_quote_tick(self, quote: QuoteTick):
        """Alias for add_quote - called by data engine."""
        self.add_quote(quote)

    def add_trade_tick(self, trade: TradeTick):
        """Alias for add_trade - called by data engine."""
        self.add_trade(trade)

    def add_quote_ticks(self, quotes: list[QuoteTick]):
        """Batch add quotes - called by data engine."""
        for quote in quotes:
            self.add_quote(quote)

    def add_trade_ticks(self, trades: list[TradeTick]):
        """Batch add trades - called by data engine."""
        for trade in trades:
            self.add_trade(trade)

    def add_bars(self, bars: list[Bar]):
        """Batch add bars - called by data engine."""
        for bar in bars:
            self.add_bar(bar)

    def update_order(self, order: Order):
        order_event_pyo3 = transform_order_event_to_pyo3(order.last_event)
        self._backing.update_order(order_event_pyo3)

    def update_account(self, account: Account):
        account_pyo3 = transform_account_to_pyo3(account)
        self._backing.update_account(account_pyo3)

    def load_index_order_position(self) -> dict[ClientOrderId, PositionId]:
        """Load the order to position index from the database."""
        index_dict = self._backing.load_index_order_position()
        # Convert from HashMap<String, PyObject> to dict[ClientOrderId, PositionId]
        result = {}
        for order_id_str, position_id_pyo3 in index_dict.items():
            # position_id_pyo3 is a PyObject representing PositionId
            # Convert PyObject to string and then to PositionId
            position_id_str = str(position_id_pyo3)
            result[ClientOrderId.from_str(order_id_str)] = PositionId.from_str(position_id_str)
        return result

    def load_index_order_client(self) -> dict[ClientOrderId, ClientId]:
        """Load the order to execution client index from the database."""
        index_dict = self._backing.load_index_order_client()
        # Convert from HashMap<String, String> to dict[ClientOrderId, ClientId]
        result = {}
        for order_id_str, client_id_str in index_dict.items():
            result[ClientOrderId.from_str(order_id_str)] = ClientId.from_str(client_id_str)
        return result

    def index_venue_order_id(self, client_order_id: ClientOrderId, venue_order_id: VenueOrderId) -> None:
        """Index a venue order ID for a client order ID."""
        client_order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        venue_order_id_pyo3 = nautilus_pyo3.VenueOrderId.from_str(str(venue_order_id))
        self._backing.index_venue_order_id(client_order_id_pyo3, venue_order_id_pyo3)

    def index_order_position(self, client_order_id: ClientOrderId, position_id: PositionId) -> None:
        """Index a position ID for a client order ID."""
        client_order_id_pyo3 = nautilus_pyo3.ClientOrderId.from_str(str(client_order_id))
        position_id_pyo3 = nautilus_pyo3.PositionId.from_str(str(position_id))
        self._backing.index_order_position(client_order_id_pyo3, position_id_pyo3)

    def load_positions(self) -> dict[PositionId, Position]:
        """Load all positions from the hybrid cache."""
        positions_dict = self._backing.load_positions()
        # positions_dict is a HashMap<String, PyObject> from Rust
        result = {}
        for position_id_str, position_pyo3 in positions_dict.items():
            # Convert PyObject to Position using from_pyo3
            position = Position.from_pyo3(position_pyo3)
            result[position.id] = position
        return result

    def load_position(self, position_id: PositionId) -> Position | None:
        """Load a single position by ID."""
        position_id_pyo3 = nautilus_pyo3.PositionId.from_str(str(position_id))
        position_pyo3 = self._backing.load_position(position_id_pyo3)
        if position_pyo3:
            return Position.from_pyo3(position_pyo3)
        return None

    def load_all(self) -> dict:
        """
        Load all cache data from the hybrid cache database.
        
        Returns
        -------
        dict[str, dict]
            A dictionary containing all cache data organized by category.
        """
        raw_data = self._backing.load_all()
        
        result = {}
        
        # Transform currencies
        currencies_raw = raw_data.get("currencies", {})
        
        if not isinstance(currencies_raw, dict):
            raise TypeError(f"Expected dict for currencies, got {type(currencies_raw)}: {currencies_raw}")
        
        currencies_dict = currencies_raw
        result["currencies"] = {
            key: transform_currency_from_pyo3(value) for key, value in currencies_dict.items()
        }
        
        # Transform instruments
        instruments_dict = raw_data.get("instruments", {})
        result["instruments"] = {
            key: transform_instrument_from_pyo3(value) for key, value in instruments_dict.items()
        }
        
        # Synthetics (no transformation needed if already PyObjects)
        result["synthetics"] = raw_data.get("synthetics", {})
        
        # Transform accounts
        accounts_dict = raw_data.get("accounts", {})
        result["accounts"] = {
            key: transform_account_from_pyo3(value) for key, value in accounts_dict.items()
        }
        
        # Transform orders
        orders_dict = raw_data.get("orders", {})
        result["orders"] = {
            key: transform_order_from_pyo3(value) for key, value in orders_dict.items()
        }
        
        # Positions (no transformation needed if already PyObjects)
        result["positions"] = raw_data.get("positions", {})
        
        return result


def create_cache_database_adapter(
    config: CacheConfig,
    user_id: str | None = None,
) -> CacheDatabaseFacade | None:
    """
    Factory function to create a cache database adapter from CacheConfig.
    
    Parameters
    ----------
    config : CacheConfig
        The cache configuration.
    user_id : str, optional
        User ID for hybrid cache (required if using hybrid type).
        
    Returns
    -------
    CacheDatabaseFacade | None
        The created cache database adapter, or None if no database config is provided.
    """
    if config.database is None:
        return None
    
    db_config = config.database
    
    # Use hybrid cache for "hybrid" type, regular postgres for "postgres" type
    if db_config.type == "hybrid":
        if not HAS_HYBRID_CACHE:
            raise ImportError(
                "GreptimePostgresHybridCacheAdapter is not available. "
                "Ensure the 'greptime' feature is enabled."
            )
        if user_id is None:
            # Try to get user_id from backtest service module (per-process variable)
            try:
                from nautilus_trader.services.backtest_service import get_current_user_id
                user_id = get_current_user_id()
            except (ImportError, AttributeError) as e:
                pass
            
            if user_id is None:
                import uuid
                user_id = str(uuid.uuid4())
                print(f"WARNING: No user_id provided. Generated random UUID: {user_id}", file=sys.stderr)
        
        # Get trading_mode from backtest service module (per-process variable)
        trading_mode = None
        try:
            from nautilus_trader.services.backtest_service import get_current_trading_mode
            trading_mode = get_current_trading_mode()
        except (ImportError, AttributeError):
            pass
        
        # Default trading_mode to "backtest" if not set
        if trading_mode is None:
            trading_mode = "backtest"
        
        # Read GreptimeDB configuration from environment variables
        import os
        greptime_host = os.getenv("GREPTIME_HOST", "localhost")
        greptime_port = int(os.getenv("GREPTIME_PORT", "4000")) if os.getenv("GREPTIME_PORT") else None
        greptime_database = os.getenv("GREPTIME_DATABASE", "nautilus_timeseries")
        greptime_username = os.getenv("GREPTIME_USERNAME")
        greptime_password = os.getenv("GREPTIME_PASSWORD")
        
        # Read PostgreSQL database name from environment (set by backtest_service)
        # or use default
        postgres_database = os.getenv("POSTGRES_DATABASE", "fincept_terminal")
        
        return CacheHybridAdapter(
            config=config,
            user_id=user_id,
            postgres_host=db_config.host,
            postgres_port=db_config.port,
            postgres_username=db_config.username,
            postgres_password=db_config.password,
            postgres_database=postgres_database,
            greptime_host=greptime_host,
            greptime_port=greptime_port,
            greptime_database=greptime_database,
            greptime_username=greptime_username,
            greptime_password=greptime_password,
            buffer_interval_ms=config.buffer_interval_ms,
            trading_mode=trading_mode,
        )
    elif db_config.type == "postgres":
        return CachePostgresAdapter(config=config)
    else:
        raise ValueError(f"Unsupported database type: {db_config.type}. Supported types are: 'postgres', 'hybrid'.")
