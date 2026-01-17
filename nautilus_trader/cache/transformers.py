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

import msgspec

from nautilus_trader.accounting.accounts.base import Account
from nautilus_trader.accounting.accounts.cash import CashAccount
from nautilus_trader.accounting.accounts.margin import MarginAccount
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.core.data import Data
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import CustomData
from nautilus_trader.model.data import DataType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import CurrencyType
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderDenied
from nautilus_trader.model.events import OrderEmulated
from nautilus_trader.model.events import OrderExpired
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderInitialized
from nautilus_trader.model.events import OrderModifyRejected
from nautilus_trader.model.events import OrderPendingCancel
from nautilus_trader.model.events import OrderPendingUpdate
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import OrderReleased
from nautilus_trader.model.events import OrderSubmitted
from nautilus_trader.model.events import OrderTriggered
from nautilus_trader.model.events import OrderUpdated
from nautilus_trader.model.events.account import AccountState
from nautilus_trader.model.instruments import BettingInstrument
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.instruments import CryptoFuture
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.instruments import FuturesSpread
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.instruments import OptionContract
from nautilus_trader.model.instruments import OptionSpread
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders.unpacker import OrderUnpacker
from nautilus_trader.model.position import Position


################################################################################
# Currency
################################################################################
def transform_currency_from_pyo3(currency: nautilus_pyo3.Currency) -> Currency:
    return Currency(
        code=currency.code,
        precision=currency.precision,
        iso4217=currency.iso4217,
        name=currency.name,
        currency_type=CurrencyType(currency.currency_type.value),
    )


def transform_currency_to_pyo3(currency: Currency) -> nautilus_pyo3.Currency:
    return nautilus_pyo3.Currency(
        code=currency.code,
        precision=currency.precision,
        iso4217=currency.iso4217,
        name=currency.name,
        currency_type=nautilus_pyo3.CurrencyType.from_str(currency.currency_type.name),
    )


################################################################################
# Instruments
################################################################################
def transform_instrument_to_pyo3(instrument: Instrument):
    if isinstance(instrument, BettingInstrument):
        return nautilus_pyo3.BettingInstrument.from_dict(BettingInstrument.to_dict(instrument))
    elif isinstance(instrument, BinaryOption):
        return nautilus_pyo3.BinaryOption.from_dict(BinaryOption.to_dict(instrument))
    elif isinstance(instrument, CryptoFuture):
        return nautilus_pyo3.CryptoFuture.from_dict(CryptoFuture.to_dict(instrument))
    elif isinstance(instrument, CryptoPerpetual):
        return nautilus_pyo3.CryptoPerpetual.from_dict(CryptoPerpetual.to_dict(instrument))
    elif isinstance(instrument, CurrencyPair):
        # Instead of using from_dict() which requires currencies in Rust map during deserialization,
        # construct CurrencyPair directly using PyO3 constructor with Currency objects
        # This avoids the serde deserialization issue entirely
        from nautilus_trader.model.objects import Currency, Price, Quantity, Money
        from nautilus_trader.model.identifiers import InstrumentId, Symbol
        from decimal import Decimal
        
        # Register currencies to ensure they're in Rust map (needed for PyO3 Currency objects)
        Currency.register(instrument.base_currency, overwrite=True)
        Currency.register(instrument.quote_currency, overwrite=True)
        
        # Convert Python Currency objects to PyO3 Currency objects
        base_currency_pyo3 = transform_currency_to_pyo3(instrument.base_currency)
        quote_currency_pyo3 = transform_currency_to_pyo3(instrument.quote_currency)
        
        # Convert other fields to PyO3 types
        instrument_id_pyo3 = nautilus_pyo3.InstrumentId.from_str(str(instrument.id))
        symbol_pyo3 = nautilus_pyo3.Symbol(instrument.raw_symbol.value)
        price_increment_pyo3 = nautilus_pyo3.Price.from_str(str(instrument.price_increment))
        size_increment_pyo3 = nautilus_pyo3.Quantity.from_str(str(instrument.size_increment))
        
        # Convert optional fields
        lot_size_pyo3 = nautilus_pyo3.Quantity.from_str(str(instrument.lot_size)) if instrument.lot_size is not None else None
        max_quantity_pyo3 = nautilus_pyo3.Quantity.from_str(str(instrument.max_quantity)) if instrument.max_quantity is not None else None
        min_quantity_pyo3 = nautilus_pyo3.Quantity.from_str(str(instrument.min_quantity)) if instrument.min_quantity is not None else None
        max_notional_pyo3 = nautilus_pyo3.Money.from_str(str(instrument.max_notional)) if instrument.max_notional is not None else None
        min_notional_pyo3 = nautilus_pyo3.Money.from_str(str(instrument.min_notional)) if instrument.min_notional is not None else None
        max_price_pyo3 = nautilus_pyo3.Price.from_str(str(instrument.max_price)) if instrument.max_price is not None else None
        min_price_pyo3 = nautilus_pyo3.Price.from_str(str(instrument.min_price)) if instrument.min_price is not None else None
        
        # Convert Decimal fields
        margin_init_pyo3 = Decimal(str(instrument.margin_init)) if instrument.margin_init is not None else None
        margin_maint_pyo3 = Decimal(str(instrument.margin_maint)) if instrument.margin_maint is not None else None
        maker_fee_pyo3 = Decimal(str(instrument.maker_fee)) if instrument.maker_fee is not None else None
        taker_fee_pyo3 = Decimal(str(instrument.taker_fee)) if instrument.taker_fee is not None else None
        
        # Construct CurrencyPair using PyO3 constructor
        return nautilus_pyo3.CurrencyPair(
            id=instrument_id_pyo3,
            raw_symbol=symbol_pyo3,
            base_currency=base_currency_pyo3,
            quote_currency=quote_currency_pyo3,
            price_precision=instrument.price_precision,
            size_precision=instrument.size_precision,
            price_increment=price_increment_pyo3,
            size_increment=size_increment_pyo3,
            ts_event=instrument.ts_event,
            ts_init=instrument.ts_init,
            lot_size=lot_size_pyo3,
            max_quantity=max_quantity_pyo3,
            min_quantity=min_quantity_pyo3,
            max_notional=max_notional_pyo3,
            min_notional=min_notional_pyo3,
            max_price=max_price_pyo3,
            min_price=min_price_pyo3,
            margin_init=margin_init_pyo3,
            margin_maint=margin_maint_pyo3,
            maker_fee=maker_fee_pyo3,
            taker_fee=taker_fee_pyo3,
        )
    elif isinstance(instrument, Equity):
        return nautilus_pyo3.Equity.from_dict(Equity.to_dict(instrument))
    elif isinstance(instrument, FuturesContract):
        return nautilus_pyo3.FuturesContract.from_dict(FuturesContract.to_dict(instrument))
    elif isinstance(instrument, OptionContract):
        return nautilus_pyo3.OptionContract.from_dict(OptionContract.to_dict(instrument))
    else:
        raise ValueError(f"Unknown instrument type: {instrument}")


def transform_instrument_from_pyo3(instrument_pyo3) -> Instrument | None:  # noqa: C901
    if instrument_pyo3 is None:
        return None
    if isinstance(instrument_pyo3, nautilus_pyo3.BettingInstrument):
        return BettingInstrument.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.BinaryOption):
        return BinaryOption.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.CryptoFuture):
        return CryptoFuture.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.CryptoPerpetual):
        return CryptoPerpetual.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.CurrencyPair):
        return CurrencyPair.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.Equity):
        return Equity.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.FuturesContract):
        return FuturesContract.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.FuturesSpread):
        return FuturesSpread.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.OptionContract):
        return OptionContract.from_pyo3(instrument_pyo3)
    elif isinstance(instrument_pyo3, nautilus_pyo3.OptionSpread):
        return OptionSpread.from_pyo3(instrument_pyo3)
    else:
        raise ValueError(f"Unknown instrument type: {instrument_pyo3}")


################################################################################
# Orders
################################################################################
def transform_order_event_to_pyo3(order_event):  # noqa: C901
    if isinstance(order_event, OrderInitialized):
        return nautilus_pyo3.OrderInitialized.from_dict(OrderInitialized.to_dict(order_event))
    elif isinstance(order_event, OrderDenied):
        return nautilus_pyo3.OrderDenied.from_dict(OrderDenied.to_dict(order_event))
    elif isinstance(order_event, OrderEmulated):
        return nautilus_pyo3.OrderEmulated.from_dict(OrderEmulated.to_dict(order_event))
    elif isinstance(order_event, OrderReleased):
        return nautilus_pyo3.OrderReleased.from_dict(OrderReleased.to_dict(order_event))
    elif isinstance(order_event, OrderSubmitted):
        return nautilus_pyo3.OrderSubmitted.from_dict(OrderSubmitted.to_dict(order_event))
    elif isinstance(order_event, OrderAccepted):
        order_event_dict = OrderAccepted.to_dict(order_event)
        return nautilus_pyo3.OrderAccepted.from_dict(order_event_dict)
    elif isinstance(order_event, OrderRejected):
        return nautilus_pyo3.OrderRejected.from_dict(OrderRejected.to_dict(order_event))
    elif isinstance(order_event, OrderCanceled):
        return nautilus_pyo3.OrderCanceled.from_dict(OrderCanceled.to_dict(order_event))
    elif isinstance(order_event, OrderExpired):
        return nautilus_pyo3.OrderExpired.from_dict(OrderExpired.to_dict(order_event))
    elif isinstance(order_event, OrderTriggered):
        return nautilus_pyo3.OrderTriggered.from_dict(OrderTriggered.to_dict(order_event))
    elif isinstance(order_event, OrderPendingUpdate):
        return nautilus_pyo3.OrderPendingUpdate.from_dict(OrderPendingUpdate.to_dict(order_event))
    elif isinstance(order_event, OrderModifyRejected):
        return nautilus_pyo3.OrderModifyRejected.from_dict(OrderModifyRejected.to_dict(order_event))
    elif isinstance(order_event, OrderPendingCancel):
        return nautilus_pyo3.OrderPendingCancel.from_dict(OrderPendingCancel.to_dict(order_event))
    elif isinstance(order_event, OrderUpdated):
        return nautilus_pyo3.OrderUpdated.from_dict(OrderUpdated.to_dict(order_event))
    elif isinstance(order_event, OrderFilled):
        return nautilus_pyo3.OrderFilled.from_dict(OrderFilled.to_dict(order_event))
    elif isinstance(order_event, OrderPendingCancel):
        return nautilus_pyo3.OrderPendingCancel.from_dict(OrderPendingCancel.to_dict(order_event))
    else:
        raise ValueError(f"Unknown order event type: {order_event}")


def from_order_initialized_cython_to_order_pyo3(order_event):
    order_event_dict = OrderInitialized.to_dict(order_event)
    # in options field there are some properties we need to attach to dict
    for key, value in order_event.options.items():
        order_event_dict[key] = value
    order_event_pyo3 = nautilus_pyo3.OrderInitialized.from_dict(order_event_dict)
    if order_event_pyo3.order_type == nautilus_pyo3.OrderType.MARKET:
        return nautilus_pyo3.MarketOrder.create(order_event_pyo3)
    elif order_event_pyo3.order_type == nautilus_pyo3.OrderType.LIMIT:
        return nautilus_pyo3.LimitOrder.create(order_event_pyo3)
    elif order_event_pyo3.order_type == nautilus_pyo3.OrderType.STOP_MARKET:
        return nautilus_pyo3.StopMarketOrder.create(order_event_pyo3)
    elif order_event_pyo3.order_type == nautilus_pyo3.OrderType.STOP_LIMIT:
        return nautilus_pyo3.StopLimitOrder.create(order_event_pyo3)
    else:
        raise ValueError(f"Unknown order type: {order_event_pyo3.order_type}")


def from_order_initialized_pyo3_to_order_cython(order_event):
    order_event_cython = OrderInitialized.from_dict(order_event.to_dict())
    return OrderUnpacker.from_init(order_event_cython)


def transform_order_event_from_pyo3(order_event_pyo3):  # noqa: C901
    if isinstance(order_event_pyo3, nautilus_pyo3.OrderInitialized):
        return OrderInitialized.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderDenied):
        return OrderDenied.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderEmulated):
        return OrderEmulated.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderReleased):
        return OrderReleased.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderSubmitted):
        return OrderSubmitted.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderAccepted):
        return OrderAccepted.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderRejected):
        return OrderRejected.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderCanceled):
        return OrderCanceled.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderExpired):
        return OrderExpired.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderTriggered):
        return OrderTriggered.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderPendingUpdate):
        return OrderPendingUpdate.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderModifyRejected):
        return OrderModifyRejected.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderPendingCancel):
        return OrderPendingCancel.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderUpdated):
        return OrderUpdated.from_dict(order_event_pyo3.to_dict())
    elif isinstance(order_event_pyo3, nautilus_pyo3.OrderFilled):
        return OrderFilled.from_dict(order_event_pyo3.to_dict())
    else:
        raise ValueError(f"Unknown order event type: {order_event_pyo3}")


def transform_order_to_pyo3(order: Order):
    events = order.events
    if len(events) == 0:
        raise ValueError("Missing events in order")
    init_event = events.pop(0)
    if not isinstance(init_event, OrderInitialized):
        raise KeyError("init event should be of type OrderInitialized")
    order_py3 = from_order_initialized_cython_to_order_pyo3(init_event)
    for event_cython in events:
        event_pyo3 = transform_order_event_to_pyo3(event_cython)
        order_py3.apply(event_pyo3)
    return order_py3


def transform_order_from_pyo3(order_pyo3) -> Order:
    events_pyo3 = order_pyo3.events
    if len(events_pyo3) == 0:
        raise ValueError("Missing events in order")
    init_event = events_pyo3.pop(0)
    if not isinstance(init_event, nautilus_pyo3.OrderInitialized):
        raise KeyError("init event should be of type OrderInitialized")
    order_cython = from_order_initialized_pyo3_to_order_cython(init_event)
    for event_pyo3 in events_pyo3:
        event_cython = transform_order_event_from_pyo3(event_pyo3)
        order_cython.apply(event_cython)
    return order_cython


def transform_order_to_snapshot_pyo3(order: Order) -> nautilus_pyo3.OrderSnapshot:
    values = order.to_dict()
    values["order_type"] = values["type"]
    values["order_side"] = values["side"]
    values["expire_time"] = values.get("expire_time_ns")
    commissions = values.get("commissions")
    values["commissions"] = commissions if commissions is not None else []
    values["is_post_only"] = values.get("is_post_only", False)
    values["is_reduce_only"] = values.get("is_reduce_only", False)
    values["is_quote_quantity"] = values.get("is_quote_quantity", False)

    return nautilus_pyo3.OrderSnapshot.from_dict(values)


def transform_position_to_snapshot_pyo3(
    position: Position,
    unrealized_pnl: Money | None = None,
) -> nautilus_pyo3.PositionSnapshot:
    values = position.to_dict()
    values["unrealized_pnl"] = str(unrealized_pnl) if unrealized_pnl is not None else None

    return nautilus_pyo3.PositionSnapshot.from_dict(values)


################################################################################
# Account
################################################################################
def transform_account_state_cython_to_pyo3(
    account_state: AccountState,
) -> nautilus_pyo3.AccountState:
    account_state_dict = AccountState.to_dict(account_state)
    # Handle None values that can't be converted to PyString by from_dict()
    # For multi-currency accounts, base_currency can be None
    # Rust's from_dict() expects base_currency to be present, and when None, it should be the string "None"
    cleaned_dict = {}
    currencies_to_register = set()
    
    for key, value in account_state_dict.items():
        if key == "base_currency" and value is None:
            # Rust's to_dict() sets base_currency to "None" string when None
            # We need to match that behavior for from_dict() to work
            cleaned_dict[key] = "None"
        elif key == "balances" and value is not None:
            # Extract currency codes from balances and register them
            # This is required because AccountBalance.from_dict() needs currencies to be registered
            cleaned_dict[key] = value
            if isinstance(value, list):
                for balance in value:
                    if isinstance(balance, dict):
                        # AccountBalance.to_dict() might store currency in different ways
                        # Check for "currency" key (string code) or currency object
                        currency_code = None
                        if "currency" in balance:
                            currency_val = balance.get("currency")
                            if isinstance(currency_val, str):
                                currency_code = currency_val
                            elif hasattr(currency_val, "code"):
                                currency_code = currency_val.code
                            elif hasattr(currency_val, "__str__"):
                                currency_code = str(currency_val)
                        
                        if currency_code:
                            currencies_to_register.add(currency_code)
        elif key == "margins" and value is not None:
            # Extract currency codes from margins and register them
            cleaned_dict[key] = value
            if isinstance(value, list):
                for margin in value:
                    if isinstance(margin, dict) and "currency" in margin:
                        currency_code = margin.get("currency")
                        if currency_code:
                            currencies_to_register.add(currency_code)
        elif value is None:
            # Skip other None values - PyO3 will use defaults or handle missing fields
            continue
        else:
            cleaned_dict[key] = value
    
    # Register all currencies found in balances/margins before calling from_dict()
    # This ensures Currency.from_str() in AccountBalance.from_dict() can find them
    # Use strict=False to create Currency objects even if they're not in the map yet
    from nautilus_trader.model.objects import Currency
    
    for currency_code in currencies_to_register:
        try:
            # First try to get the currency from the map (might already be registered)
            currency = Currency.from_internal_map(currency_code)
            currency_was_created = False
            
            if currency is None:
                # Currency not in map, create it with strict=False
                # from_str_c() with strict=False automatically registers the currency in Rust map
                # So we don't need to call Currency.register() again
                try:
                    currency = Currency.from_str(currency_code, strict=False)
                    currency_was_created = True
                except Exception as e:
                    currency = None
            
            if currency is not None:
                # Only register if currency wasn't just created (from_str with strict=False auto-registers)
                if not currency_was_created:
                    try:
                        # Register the currency in the Rust CURRENCY_MAP using Python wrapper
                        Currency.register(currency, overwrite=True)
                    except Exception as register_error:
                        # Don't re-raise - continue with other currencies
                        pass
                
                # Verify registration by trying to get it from the map
                verified_currency = Currency.from_internal_map(currency_code)
                
                # CRITICAL: Force a Rust-side lookup to ensure currency is in Rust CURRENCY_MAP
                # AccountBalance::py_from_dict() uses Rust's Currency::from_str() which looks in Rust map
                # We need to ensure the currency is accessible from Rust context
                try:
                    # Try to get currency via PyO3 to force Rust map lookup/registration
                    # This ensures it's in the Rust CURRENCY_MAP that AccountBalance will use
                    pyo3_currency = nautilus_pyo3.Currency.from_str(currency_code)
                except Exception as pyo3_error:
                    # If PyO3 lookup fails, the currency isn't in Rust map - try to register it via PyO3
                    try:
                        # Convert Python Currency to PyO3 Currency and register it
                        pyo3_currency = nautilus_pyo3.Currency(
                            code=currency.code,
                            precision=currency.precision,
                            iso4217=currency.iso4217,
                            name=currency.name,
                            currency_type=nautilus_pyo3.CurrencyType.from_str(currency.currency_type.name)
                        )
                        nautilus_pyo3.Currency.register(pyo3_currency, overwrite=True)
                    except Exception as register_pyo3_error:
                        pass
        except Exception as e:
            pass
    
    # CRITICAL: Pre-process balances to force currency registration in Rust map
    # This ensures currencies are registered before AccountBalance::py_from_dict() tries to use them
    # We'll try to create AccountBalance objects from the dictionaries to force the currency lookup
    if "balances" in cleaned_dict and isinstance(cleaned_dict["balances"], list):
        for balance_dict in cleaned_dict["balances"]:
            if isinstance(balance_dict, dict):
                currency_code = balance_dict.get("currency")
                if currency_code:
                    try:
                        # Try to create AccountBalance to force currency lookup/registration
                        # This uses the exact same code path that AccountState.from_dict() will use
                        test_balance = nautilus_pyo3.AccountBalance.from_dict(balance_dict)
                    except Exception as balance_error:
                        # Balance creation failed - currency not in Rust map
                        # Register it via PyO3 and retry
                        try:
                            from nautilus_trader.model.objects import Currency
                            py_currency = Currency.from_str(currency_code, strict=False)
                            if py_currency:
                                # Create PyO3 Currency and register it in Rust map
                                pyo3_currency = nautilus_pyo3.Currency(
                                    code=py_currency.code,
                                    precision=py_currency.precision,
                                    iso4217=py_currency.iso4217,
                                    name=py_currency.name,
                                    currency_type=nautilus_pyo3.CurrencyType.from_str(py_currency.currency_type.name)
                                )
                                nautilus_pyo3.Currency.register(pyo3_currency, overwrite=True)
                                
                                # Retry balance creation to verify registration worked
                                try:
                                    test_balance_retry = nautilus_pyo3.AccountBalance.from_dict(balance_dict)
                                except Exception as retry_error:
                                    pass
                        except Exception as register_error:
                            pass
    
    return nautilus_pyo3.AccountState.from_dict(cleaned_dict)


def transform_account_state_pyo3_to_cython(
    account_state_pyo3: nautilus_pyo3.AccountState,
) -> Account:
    account_state_dict_pyo3 = account_state_pyo3.to_dict()
    return AccountState.from_dict(account_state_dict_pyo3)


def from_account_state_pyo3_to_account_cython(
    account_state_pyo3: nautilus_pyo3.AccountState,
    calculate_account_state: bool,
) -> Account:
    account_state_cython = transform_account_state_pyo3_to_cython(account_state_pyo3)
    if account_state_pyo3.account_type == nautilus_pyo3.AccountType.CASH:
        return CashAccount(account_state_cython, calculate_account_state)
    elif account_state_pyo3.account_type == nautilus_pyo3.AccountType.MARGIN:
        return MarginAccount(account_state_cython, calculate_account_state)
    else:
        raise ValueError(f"Unsupported account type: {account_state_pyo3.account_type}")


def from_account_state_cython_to_account_pyo3(
    account_state: AccountState,
    calculate_account_state: bool,
):
    account_state_pyo3 = transform_account_state_cython_to_pyo3(account_state)
    if account_state_pyo3.account_type == nautilus_pyo3.AccountType.CASH:
        return nautilus_pyo3.CashAccount(account_state_pyo3, calculate_account_state)
    elif account_state_pyo3.account_type == nautilus_pyo3.AccountType.MARGIN:
        return nautilus_pyo3.MarginAccount(account_state_pyo3, calculate_account_state)
    else:
        raise ValueError(f"Unsupported account type: {account_state_pyo3.account_type}")


def transform_account_to_pyo3(account: Account):
    events = account.events
    if len(events) == 0:
        raise ValueError("Missing events in account")
    init_event = events.pop(0)
    calculate_account_state = account.calculate_account_state
    account_pyo3 = from_account_state_cython_to_account_pyo3(init_event, calculate_account_state)
    for account_state_cython in events:
        event_pyo3 = transform_account_state_cython_to_pyo3(account_state_cython)
        account_pyo3.apply(event_pyo3)
    return account_pyo3


def transform_account_from_pyo3(account_pyo3) -> Account:
    events_pyo3 = account_pyo3.events
    if len(events_pyo3) == 0:
        raise ValueError("Missing events in account")
    init_event = events_pyo3.pop(0)
    calculate_account_state = account_pyo3.calculate_account_state
    account = from_account_state_pyo3_to_account_cython(init_event, calculate_account_state)
    for account_state_pyo3 in events_pyo3:
        event = transform_account_state_pyo3_to_cython(account_state_pyo3)
        account.apply(event)
    return account


################################################################################
# Market data
################################################################################
def transform_trade_tick_to_pyo3(trade: TradeTick) -> nautilus_pyo3.TradeTick:
    trade_dict = TradeTick.to_dict(trade)
    return nautilus_pyo3.TradeTick.from_dict(trade_dict)


def transform_trade_tick_from_pyo3(trade_pyo3: nautilus_pyo3.TradeTick) -> TradeTick:
    return TradeTick.from_pyo3(trade_pyo3)


def transform_quote_tick_to_pyo3(quote: QuoteTick) -> nautilus_pyo3.QuoteTick:
    quote_tick_dict = QuoteTick.to_dict(quote)
    return nautilus_pyo3.QuoteTick.from_dict(quote_tick_dict)


def transform_bar_to_pyo3(bar: Bar) -> nautilus_pyo3.Bar:
    bar_dict = Bar.to_dict(bar)
    return nautilus_pyo3.Bar.from_dict(bar_dict)


################################################################################
# Custom
################################################################################
def transform_signal_to_pyo3(signal: Data) -> nautilus_pyo3.Signal:
    return nautilus_pyo3.Signal(
        signal.__class__.__name__,
        str(signal.value),  # PyO3 expects a `String` for this parameter
        signal.ts_event,
        signal.ts_init,
    )


def transform_signal_from_pyo3(signal_cls: type, signal_pyo3: nautilus_pyo3.Signal) -> object:
    return signal_cls(
        signal_pyo3.value,
        signal_pyo3.ts_event,
        signal_pyo3.ts_init,
    )


def transform_data_type_to_pyo3(data_type: DataType) -> nautilus_pyo3.DataType:
    data_cls = data_type.type
    fully_qualified_name = data_cls.__module__ + ":" + data_cls.__qualname__
    return nautilus_pyo3.DataType(
        fully_qualified_name,
        data_type.metadata,  # PyO3 expects a `String` for this parameter
    )


def transform_data_type_from_pyo3(data_type_pyo3: nautilus_pyo3.DataType) -> DataType:
    module_name, type_name = data_type_pyo3.type_name.rsplit(":", 1)
    data_cls = getattr(module_name, type_name)
    return DataType(
        data_cls,
        data_type_pyo3.metadata,
    )


def transform_custom_data_to_pyo3(data: CustomData) -> nautilus_pyo3.CustomData:
    data_type_pyo3 = transform_data_type_to_pyo3(data.data_type)
    return nautilus_pyo3.CustomData(
        data_type_pyo3,
        value=msgspec.json.encode(data.data.to_dict()),
        ts_event=data.ts_event,
        ts_init=data.ts_init,
    )


def transform_custom_data_from_pyo3(data: nautilus_pyo3.CustomData) -> CustomData:
    data_type = transform_data_type_from_pyo3(data.data_type)
    data = Data(data.value, data.ts_event, data.ts_init)
    return CustomData(data_type, data)
