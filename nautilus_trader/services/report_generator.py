#!/usr/bin/env python3
"""
Report Generator
Generates backtest reports from engine results.
"""

from typing import Dict, Any, Optional
from pathlib import Path
import pandas as pd
import os
from nautilus_trader.backtest.engine import BacktestEngine


def generate_reports(
    engine: BacktestEngine,
    strategy_run_id: str,
    catalog_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Generate reports from backtest engine and save to files.
    
    Args:
        engine: The BacktestEngine instance
        strategy_run_id: The strategy run ID
        catalog_path: Optional path to catalog for saving reports
        
    Returns:
        Dictionary with report file paths (not content)
    """
    # Create reports directory
    reports_dir = os.path.join(os.getcwd(), "logs", "backtests", "reports", strategy_run_id)
    os.makedirs(reports_dir, exist_ok=True)
    
    reports = {
        "strategy_run_id": strategy_run_id,
        "status": "completed",
        "log_file_location": None,
        "orders_report": None,
        "order_fills_report": None,
        "fills_report": None,
        "positions_report": None,
        "account_report": None,
        "profit_loss_report": None,
        "performance_stats_pnls": None,
        "performance_stats_returns": None,
        "performance_stats_general": None,
    }
    
    try:
        # Generate reports using engine.trader methods (per Nautilus Trader docs)
        trader = engine.trader
        
        # Orders report
        try:
            orders_report = trader.generate_orders_report()
            if orders_report is not None and not orders_report.empty:
                report_path = os.path.join(reports_dir, "orders_report.csv")
                orders_report.to_csv(report_path, index=False)
                reports["orders_report"] = report_path
        except Exception as e:
            print(f"Error generating orders report: {e}", file=__import__('sys').stderr)
        
        # Order fills report
        try:
            order_fills_report = trader.generate_order_fills_report()
            if order_fills_report is not None and not order_fills_report.empty:
                report_path = os.path.join(reports_dir, "order_fills_report.csv")
                order_fills_report.to_csv(report_path, index=False)
                reports["order_fills_report"] = report_path
        except Exception as e:
            print(f"Error generating order fills report: {e}", file=__import__('sys').stderr)
        
        # Fills report
        try:
            fills_report = trader.generate_fills_report()
            if fills_report is not None and not fills_report.empty:
                report_path = os.path.join(reports_dir, "fills_report.csv")
                fills_report.to_csv(report_path, index=False)
                reports["fills_report"] = report_path
        except Exception as e:
            print(f"Error generating fills report: {e}", file=__import__('sys').stderr)
        
        # Positions report
        try:
            positions_report = trader.generate_positions_report()
            if positions_report is not None and not positions_report.empty:
                report_path = os.path.join(reports_dir, "positions_report.csv")
                positions_report.to_csv(report_path, index=False)
                reports["positions_report"] = report_path
        except Exception as e:
            print(f"Error generating positions report: {e}", file=__import__('sys').stderr)
        
        # Account report - need to get venue from portfolio or instruments
        try:
            # Get venue from portfolio - portfolio has account info with venue
            portfolio = engine.portfolio
            if portfolio:
                # Get accounts from portfolio
                accounts = engine.cache.accounts()
                if accounts:
                    # accounts() returns a dict-like object
                    if isinstance(accounts, dict) and len(accounts) > 0:
                        account = list(accounts.values())[0]
                    elif isinstance(accounts, list) and len(accounts) > 0:
                        account = accounts[0]
                    else:
                        account = None
                    
                    if account:
                        # AccountId format is typically "{VENUE}-001", extract venue from it
                        account_id_str = str(account.id)
                        if "-" in account_id_str:
                            venue_str = account_id_str.split("-")[0]
                            from nautilus_trader.model.identifiers import Venue
                            venue = Venue(venue_str)
                            account_report = trader.generate_account_report(venue)
                            if account_report is not None and not account_report.empty:
                                report_path = os.path.join(reports_dir, "account_report.csv")
                                account_report.to_csv(report_path, index=False)
                                reports["account_report"] = report_path
        except Exception as e:
            print(f"Error generating account report: {e}", file=__import__('sys').stderr)
            import traceback
            traceback.print_exc()
        
        # Performance stats - use engine.portfolio (per Nautilus Trader docs)
        try:
            portfolio = engine.portfolio
            if portfolio and portfolio.analyzer:
                analyzer = portfolio.analyzer
                
                # Get currency from instrument's quote currency (e.g., USD for XDG/USD)
                currency = None
                try:
                    # Get instruments from cache
                    instruments = engine.cache.instruments()
                    if instruments:
                        # Get first instrument (should be the one we're trading)
                        instrument = list(instruments.values())[0] if isinstance(instruments, dict) else instruments[0]
                        # For CurrencyPair, get quote_currency
                        if hasattr(instrument, 'quote_currency'):
                            currency = instrument.quote_currency
                except Exception as e:
                    print(f"Warning: Could not get currency from instrument: {e}", file=__import__('sys').stderr)
                
                # Fallback: try to get from portfolio accounts
                if currency is None:
                    try:
                        accounts = engine.cache.accounts()
                        if accounts:
                            account = list(accounts.values())[0] if isinstance(accounts, dict) else accounts[0]
                            # Get quote currency from account balances (look for USD, USDT, USDC, etc.)
                            if hasattr(account, 'balances'):
                                for balance in account.balances:
                                    # Prefer USD, USDT, USDC as quote currencies
                                    if balance.currency.code in ["USD", "USDT", "USDC"]:
                                        currency = balance.currency
                                        break
                    except Exception as e:
                        print(f"Warning: Could not get currency from accounts: {e}", file=__import__('sys').stderr)
                
                # Final fallback: use USD if still not found
                if currency is None:
                    from nautilus_trader.model.currency import Currency
                    currency = Currency.from_str("USD")
                    print(f"Warning: Using default USD currency for P&L stats", file=__import__('sys').stderr)
                
                # P&L stats - pass currency for multi-currency portfolios
                try:
                    pnl_stats = analyzer.get_performance_stats_pnls(currency=currency)
                    if pnl_stats is not None:
                        # Save to file
                        report_path = os.path.join(reports_dir, "performance_stats_pnls.txt")
                        with open(report_path, 'w') as f:
                            if isinstance(pnl_stats, pd.Series):
                                f.write(pnl_stats.to_string())
                            elif isinstance(pnl_stats, dict):
                                f.write(str(pnl_stats))
                            else:
                                f.write(str(pnl_stats))
                        reports["performance_stats_pnls"] = report_path
                except Exception as e:
                    print(f"Error generating P&L stats: {e}", file=__import__('sys').stderr)
                
                # Returns stats - does not accept currency parameter
                try:
                    returns_stats = analyzer.get_performance_stats_returns()
                    if returns_stats is not None:
                        report_path = os.path.join(reports_dir, "performance_stats_returns.txt")
                        with open(report_path, 'w') as f:
                            if isinstance(returns_stats, pd.Series):
                                f.write(returns_stats.to_string())
                            elif isinstance(returns_stats, dict):
                                f.write(str(returns_stats))
                            else:
                                f.write(str(returns_stats))
                        reports["performance_stats_returns"] = report_path
                except Exception as e:
                    print(f"Error generating returns stats: {e}", file=__import__('sys').stderr)
                
                # General stats
                try:
                    general_stats = analyzer.get_performance_stats_general()
                    if general_stats is not None:
                        report_path = os.path.join(reports_dir, "performance_stats_general.txt")
                        with open(report_path, 'w') as f:
                            if isinstance(general_stats, pd.Series):
                                f.write(general_stats.to_string())
                            elif isinstance(general_stats, dict):
                                f.write(str(general_stats))
                            else:
                                f.write(str(general_stats))
                        reports["performance_stats_general"] = report_path
                except Exception as e:
                    print(f"Error generating general stats: {e}", file=__import__('sys').stderr)
        except Exception as e:
            print(f"Error generating performance stats: {e}", file=__import__('sys').stderr)
            import traceback
            traceback.print_exc()
    
    except Exception as e:
        print(f"Error generating reports: {e}", file=__import__('sys').stderr)
        import traceback
        traceback.print_exc()
    
    return reports

