"""
Get YOUR personal trading volume for the last N days from Aster Finance DEX.

This script fetches your account's trade history and calculates your total trading volume
for a specified symbol over a configurable time period.

Usage:
    python get_my_trading_volume.py --symbol ETHUSDT --days 7
    python get_my_trading_volume.py --symbol BTCUSDT --days 30
    python get_my_trading_volume.py --days 7  # All symbols (top 40 by 24h volume)
"""

import argparse
import asyncio
import os
import time
from datetime import datetime

from dotenv import load_dotenv

from api_client import ApiClient

load_dotenv()

API_USER = os.getenv('API_USER')
API_SIGNER = os.getenv('API_SIGNER')
API_PRIVATE_KEY = os.getenv('API_PRIVATE_KEY')

DAY_MS = 24 * 60 * 60 * 1000
MAX_DAYS_PER_REQUEST = 7  # /fapi/v3/userTrades rejects startTime..endTime spans over 7 days
PAGE_LIMIT = 1000
TOP_SYMBOLS = 40


async def fetch_trades(client, symbol, start_time, end_time):
    """All of this account's trades for symbol in [start_time, end_time).

    Queries <=7-day windows; within a window, pages by fromId (which cannot
    be combined with time filters) until a short page or the window end.
    """
    trades = []
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + MAX_DAYS_PER_REQUEST * DAY_MS, end_time)
        params = {'symbol': symbol, 'limit': PAGE_LIMIT, 'startTime': chunk_start, 'endTime': chunk_end}
        while True:
            try:
                page = await client.signed_request("GET", "/fapi/v3/userTrades", params)
            except Exception as exc:
                print(f"[WARN] {symbol}: trade fetch failed ({exc}); {symbol} totals may be incomplete")
                break
            if not page:
                break
            trades.extend(t for t in page if chunk_start <= int(t['time']) < chunk_end)
            if len(page) < PAGE_LIMIT or int(page[-1]['time']) >= chunk_end:
                break
            params = {'symbol': symbol, 'limit': PAGE_LIMIT, 'fromId': page[-1]['id'] + 1}
            await asyncio.sleep(0.1)
        chunk_start = chunk_end
    return trades


def _new_bucket():
    return {'base_volume': 0.0, 'quote_volume': 0.0, 'trade_count': 0,
            'buy_volume': 0.0, 'sell_volume': 0.0, 'buy_count': 0, 'sell_count': 0}


def aggregate(trades_by_symbol):
    """Per-symbol and per-day (local date) volume buckets from {symbol: [trade, ...]}."""
    symbol_volumes, daily_volumes = {}, {}
    for symbol, trades in trades_by_symbol.items():
        for trade in trades:
            qty = float(trade['qty'])
            quote_qty = qty * float(trade['price'])
            day = datetime.fromtimestamp(int(trade['time']) / 1000).strftime('%Y-%m-%d')
            side = 'buy' if trade['side'] == 'BUY' else 'sell'
            for bucket in (symbol_volumes.setdefault(symbol, _new_bucket()), daily_volumes.setdefault(day, _new_bucket())):
                bucket['base_volume'] += qty
                bucket['quote_volume'] += quote_qty
                bucket['trade_count'] += 1
                bucket[f'{side}_volume'] += quote_qty
                bucket[f'{side}_count'] += 1
    return symbol_volumes, daily_volumes


async def top_symbols(client):
    """The TOP_SYMBOLS most traded symbols by 24h quote volume."""
    async with client.session.get(f"{client.base_url}/fapi/v1/ticker/24hr") as response:
        response.raise_for_status()
        tickers = await response.json()
    ranked = sorted(tickers, key=lambda t: float(t.get('quoteVolume', 0)), reverse=True)
    print(f"[INFO] Checking the top {TOP_SYMBOLS} of {len(ranked)} symbols by 24h volume "
          f"(trades on other symbols are not counted)")
    return [t['symbol'] for t in ranked[:TOP_SYMBOLS]]


def print_report(days, start_time, end_time, symbol_volumes, daily_volumes):
    total_quote_volume = sum(v['quote_volume'] for v in symbol_volumes.values())
    total_trades = sum(v['trade_count'] for v in symbol_volumes.values())

    print()
    print("=" * 70)
    print("YOUR TRADING VOLUME REPORT")
    print("=" * 70)
    print(f"Period: {days} days")
    print(f"From:   {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"To:     {datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if not symbol_volumes:
        print("[INFO] No trades found in the specified period")
        print("=" * 70)
        return

    print("DAILY BREAKDOWN:")
    print("=" * 70)
    print(f"{'Date':<12} {'Volume ($)':>15} {'Trades':>10} {'Buy ($)':>15} {'Sell ($)':>15}")
    print("-" * 70)
    for date in sorted(daily_volumes):
        vol = daily_volumes[date]
        print(f"{date:<12} ${vol['quote_volume']:>14,.2f} {vol['trade_count']:>10,} "
              f"${vol['buy_volume']:>14,.2f} ${vol['sell_volume']:>14,.2f}")
    print("-" * 70)
    print(f"{'TOTAL':<12} ${total_quote_volume:>14,.2f} {total_trades:>10,} "
          f"${sum(v['buy_volume'] for v in daily_volumes.values()):>14,.2f} "
          f"${sum(v['sell_volume'] for v in daily_volumes.values()):>14,.2f}")
    print("=" * 70)
    print()

    print("BREAKDOWN BY SYMBOL:")
    print("-" * 70)
    for sym, vol in sorted(symbol_volumes.items(), key=lambda x: x[1]['quote_volume'], reverse=True):
        base_asset = sym[:-4] if sym.endswith('USDT') else sym
        print(f"\n{sym}:")
        print(f"  Base Volume:   {vol['base_volume']:,.4f} {base_asset}")
        print(f"  Quote Volume:  ${vol['quote_volume']:,.2f}")
        print(f"  Trade Count:   {vol['trade_count']:,}")
        print(f"  Buy Volume:    ${vol['buy_volume']:,.2f} ({vol['buy_count']} trades)")
        print(f"  Sell Volume:   ${vol['sell_volume']:,.2f} ({vol['sell_count']} trades)")

    print()
    print("-" * 70)
    print("SUMMARY:")
    print(f"  Total Quote Volume: ${total_quote_volume:,.2f}")
    print(f"  Total Trades:       {total_trades:,}")
    print(f"  Daily Average:      ${total_quote_volume / days:,.2f}/day ({total_trades / days:,.0f} trades/day)")
    print("=" * 70)


async def get_my_trading_volume(symbol: str = None, days: int = 7):
    """Fetch and print YOUR trading volume for the last N days (one symbol, or the top 40)."""
    end_time = int(time.time() * 1000)
    start_time = end_time - days * DAY_MS

    print("[INFO] Fetching YOUR trading volume")
    print(f"[INFO] Symbol: {symbol or 'ALL'}")
    print(f"[INFO] Period: {datetime.fromtimestamp(start_time/1000)} to {datetime.fromtimestamp(end_time/1000)}")
    print(f"[INFO] Days: {days}")
    print()

    async with ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY) as client:
        symbols = [symbol] if symbol else await top_symbols(client)
        results = await asyncio.gather(*(fetch_trades(client, sym, start_time, end_time) for sym in symbols))

    trades_by_symbol = dict(zip(symbols, results))
    for sym, trades in trades_by_symbol.items():
        if trades:
            print(f"[INFO] {sym}: {len(trades)} trades")
    print_report(days, start_time, end_time, *aggregate(trades_by_symbol))


async def main():
    parser = argparse.ArgumentParser(description='Get YOUR perpetual futures trading volume for the last N days')
    parser.add_argument('--symbol', type=str, default=None, help='Trading pair symbol (default: all symbols)')
    parser.add_argument('--days', type=int, default=7, help='Number of days to look back (default: 7)')
    args = parser.parse_args()

    if args.days <= 0:
        print("[ERROR] days must be greater than 0")
        return
    if args.days > 365:
        print("[WARN] Fetching more than 365 days of data may take a long time")

    try:
        await get_my_trading_volume(args.symbol.upper() if args.symbol else None, args.days)
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())
