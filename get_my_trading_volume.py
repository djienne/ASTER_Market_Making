"""
Get YOUR personal trading volume for the last N days from Aster Finance DEX.

This script fetches your account's trade history and calculates your total trading volume
for a specified symbol over a configurable time period.

Usage:
    python get_my_trading_volume.py --symbol ETHUSDT --days 7
    python get_my_trading_volume.py --symbol BTCUSDT --days 30
    python get_my_trading_volume.py --days 7  # All symbols
"""

import asyncio
import argparse
import time
from datetime import datetime
from api_client import ApiClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API credentials
API_USER = os.getenv('API_USER')
API_SIGNER = os.getenv('API_SIGNER')
API_PRIVATE_KEY = os.getenv('API_PRIVATE_KEY')


async def get_my_trading_volume(symbol: str = None, days: int = 7):
    """
    Fetch YOUR trading volume for the last N days.

    Args:
        symbol: Trading pair symbol (e.g., 'ETHUSDT'), or None for all symbols
        days: Number of days to look back (default: 7)

    Returns:
        dict: Your volume statistics including base volume, quote volume, and trade count
    """
    client = ApiClient(API_USER, API_SIGNER, API_PRIVATE_KEY)

    # Calculate time range
    end_time = int(time.time() * 1000)  # Current time in milliseconds
    start_time = end_time - (days * 24 * 60 * 60 * 1000)  # N days ago

    print(f"[INFO] Fetching YOUR trading volume")
    if symbol:
        print(f"[INFO] Symbol: {symbol}")
    else:
        print(f"[INFO] Symbol: ALL")
    print(f"[INFO] Period: {datetime.fromtimestamp(start_time/1000)} to {datetime.fromtimestamp(end_time/1000)}")
    print(f"[INFO] Days: {days}")
    print()

    # Track volumes by symbol and by day
    symbol_volumes = {}
    daily_volumes = {}  # day_key -> {quote_volume, trade_count, buy_volume, sell_volume}
    total_quote_volume = 0.0
    total_trades = 0

    async with client:
        try:
            # Fetch account trade list
            # API endpoint: GET /fapi/v3/userTrades
            url = f"{client.base_url}/fapi/v3/userTrades"

            if symbol:
                # Get trades for specific symbol with pagination
                # Note: API has a 7-day maximum for startTime/endTime queries
                all_trades = []

                # Break into 7-day chunks if needed
                MAX_DAYS_PER_REQUEST = 7
                if days > MAX_DAYS_PER_REQUEST:
                    print(f"[INFO] Period exceeds {MAX_DAYS_PER_REQUEST} days, will fetch in chunks...")

                    # Calculate chunks
                    chunk_size_ms = MAX_DAYS_PER_REQUEST * 24 * 60 * 60 * 1000
                    current_start = start_time

                    while current_start < end_time:
                        current_end = min(current_start + chunk_size_ms, end_time)

                        print(f"[INFO] Fetching chunk: {datetime.fromtimestamp(current_start/1000)} to {datetime.fromtimestamp(current_end/1000)}")

                        # Fetch this chunk with pagination
                        from_id = None
                        batch = 0

                        while True:
                            batch += 1
                            params = {
                                'symbol': symbol,
                                'limit': 1000
                            }

                            if from_id:
                                params['fromId'] = from_id
                            else:
                                params['startTime'] = current_start
                                params['endTime'] = current_end

                            signed_params = client._sign(params)

                            async with client.session.get(url, params=signed_params) as response:
                                if response.status != 200:
                                    error_text = await response.text()
                                    print(f"[ERROR] API returned status {response.status}: {error_text}")
                                    break

                                chunk_trades = await response.json()

                                if not chunk_trades:
                                    break

                                # Filter by chunk time range
                                filtered_trades = [t for t in chunk_trades if current_start <= int(t['time']) < current_end]
                                all_trades.extend(filtered_trades)

                                print(f"[INFO] Chunk batch {batch}: Fetched {len(chunk_trades)} trades ({len(filtered_trades)} in range)")

                                if len(chunk_trades) < 1000:
                                    break

                                from_id = chunk_trades[-1]['id'] + 1

                                if int(chunk_trades[-1]['time']) >= current_end:
                                    break

                                await asyncio.sleep(0.1)

                        # Move to next chunk
                        current_start = current_end
                        await asyncio.sleep(0.1)

                    print(f"[INFO] Total: {len(all_trades)} trades for {symbol}")
                    trades = all_trades

                else:
                    # Single request for <= 7 days
                    from_id = None
                    batch = 0

                    # Pagination loop - keep fetching until we get all trades
                    while True:
                        batch += 1
                        params = {
                            'symbol': symbol,
                            'limit': 1000
                        }

                        # Use fromId for pagination (not compatible with time filters)
                        if from_id:
                            params['fromId'] = from_id
                        else:
                            # First request - use time filters
                            params['startTime'] = start_time
                            params['endTime'] = end_time

                        signed_params = client._sign(params)

                        async with client.session.get(url, params=signed_params) as response:
                            if response.status != 200:
                                error_text = await response.text()
                                print(f"[ERROR] API returned status {response.status}: {error_text}")
                                break

                            trades = await response.json()

                            if not trades:
                                break

                            # Filter trades by time range (important when using fromId)
                            # Use < for end_time to avoid boundary issues
                            filtered_trades = [t for t in trades if start_time <= int(t['time']) < end_time]
                            all_trades.extend(filtered_trades)

                            print(f"[INFO] Batch {batch}: Fetched {len(trades)} trades ({len(filtered_trades)} in time range)")

                            # If we got less than 1000, we're done
                            if len(trades) < 1000:
                                break

                            # Update fromId for next batch
                            from_id = trades[-1]['id'] + 1

                            # Check if last trade is beyond our end time
                            if int(trades[-1]['time']) >= end_time:
                                break

                            await asyncio.sleep(0.1)

                    print(f"[INFO] Total: {len(all_trades)} trades for {symbol}")
                    trades = all_trades

                # Process trades
                for trade in trades:
                    qty = float(trade['qty'])
                    price = float(trade['price'])
                    quote_qty = qty * price
                    trade_time = int(trade['time'])

                    # Get the date for this trade
                    trade_date = datetime.fromtimestamp(trade_time / 1000).strftime('%Y-%m-%d')

                    if symbol not in symbol_volumes:
                        symbol_volumes[symbol] = {
                            'base_volume': 0.0,
                            'quote_volume': 0.0,
                            'trade_count': 0,
                            'buy_volume': 0.0,
                            'sell_volume': 0.0,
                            'buy_count': 0,
                            'sell_count': 0
                        }

                    symbol_volumes[symbol]['base_volume'] += qty
                    symbol_volumes[symbol]['quote_volume'] += quote_qty
                    symbol_volumes[symbol]['trade_count'] += 1
                    total_quote_volume += quote_qty
                    total_trades += 1

                    # Track by day
                    if trade_date not in daily_volumes:
                        daily_volumes[trade_date] = {
                            'quote_volume': 0.0,
                            'trade_count': 0,
                            'buy_volume': 0.0,
                            'sell_volume': 0.0,
                            'buy_count': 0,
                            'sell_count': 0
                        }

                    daily_volumes[trade_date]['quote_volume'] += quote_qty
                    daily_volumes[trade_date]['trade_count'] += 1

                    # Track buy vs sell
                    if trade['side'] == 'BUY':
                        symbol_volumes[symbol]['buy_volume'] += quote_qty
                        symbol_volumes[symbol]['buy_count'] += 1
                        daily_volumes[trade_date]['buy_volume'] += quote_qty
                        daily_volumes[trade_date]['buy_count'] += 1
                    else:
                        symbol_volumes[symbol]['sell_volume'] += quote_qty
                        symbol_volumes[symbol]['sell_count'] += 1
                        daily_volumes[trade_date]['sell_volume'] += quote_qty
                        daily_volumes[trade_date]['sell_count'] += 1

            else:
                # Get trades for top symbols by volume
                # Get 24hr tickers to find most traded symbols
                ticker_url = f"{client.base_url}/fapi/v1/ticker/24hr"

                print(f"[INFO] Fetching top symbols by 24hr volume...")

                async with client.session.get(ticker_url) as response:
                    if response.status == 200:
                        tickers = await response.json()
                        # Sort by 24hr quote volume (descending)
                        sorted_tickers = sorted(tickers, key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)
                        # Take top 40 symbols
                        top_symbols = [t['symbol'] for t in sorted_tickers[:40]]
                        print(f"[INFO] Checking the top 40 of {len(sorted_tickers)} symbols by 24h volume (trades on other symbols are not counted): {', '.join(top_symbols[:10])}...")
                    else:
                        print(f"[WARN] Could not fetch tickers, using common symbols instead")
                        top_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT',
                                      'ADAUSDT', 'DOGEUSDT', 'MATICUSDT', 'DOTUSDT', 'AVAXUSDT',
                                      'LINKUSDT', 'ATOMUSDT', 'UNIUSDT', 'LTCUSDT', 'ETCUSDT',
                                      'FILUSDT', 'TRXUSDT', 'XLMUSDT', 'VETUSDT', 'ICPUSDT']

                print(f"[INFO] Checking {len(top_symbols)} top symbols for your trades...")

                # Define async function to fetch trades for a single symbol
                async def fetch_symbol_trades(sym):
                    """Fetch all trades for a single symbol."""
                    try:
                        all_trades_for_symbol = []

                        # Break into 7-day chunks if needed
                        MAX_DAYS_PER_REQUEST = 7
                        if days > MAX_DAYS_PER_REQUEST:
                            chunk_size_ms = MAX_DAYS_PER_REQUEST * 24 * 60 * 60 * 1000
                            current_start = start_time

                            while current_start < end_time:
                                current_end = min(current_start + chunk_size_ms, end_time)

                                from_id = None
                                while True:
                                    params = {
                                        'symbol': sym,
                                        'limit': 1000
                                    }

                                    if from_id:
                                        params['fromId'] = from_id
                                    else:
                                        params['startTime'] = current_start
                                        params['endTime'] = current_end

                                    signed_params = client._sign(params)

                                    async with client.session.get(url, params=signed_params) as response:
                                        if response.status == 200:
                                            trades = await response.json()

                                            if not trades:
                                                break

                                            filtered_trades = [t for t in trades if current_start <= int(t['time']) < current_end]
                                            all_trades_for_symbol.extend(filtered_trades)

                                            if len(trades) < 1000:
                                                break

                                            from_id = trades[-1]['id'] + 1

                                            if int(trades[-1]['time']) >= current_end:
                                                break

                                            await asyncio.sleep(0.05)
                                        else:
                                            print(f"[WARN] {sym}: HTTP {response.status} while fetching trades; {sym} totals may be incomplete")
                                            break

                                current_start = current_end
                                await asyncio.sleep(0.05)
                        else:
                            # Single fetch for <= 7 days
                            from_id = None
                            batch = 0

                            while True:
                                batch += 1
                                params = {
                                    'symbol': sym,
                                    'limit': 1000
                                }

                                if from_id:
                                    params['fromId'] = from_id
                                else:
                                    params['startTime'] = start_time
                                    params['endTime'] = end_time

                                signed_params = client._sign(params)

                                async with client.session.get(url, params=signed_params) as response:
                                    if response.status == 200:
                                        trades = await response.json()

                                        if not trades:
                                            break

                                        # Filter by time range
                                        filtered_trades = [t for t in trades if start_time <= int(t['time']) < end_time]
                                        all_trades_for_symbol.extend(filtered_trades)

                                        # If we got less than 1000, we're done
                                        if len(trades) < 1000:
                                            break

                                        # Update fromId for next batch
                                        from_id = trades[-1]['id'] + 1

                                        # Check if last trade is beyond our end time
                                        if int(trades[-1]['time']) >= end_time:
                                            break

                                        await asyncio.sleep(0.05)
                                    else:
                                        print(f"[WARN] {sym}: HTTP {response.status} while fetching trades; {sym} totals may be incomplete")
                                        break

                        return sym, all_trades_for_symbol

                    except Exception as e:
                        print(f"[WARN] {sym}: trade fetch failed ({e}); {sym} is missing from the totals")
                        return sym, []

                # Fetch all symbols concurrently
                results = await asyncio.gather(*[fetch_symbol_trades(sym) for sym in top_symbols])

                # Process results
                for sym, trades in results:
                    if trades:
                        print(f"[INFO] Found {len(trades)} trades for {sym}")

                        for trade in trades:
                            qty = float(trade['qty'])
                            price = float(trade['price'])
                            quote_qty = qty * price
                            trade_time = int(trade['time'])

                            # Get the date for this trade
                            trade_date = datetime.fromtimestamp(trade_time / 1000).strftime('%Y-%m-%d')

                            if sym not in symbol_volumes:
                                symbol_volumes[sym] = {
                                    'base_volume': 0.0,
                                    'quote_volume': 0.0,
                                    'trade_count': 0,
                                    'buy_volume': 0.0,
                                    'sell_volume': 0.0,
                                    'buy_count': 0,
                                    'sell_count': 0
                                }

                            symbol_volumes[sym]['base_volume'] += qty
                            symbol_volumes[sym]['quote_volume'] += quote_qty
                            symbol_volumes[sym]['trade_count'] += 1
                            total_quote_volume += quote_qty
                            total_trades += 1

                            # Track by day
                            if trade_date not in daily_volumes:
                                daily_volumes[trade_date] = {
                                    'quote_volume': 0.0,
                                    'trade_count': 0,
                                    'buy_volume': 0.0,
                                    'sell_volume': 0.0,
                                    'buy_count': 0,
                                    'sell_count': 0
                                }

                            daily_volumes[trade_date]['quote_volume'] += quote_qty
                            daily_volumes[trade_date]['trade_count'] += 1

                            # Track buy vs sell
                            if trade['side'] == 'BUY':
                                symbol_volumes[sym]['buy_volume'] += quote_qty
                                symbol_volumes[sym]['buy_count'] += 1
                                daily_volumes[trade_date]['buy_volume'] += quote_qty
                                daily_volumes[trade_date]['buy_count'] += 1
                            else:
                                symbol_volumes[sym]['sell_volume'] += quote_qty
                                symbol_volumes[sym]['sell_count'] += 1
                                daily_volumes[trade_date]['sell_volume'] += quote_qty
                                daily_volumes[trade_date]['sell_count'] += 1

        except Exception as e:
            print(f"[ERROR] Error fetching trades: {e}")
            import traceback
            traceback.print_exc()
            return None

    # Calculate averages
    avg_daily_quote_volume = total_quote_volume / days if days > 0 else 0
    avg_daily_trades = total_trades / days if days > 0 else 0

    # Display results
    print()
    print("=" * 70)
    print(f"YOUR TRADING VOLUME REPORT")
    print("=" * 70)
    print(f"Period: {days} days")
    print(f"From:   {datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"To:     {datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if not symbol_volumes:
        print("[INFO] No trades found in the specified period")
        print("=" * 70)
        return None

    # Display daily breakdown table
    print("DAILY BREAKDOWN:")
    print("=" * 70)
    print(f"{'Date':<12} {'Volume ($)':>15} {'Trades':>10} {'Buy ($)':>15} {'Sell ($)':>15}")
    print("-" * 70)

    # Sort dates chronologically
    sorted_dates = sorted(daily_volumes.keys())

    for date in sorted_dates:
        vol = daily_volumes[date]
        print(f"{date:<12} ${vol['quote_volume']:>14,.2f} {vol['trade_count']:>10,} "
              f"${vol['buy_volume']:>14,.2f} ${vol['sell_volume']:>14,.2f}")

    print("-" * 70)
    print(f"{'TOTAL':<12} ${total_quote_volume:>14,.2f} {total_trades:>10,} "
          f"${sum(v['buy_volume'] for v in daily_volumes.values()):>14,.2f} "
          f"${sum(v['sell_volume'] for v in daily_volumes.values()):>14,.2f}")
    print("=" * 70)
    print()

    # Display per-symbol breakdown
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
    print(f"  Daily Average:      ${avg_daily_quote_volume:,.2f}/day ({avg_daily_trades:,.0f} trades/day)")
    print("=" * 70)

    return {
        'symbol_volumes': symbol_volumes,
        'total_quote_volume': total_quote_volume,
        'total_trades': total_trades,
        'avg_daily_quote_volume': avg_daily_quote_volume,
        'avg_daily_trades': avg_daily_trades,
        'days': days
    }


async def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Get YOUR perpetual futures trading volume for the last N days'
    )
    parser.add_argument(
        '--symbol',
        type=str,
        default=None,
        help='Trading pair symbol (default: all symbols)'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Number of days to look back (default: 7)'
    )

    args = parser.parse_args()

    # Validate inputs
    if args.days <= 0:
        print("[ERROR] days must be greater than 0")
        return

    if args.days > 365:
        print("[WARN] Fetching more than 365 days of data may take a long time")

    try:
        symbol = args.symbol.upper() if args.symbol else None
        await get_my_trading_volume(symbol, args.days)
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user")
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())