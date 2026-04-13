import ccxt
import pandas as pd
import time

def get_binance_data(symbol="BTC/USDT", interval="5m",
                     start_date="2026-01-01 00:00:00",
                     end_date="2026-01-31 23:59:59"):
    print(f"{symbol} (바이낸스) {interval} 데이터 수집 중 ({start_date} ~ {end_date})...")
    try:
        exchange      = ccxt.binance()
        since         = exchange.parse8601(start_date.replace(" ", "T") + "Z")
        end_timestamp = exchange.parse8601(end_date.replace(" ", "T") + "Z")
        all_ohlcv     = []
        while True:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=interval, since=since, limit=1000)
            if not ohlcv: break
            for candle in ohlcv:
                if candle[0] <= end_timestamp: all_ohlcv.append(candle)
            if ohlcv[-1][0] >= end_timestamp: break
            since = ohlcv[-1][0] + 1
            time.sleep(0.1)
        if not all_ohlcv: return pd.DataFrame()
        df = pd.DataFrame(all_ohlcv, columns=['Timestamp','Open','High','Low','Close','Volume'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
        df.set_index('Timestamp', inplace=True)
        return df[~df.index.duplicated(keep='last')]
    except Exception as e:
        print(f"데이터 로드 오류: {e}")
        return pd.DataFrame()

def get_binance_daily_data(symbol="BTC/USDT",
                           start_date="2025-06-01 00:00:00",
                           end_date="2026-01-31 23:59:59"):
    print(f"{symbol} (바이낸스) 일봉 데이터 로드 중 ({start_date} ~ {end_date})...")
    try:
        exchange      = ccxt.binance()
        since         = exchange.parse8601(start_date.replace(" ", "T") + "Z")
        end_timestamp = exchange.parse8601(end_date.replace(" ", "T") + "Z")
        all_ohlcv     = []
        while True:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', since=since, limit=1000)
            if not ohlcv: break
            for candle in ohlcv:
                if candle[0] <= end_timestamp: all_ohlcv.append(candle)
            if ohlcv[-1][0] >= end_timestamp: break
            since = ohlcv[-1][0] + 1
            time.sleep(0.1)
        if not all_ohlcv: return pd.DataFrame()
        df = pd.DataFrame(all_ohlcv, columns=['Timestamp','Open','High','Low','Close','Volume'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
        df.set_index('Timestamp', inplace=True)
        return df[~df.index.duplicated(keep='last')]
    except Exception as e:
        print(f"일봉 데이터 로드 오류: {e}")
        return pd.DataFrame()