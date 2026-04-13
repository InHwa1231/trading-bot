"""
Parker Brooks 스타일 5분봉 스캘핑 지표
VWAP + EMA + Volume Profile (최적화 버전)

5분봉 특화:
- EMA 9/21/50 (단기 반응)
- VWAP 일간 리셋
- Volume Profile: 48봉 롤링 (4시간, numpy 최적화)
"""
import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange


def calc_volume_profile_fast(highs, lows, closes, volumes, lookback=48, n_bins=15):
    """
    최적화된 Volume Profile (numpy 벡터화)
    - typical price 기반 빈 할당 (O(n) per window)
    """
    n = len(closes)
    poc = np.full(n, np.nan)
    vah = np.full(n, np.nan)
    val_arr = np.full(n, np.nan)

    tp = ((highs.values + lows.values + closes.values) / 3)
    vol = volumes.values

    for i in range(lookback, n):
        w_tp = tp[i - lookback:i]
        w_vol = vol[i - lookback:i]
        w_high = highs.values[i - lookback:i]
        w_low = lows.values[i - lookback:i]

        price_min = w_low.min()
        price_max = w_high.max()

        if price_max <= price_min or price_max == 0:
            continue

        # bin 할당 (typical price 기반)
        bin_edges = np.linspace(price_min, price_max, n_bins + 1)
        bin_idx = np.clip(np.digitize(w_tp, bin_edges) - 1, 0, n_bins - 1)

        # bin별 볼륨 합산
        bin_volumes = np.zeros(n_bins)
        np.add.at(bin_volumes, bin_idx, w_vol)

        # POC
        poc_bin = np.argmax(bin_volumes)
        poc[i] = (bin_edges[poc_bin] + bin_edges[poc_bin + 1]) / 2

        # Value Area (70%)
        total_vol = bin_volumes.sum()
        if total_vol == 0:
            continue

        va_target = total_vol * 0.70
        va_vol = bin_volumes[poc_bin]
        lo, hi = poc_bin, poc_bin

        while va_vol < va_target and (lo > 0 or hi < n_bins - 1):
            exp_lo = bin_volumes[lo - 1] if lo > 0 else 0
            exp_hi = bin_volumes[hi + 1] if hi < n_bins - 1 else 0
            if exp_lo >= exp_hi and lo > 0:
                lo -= 1
                va_vol += bin_volumes[lo]
            elif hi < n_bins - 1:
                hi += 1
                va_vol += bin_volumes[hi]
            elif lo > 0:
                lo -= 1
                va_vol += bin_volumes[lo]
            else:
                break

        vah[i] = bin_edges[hi + 1]
        val_arr[i] = bin_edges[lo]

    return (pd.Series(poc, index=closes.index),
            pd.Series(vah, index=closes.index),
            pd.Series(val_arr, index=closes.index))


def calc_vwap_daily(closes, highs, lows, volumes, index):
    """일간 리셋 VWAP"""
    tp = (highs + lows + closes) / 3
    vwap = np.full(len(closes), np.nan)

    cum_tp_vol = 0.0
    cum_vol = 0.0
    prev_date = None

    for i in range(len(closes)):
        cur_date = index[i].date()
        if cur_date != prev_date:
            cum_tp_vol = 0.0
            cum_vol = 0.0
            prev_date = cur_date

        cum_tp_vol += tp.iloc[i] * volumes.iloc[i]
        cum_vol += volumes.iloc[i]
        if cum_vol > 0:
            vwap[i] = cum_tp_vol / cum_vol

    return pd.Series(vwap, index=index)


def calc_signals_for_asset(data_5m, asset_name="BTC"):
    """5분봉 Parker Brooks 스타일 시그널"""
    pd.set_option('future.no_silent_downcasting', True)

    df = data_5m.copy()
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']

    # ── EMA (5분봉 특화) ──
    df['ema9'] = EMAIndicator(c, 9).ema_indicator()      # 45분
    df['ema21'] = EMAIndicator(c, 21).ema_indicator()     # 1.75시간
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()     # 4.2시간

    # ── ADX & ATR ──
    df['adx'] = ADXIndicator(h, l, c, 14).adx()
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()

    # ── VWAP ──
    df['vwap'] = calc_vwap_daily(c, h, l, v, df.index)

    # ── Volume Profile (48봉 = 4시간) ──
    print(f"    {asset_name}: Volume Profile 계산중...")
    df['poc'], df['vah'], df['val'] = calc_volume_profile_fast(h, l, c, v, lookback=48)

    # ── 추세 판단 ──
    ema_bull = df['ema21'] > df['ema50']
    ema_bear = df['ema21'] < df['ema50']

    # ── 방향성 ──
    price_above_vwap = c > df['vwap']
    price_below_vwap = c < df['vwap']

    # ── VP 근접도 (5분봉은 좀 더 타이트) ──
    near_val = (c - df['val']).abs() / c * 100 < 0.8
    near_poc = (c - df['poc']).abs() / c * 100 < 0.5
    near_vah = (c - df['vah']).abs() / c * 100 < 0.8

    # ── 볼륨 확인 ──
    vol_ma = v.rolling(48).mean()
    vol_above_avg = v > vol_ma * 0.8

    # ── EMA9 방향 (단기 모멘텀) ──
    ema9_rising = df['ema9'] > df['ema9'].shift(2)
    ema9_falling = df['ema9'] < df['ema9'].shift(2)

    # ═══════════════════════════════════
    # 롱 시그널
    # ═══════════════════════════════════
    # 타입1: VWAP 위 + VAL 지지 반등
    long_t1 = (
        ema_bull & price_above_vwap & near_val &
        ema9_rising & vol_above_avg
    )
    # 타입2: POC 지지 + VWAP 위
    long_t2 = (
        ema_bull & price_above_vwap & near_poc &
        (c > c.shift(1)) & (df['adx'] > 18)
    )
    # 타입3: VAH 돌파
    long_t3 = (
        ema_bull & price_above_vwap &
        (c > df['vah']) & (c.shift(1) <= df['vah'].shift(1)) &
        (df['adx'] > 20) & vol_above_avg
    )

    df['raw_long_buy'] = long_t1 | long_t2 | long_t3
    df['raw_long_exit'] = (
        (c < df['vwap']) & (c < df['poc'])
    ) | ema_bear

    # ═══════════════════════════════════
    # 숏 시그널
    # ═══════════════════════════════════
    short_t1 = (
        ema_bear & price_below_vwap & near_vah &
        ema9_falling & vol_above_avg
    )
    short_t2 = (
        ema_bear & price_below_vwap & near_poc &
        (c < c.shift(1)) & (df['adx'] > 18)
    )
    short_t3 = (
        ema_bear & price_below_vwap &
        (c < df['val']) & (c.shift(1) >= df['val'].shift(1)) &
        (df['adx'] > 20) & vol_above_avg
    )

    df['raw_short_sell'] = short_t1 | short_t2 | short_t3
    df['raw_short_exit'] = (
        (c > df['vwap']) & (c > df['poc'])
    ) | ema_bull

    # shift 1봉 (lookahead 방지)
    for sig in ['long_buy', 'long_exit', 'short_sell', 'short_exit']:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    df['Open'] = data_5m['Open']
    df['trend_bull'] = ema_bull.shift(1).fillna(False)
    df['trend_bear'] = ema_bear.shift(1).fillna(False)
    df['Index_Num'] = range(len(df))
    df['asset'] = asset_name

    return df
