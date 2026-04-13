# indicators/market_regime.py [V28]
#
# [V28 변경]
# - ADX 기준 18 → 낮춰 RANGE 과잉 분류 방지
# - BULL_WEAK / BEAR_WEAK 기준 완화 (score 4 이상)
# - 할당 가중치: e120/e180 추가 점수화
# - 변동성 필터 추가: ATR 비율로 RANGE 내에서 극저변동 필터

import pandas as pd
from ta.trend import EMAIndicator, MACD, ADXIndicator
from ta.volatility import AverageTrueRange


def detect_market_regime(daily_df, as_of_date):
    df = daily_df.copy()
    if len(df) < 60:
        return "RANGE"

    close = df['Close']
    high  = df['High']
    low   = df['Low']

    e7_s   = EMAIndicator(close=close, window=7).ema_indicator()
    e14_s  = EMAIndicator(close=close, window=14).ema_indicator()
    e30_s  = EMAIndicator(close=close, window=30).ema_indicator()
    e60_s  = EMAIndicator(close=close, window=60).ema_indicator()
    e120_s = EMAIndicator(close=close, window=120).ema_indicator()
    e180_s = EMAIndicator(close=close, window=180).ema_indicator()

    macd_hist = MACD(close=close, window_slow=26, window_fast=12,
                     window_sign=9).macd_diff()
    adx_obj   = ADXIndicator(high=high, low=low, close=close, window=14)
    adx_line  = adx_obj.adx()
    plus_di   = adx_obj.adx_pos()
    minus_di  = adx_obj.adx_neg()

    # [V28] ATR 비율 (일봉 변동성)
    atr_s   = AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range()

    mask = df.index.date <= as_of_date
    if mask.sum() < 30:
        return "RANGE"

    idx = df[mask].index[-1]

    e7   = e7_s[idx];   e14  = e14_s[idx];  e30  = e30_s[idx]
    e60  = e60_s[idx];  e120 = e120_s[idx]; e180 = e180_s[idx]
    mh   = macd_hist[idx]
    daily_adx   = adx_line[idx]
    daily_plus  = plus_di[idx]
    daily_minus = minus_di[idx]

    prev_list = df[mask].index
    prev_idx  = prev_list[-7] if len(prev_list) >= 7 else prev_list[0]
    e30_prev  = e30_s[prev_idx]
    e60_prev  = e60_s[prev_idx]

    bull_score = sum([
        e7 > e14, e14 > e30, e30 > e60, e60 > e120, e120 > e180,
        e30 > e30_prev, e60 > e60_prev, mh > 0
    ])
    bear_score = sum([
        e7 < e14, e14 < e30, e30 < e60, e60 < e120, e120 < e180,
        e30 < e30_prev, e60 < e60_prev, mh < 0
    ])

    # [V28] ADX 임계값 20으로 낮춤 (18이면 너무 민감, 22면 너무 보수적)
    if daily_adx < 20:
        return "RANGE"

    if bull_score >= 7 and daily_plus > daily_minus:
        return "BULL"
    elif bull_score >= 5 and daily_plus > daily_minus:
        return "BULL_WEAK"
    elif bear_score >= 7 and daily_minus > daily_plus:
        return "BEAR"
    elif bear_score >= 5 and daily_minus > daily_plus:
        return "BEAR_WEAK"
    else:
        return "RANGE"


def get_regime_debug(daily_df, as_of_date):
    df = daily_df.copy()
    if len(df) < 60:
        return "RANGE", {}

    close = df['Close']
    high  = df['High']
    low   = df['Low']

    e7_s   = EMAIndicator(close=close, window=7).ema_indicator()
    e14_s  = EMAIndicator(close=close, window=14).ema_indicator()
    e30_s  = EMAIndicator(close=close, window=30).ema_indicator()
    e60_s  = EMAIndicator(close=close, window=60).ema_indicator()
    e120_s = EMAIndicator(close=close, window=120).ema_indicator()
    e180_s = EMAIndicator(close=close, window=180).ema_indicator()

    macd_h   = MACD(close=close, window_slow=26, window_fast=12,
                    window_sign=9).macd_diff()
    adx_obj  = ADXIndicator(high=high, low=low, close=close, window=14)
    adx_line = adx_obj.adx()
    plus_di  = adx_obj.adx_pos()
    minus_di = adx_obj.adx_neg()

    mask = df.index.date <= as_of_date
    if mask.sum() < 30:
        return "RANGE", {}

    idx      = df[mask].index[-1]
    prev_idx = df[mask].index[-7] if len(df[mask]) >= 7 else df[mask].index[0]

    e7   = e7_s[idx];   e14  = e14_s[idx];  e30  = e30_s[idx]
    e60  = e60_s[idx];  e120 = e120_s[idx]; e180 = e180_s[idx]
    cur  = close[idx];  mh   = macd_h[idx]
    daily_adx   = adx_line[idx]
    daily_plus  = plus_di[idx]
    daily_minus = minus_di[idx]

    bull = sum([
        e7 > e14, e14 > e30, e30 > e60, e60 > e120, e120 > e180,
        e30 > e30_s[prev_idx], e60 > e60_s[prev_idx], mh > 0
    ])
    bear = sum([
        e7 < e14, e14 < e30, e30 < e60, e60 < e120, e120 < e180,
        e30 < e30_s[prev_idx], e60 < e60_s[prev_idx], mh < 0
    ])

    if daily_adx < 20:
        regime = "RANGE"
    elif bull >= 7 and daily_plus > daily_minus:
        regime = "BULL"
    elif bull >= 5 and daily_plus > daily_minus:
        regime = "BULL_WEAK"
    elif bear >= 7 and daily_minus > daily_plus:
        regime = "BEAR"
    elif bear >= 5 and daily_minus > daily_plus:
        regime = "BEAR_WEAK"
    else:
        regime = "RANGE"

    return regime, {
        "date": str(as_of_date), "regime": regime,
        "bull_score": bull, "bear_score": bear,
        "adx": round(daily_adx, 1), "cur": round(cur, 0),
        "e30": round(e30, 0), "e60": round(e60, 0),
    }