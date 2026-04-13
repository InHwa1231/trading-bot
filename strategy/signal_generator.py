"""
실시간 시그널 생성기

4h 캔들 데이터에서 V56 전략 시그널을 생성
- EMA 추세추종 시그널
- 돈치안 채널 돌파 시그널
- BTC 추세 필터
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange
import logging

logger = logging.getLogger("signal_gen")


def generate_signals(df_4h, asset_name="BTC"):
    """
    4h OHLCV 데이터프레임에서 현재 시그널 생성

    Returns:
        dict: {
            'ema_buy': bool, 'ema_sell': bool,
            'don_buy': bool, 'don_sell': bool,
            'trend_bull': bool, 'atr': float, 'adx': float,
            'rsi': float, 'close': float, 'open': float,
            'high': float, 'low': float
        }
    """
    if len(df_4h) < 60:
        logger.warning(f"{asset_name}: 데이터 부족 ({len(df_4h)}봉)")
        return None

    df = df_4h.copy()
    c, h, l = df['Close'], df['High'], df['Low']

    # 공통 지표
    df['ema20'] = EMAIndicator(c, 20).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()
    adx_ind = ADXIndicator(h, l, c, 14)
    df['adx'] = adx_ind.adx()
    df['rsi'] = RSIIndicator(c, 14).rsi()
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()
    df['ema20_slope'] = df['ema20'] - df['ema20'].shift(3)

    # EMA 추세추종 시그널
    ema_bull = df['ema20'] > df['ema50']
    ema_bull_prev = df['ema20'].shift(1) <= df['ema50'].shift(1)
    not_overbought = df['rsi'] < 65
    not_extended = (c / df['ema50'] - 1) * 100 < 5.0
    safe_to_enter = not_overbought & not_extended

    cross_buy = ema_bull & ema_bull_prev & safe_to_enter
    pullback_buy = (
        ema_bull &
        (df['ema20_slope'] > 0) &
        (df['rsi'] < 45) &
        (df['rsi'].shift(1) >= 45) &
        (df['adx'] > 20) &
        safe_to_enter
    )
    raw_ema_buy = cross_buy | pullback_buy
    raw_ema_sell = df['ema20'] < df['ema50']

    # 돈치안 채널 돌파 시그널
    don_period = 20
    df['don_high'] = h.rolling(don_period).max()
    df['don_low'] = l.rolling(don_period).min()
    df['don_mid'] = (df['don_high'] + df['don_low']) / 2

    raw_don_buy = (
        (c > df['don_high'].shift(1)) &
        (df['adx'] > 22) &
        (df['rsi'] < 70) &
        not_extended
    )
    raw_don_sell = c < df['don_mid']

    # shift 1봉 (lookahead 방지) — 마지막 완성봉 기준 시그널
    # 실시간에서는 "직전 완성봉"의 시그널을 사용
    last = len(df) - 1
    prev = last - 1  # shift(1) 효과: 직전 완성봉의 raw 시그널 사용

    if prev < 0:
        return None

    result = {
        'ema_buy': bool(raw_ema_buy.iloc[prev]),
        'ema_sell': bool(raw_ema_sell.iloc[prev]),
        'don_buy': bool(raw_don_buy.iloc[prev]),
        'don_sell': bool(raw_don_sell.iloc[prev]),
        'trend_bull': bool(ema_bull.iloc[prev]),
        'atr': float(df['atr'].iloc[prev]),
        'adx': float(df['adx'].iloc[prev]),
        'rsi': float(df['rsi'].iloc[prev]),
        'close': float(df['Close'].iloc[last]),
        'open': float(df['Open'].iloc[last]),
        'high': float(df['High'].iloc[last]),
        'low': float(df['Low'].iloc[last]),
        'ema20': float(df['ema20'].iloc[prev]),
        'ema50': float(df['ema50'].iloc[prev]),
    }

    logger.debug(
        f"{asset_name} | RSI:{result['rsi']:.1f} ADX:{result['adx']:.1f} "
        f"EMA_buy:{result['ema_buy']} DON_buy:{result['don_buy']} "
        f"Bull:{result['trend_bull']}"
    )
    return result
