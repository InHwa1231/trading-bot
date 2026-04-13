"""
V56-plus 지표 계산기 - V56 로직 100% 동일 + LINK 자산 추가

V56과 완전히 동일한 시그널 로직 (EMA20/50 + 돈치안20)
새 자산(LINK)에도 동일 로직 적용
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange


def calc_signals_for_asset(data_4h, asset_name="BTC"):
    """V56과 100% 동일한 시그널 생성 (자산 추가만 다름)"""
    pd.set_option('future.no_silent_downcasting', True)

    df = data_4h.copy()
    c, h, l = df['Close'], df['High'], df['Low']

    # 공통 지표
    df['ema20'] = EMAIndicator(c, 20).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()
    adx_ind = ADXIndicator(h, l, c, 14)
    df['adx'] = adx_ind.adx()
    df['rsi'] = RSIIndicator(c, 14).rsi()
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()
    df['atr_pct'] = df['atr'] / c * 100
    df['ema20_slope'] = df['ema20'] - df['ema20'].shift(3)

    # EMA 추세추종
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
    df['raw_ema_buy'] = cross_buy | pullback_buy
    df['raw_ema_sell'] = (df['ema20'] < df['ema50'])

    # 돈치안 채널 돌파
    don_period = 20
    df['don_high'] = h.rolling(don_period).max()
    df['don_low'] = l.rolling(don_period).min()
    df['don_mid'] = (df['don_high'] + df['don_low']) / 2

    df['raw_don_buy'] = (
        (c > df['don_high'].shift(1)) &
        (df['adx'] > 22) &
        (df['rsi'] < 70) &
        not_extended
    )
    df['raw_don_sell'] = (c < df['don_mid'])

    # shift 1봉 (lookahead 방지)
    for sig in ['ema_buy', 'ema_sell', 'don_buy', 'don_sell']:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    df['Open'] = data_4h['Open']
    df['trend_bull'] = ema_bull.shift(1).fillna(False)
    df['trend_bear'] = (~ema_bull).shift(1).fillna(False)
    df['Index_Num'] = range(len(df))
    df['asset'] = asset_name

    return df
