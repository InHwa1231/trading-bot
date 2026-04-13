"""
V56-bidir 지표 계산기 - V56-plus 롱 시그널 + 숏 시그널 추가

롱 시그널: V56-plus와 100% 동일
숏 시그널: 하락 추세에서 숏 진입/청산 시그널 추가
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange


def calc_signals_for_asset(data_4h, asset_name="BTC"):
    """V56-plus 롱 시그널 + 숏 시그널 생성"""
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

    # ═══════════════════════════════════
    # 롱 시그널 (V56-plus 100% 동일)
    # ═══════════════════════════════════
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

    # ═══════════════════════════════════
    # 숏 시그널 (NEW)
    # ═══════════════════════════════════
    ema_bear = df['ema20'] < df['ema50']
    ema_bear_prev = df['ema20'].shift(1) >= df['ema50'].shift(1)
    not_oversold = df['rsi'] > 35
    not_extended_down = (1 - c / df['ema50']) * 100 < 5.0  # 이미 너무 빠지면 진입X

    safe_to_short = not_oversold & not_extended_down

    # EMA 숏: 데드크로스 + 풀백 반등 후 재하락
    cross_short = ema_bear & ema_bear_prev & safe_to_short
    pullback_short = (
        ema_bear &
        (df['ema20_slope'] < 0) &    # EMA20 하락 중
        (df['rsi'] > 55) &            # 과매수 반등 후
        (df['rsi'].shift(1) <= 55) &  # RSI가 55 돌파
        (df['adx'] > 20) &
        safe_to_short
    )
    df['raw_ema_short'] = cross_short | pullback_short
    df['raw_ema_cover'] = (df['ema20'] > df['ema50'])  # 골든크로스 시 숏 청산

    # 돈치안 숏: 하단 채널 돌파
    df['raw_don_short'] = (
        (c < df['don_low'].shift(1)) &  # 하단 채널 돌파
        (df['adx'] > 22) &
        (df['rsi'] > 30) &              # 과매도 아닌 상태
        not_extended_down
    )
    df['raw_don_cover'] = (c > df['don_mid'])  # 중간선 회복 시 숏 청산

    # shift 1봉 (lookahead 방지) - 롱 + 숏 모두
    for sig in ['ema_buy', 'ema_sell', 'don_buy', 'don_sell',
                'ema_short', 'ema_cover', 'don_short', 'don_cover']:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    df['Open'] = data_4h['Open']
    df['trend_bull'] = ema_bull.shift(1).fillna(False)
    df['trend_bear'] = ema_bear.shift(1).fillna(False)
    df['Index_Num'] = range(len(df))
    df['asset'] = asset_name

    return df
