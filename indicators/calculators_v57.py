"""
V57 지표 계산기 - 1h 고빈도 + 롱/숏 + 3전략

전략 1: EMA 추세추종 (빠른 EMA9/21)
전략 2: 돈치안 채널 돌파 (14봉)
전략 3: RSI 역추세 (과매도 반등 / 과매수 숏)

타임프레임: 1h → 더 빈번한 시그널
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange, BollingerBands


def calc_signals_v57_4h(data_4h, asset_name="BTC"):
    """4h 데이터에서 V56 검증 로직 기반 시그널 생성 (확장 자산용)"""
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


def calc_signals_v57(data_1h, asset_name="BTC"):
    """1h 데이터에서 3전략 시그널 생성 (롱+숏)"""
    pd.set_option('future.no_silent_downcasting', True)

    df = data_1h.copy()
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']

    # ── 공통 지표 ──
    df['ema9'] = EMAIndicator(c, 9).ema_indicator()
    df['ema21'] = EMAIndicator(c, 21).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()
    adx_ind = ADXIndicator(h, l, c, 14)
    df['adx'] = adx_ind.adx()
    df['plus_di'] = adx_ind.adx_pos()
    df['minus_di'] = adx_ind.adx_neg()
    df['rsi'] = RSIIndicator(c, 14).rsi()
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()
    df['atr_pct'] = df['atr'] / c * 100
    df['ema9_slope'] = df['ema9'] - df['ema9'].shift(3)

    # 볼린저 밴드 (RSI 전략용)
    bb = BollingerBands(c, 20, 2)
    df['bb_upper'] = bb.bollinger_hband()
    df['bb_lower'] = bb.bollinger_lband()
    df['bb_mid'] = bb.bollinger_mavg()

    # 거래량 이동평균
    df['vol_ma'] = v.rolling(20).mean()
    df['vol_ratio'] = v / df['vol_ma']

    # ══════════════════════════════════════
    # 전략 1: EMA 추세추종 (빠른 9/21)
    # ══════════════════════════════════════
    ema_bull = df['ema9'] > df['ema21']
    ema_bear = df['ema9'] < df['ema21']
    above_ema50 = c > df['ema50']
    below_ema50 = c < df['ema50']

    # 롱: 골든크로스 + 눌림목
    ema_cross_buy = ema_bull & (df['ema9'].shift(1) <= df['ema21'].shift(1))
    ema_pullback_buy = (
        ema_bull &
        (df['ema9_slope'] > 0) &
        (df['rsi'] < 45) &
        (df['rsi'].shift(1) >= 45) &
        (df['adx'] > 20)
    )
    not_overbought = df['rsi'] < 68
    not_oversold = df['rsi'] > 32
    not_extended_up = (c / df['ema21'] - 1) * 100 < 3.0
    not_extended_dn = (1 - c / df['ema21']) * 100 < 3.0

    df['raw_ema_buy'] = (ema_cross_buy | ema_pullback_buy) & not_overbought & not_extended_up
    df['raw_ema_sell_signal'] = (df['ema9'] < df['ema21'])

    # 숏: 데드크로스 + 풀백
    ema_cross_sell = ema_bear & (df['ema9'].shift(1) >= df['ema21'].shift(1))
    ema_pullback_sell = (
        ema_bear &
        (df['ema9_slope'] < 0) &
        (df['rsi'] > 55) &
        (df['rsi'].shift(1) <= 55) &
        (df['adx'] > 20)
    )
    df['raw_ema_short'] = (ema_cross_sell | ema_pullback_sell) & not_oversold & not_extended_dn
    df['raw_ema_cover'] = (df['ema9'] > df['ema21'])

    # ══════════════════════════════════════
    # 전략 2: 돈치안 채널 돌파 (14봉, 1h용으로 축소)
    # ══════════════════════════════════════
    don_period = 14
    df['don_high'] = h.rolling(don_period).max()
    df['don_low'] = l.rolling(don_period).min()
    df['don_mid'] = (df['don_high'] + df['don_low']) / 2

    # 롱: 상단 돌파
    df['raw_don_buy'] = (
        (c > df['don_high'].shift(1)) &
        (df['adx'] > 20) &
        (df['rsi'] < 72) &
        not_extended_up
    )
    df['raw_don_sell_signal'] = (c < df['don_mid'])

    # 숏: 하단 돌파
    df['raw_don_short'] = (
        (c < df['don_low'].shift(1)) &
        (df['adx'] > 20) &
        (df['rsi'] > 28) &
        not_extended_dn
    )
    df['raw_don_cover'] = (c > df['don_mid'])

    # ══════════════════════════════════════
    # 전략 3: RSI 역추세 + 볼린저 밴드
    # ══════════════════════════════════════
    rsi_prev = df['rsi'].shift(1)

    # 롱: RSI 과매도 반등 + BB 하단 터치
    df['raw_rsi_buy'] = (
        (df['rsi'] > 30) &
        (rsi_prev <= 30) &         # RSI 30 상향 돌파
        (c <= df['bb_lower'] * 1.005) &  # BB 하단 근처
        (df['adx'] < 35) &         # 추세 너무 강하지 않을 때
        (df['vol_ratio'] > 0.8)    # 거래량 있음
    )
    df['raw_rsi_sell_signal'] = (df['rsi'] > 60) | (c > df['bb_mid'])

    # 숏: RSI 과매수 하락 + BB 상단 터치
    df['raw_rsi_short'] = (
        (df['rsi'] < 70) &
        (rsi_prev >= 70) &         # RSI 70 하향 돌파
        (c >= df['bb_upper'] * 0.995) &  # BB 상단 근처
        (df['adx'] < 35) &
        (df['vol_ratio'] > 0.8)
    )
    df['raw_rsi_cover'] = (df['rsi'] < 40) | (c < df['bb_mid'])

    # ── shift 1봉 (lookahead 방지) ──
    signal_cols = [
        'ema_buy', 'ema_sell_signal', 'ema_short', 'ema_cover',
        'don_buy', 'don_sell_signal', 'don_short', 'don_cover',
        'rsi_buy', 'rsi_sell_signal', 'rsi_short', 'rsi_cover',
    ]
    for sig in signal_cols:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    # 추세 방향 (상위 프레임 필터용)
    df['trend_bull'] = (df['ema9'] > df['ema50']).shift(1).fillna(False)
    df['trend_bear'] = (df['ema9'] < df['ema50']).shift(1).fillna(False)

    df['Open'] = data_1h['Open']
    df['Index_Num'] = range(len(df))
    df['asset'] = asset_name

    return df
