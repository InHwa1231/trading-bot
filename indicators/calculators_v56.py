"""
V56 지표 계산기 - 다중 전략 + 다중 자산

전략 1: EMA 추세추종 (V55 계승)
  - 4h EMA20/50 크로스오버 + 눌림목 진입
전략 2: 돈치안 채널 돌파
  - 4h 20봉 고가/저가 돌파 진입

모든 자산(BTC, ETH, SOL)에 동일 로직 적용
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange


def calc_signals_for_asset(data_4h, asset_name="BTC"):
    """단일 자산의 4h 데이터에서 두 가지 전략 시그널 생성"""
    pd.set_option('future.no_silent_downcasting', True)

    df = data_4h.copy()
    c, h, l = df['Close'], df['High'], df['Low']

    # ── 공통 지표 ──
    df['ema20'] = EMAIndicator(c, 20).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()
    adx_ind = ADXIndicator(h, l, c, 14)
    df['adx'] = adx_ind.adx()
    df['rsi'] = RSIIndicator(c, 14).rsi()
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()
    df['atr_pct'] = df['atr'] / c * 100
    df['ema20_slope'] = df['ema20'] - df['ema20'].shift(3)

    # ══════════════════════════════════════
    # 전략 1: EMA 추세추종 (V55 계승)
    # ══════════════════════════════════════
    ema_bull = df['ema20'] > df['ema50']
    ema_bull_prev = df['ema20'].shift(1) <= df['ema50'].shift(1)
    not_overbought = df['rsi'] < 65
    not_extended = (c / df['ema50'] - 1) * 100 < 5.0
    safe_to_enter = not_overbought & not_extended

    # 골든크로스 진입
    cross_buy = ema_bull & ema_bull_prev & safe_to_enter
    # 눌림목 진입
    pullback_buy = (
        ema_bull &
        (df['ema20_slope'] > 0) &
        (df['rsi'] < 45) &
        (df['rsi'].shift(1) >= 45) &
        (df['adx'] > 20) &
        safe_to_enter
    )
    df['raw_ema_buy'] = cross_buy | pullback_buy
    df['raw_ema_sell'] = (df['ema20'] < df['ema50'])  # 데드크로스 청산

    # ══════════════════════════════════════
    # 전략 2: 돈치안 채널 돌파
    # ══════════════════════════════════════
    don_period = 20
    df['don_high'] = h.rolling(don_period).max()
    df['don_low'] = l.rolling(don_period).min()
    df['don_mid'] = (df['don_high'] + df['don_low']) / 2

    # 돌파 진입: 종가가 돈치안 상단 돌파 + ADX 확인
    df['raw_don_buy'] = (
        (c > df['don_high'].shift(1)) &  # 이전 봉의 상단 돌파
        (df['adx'] > 22) &               # 추세 강도 확인
        (df['rsi'] < 70) &               # 과열 아님
        not_extended                       # EMA50 대비 과도 확장 아님
    )
    # 돈치안 청산: 중간선 하회
    df['raw_don_sell'] = (c < df['don_mid'])

    # ── shift 1봉 (lookahead 방지) ──
    for sig in ['ema_buy', 'ema_sell', 'don_buy', 'don_sell']:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    df['Open'] = data_4h['Open']
    df['trend_bull'] = ema_bull.shift(1).fillna(False)
    df['trend_bear'] = (~ema_bull).shift(1).fillna(False)
    df['Index_Num'] = range(len(df))
    df['asset'] = asset_name

    return df
