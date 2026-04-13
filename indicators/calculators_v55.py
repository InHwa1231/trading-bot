"""
V55 지표 계산기 - 4h EMA 추세추종 (v14)

완전히 새로운 패러다임: 평균회귀 포기, 추세추종으로 전환
- BTC는 강한 추세 자산: 2023 $28K → 2026 $70K (+150%)
- EMA 크로스오버로 추세 방향 잡고 레버리지로 수익 극대화
- 거래 극소 (년 10건 내외) → 수수료 최소화
"""
import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange


def prep_data_vectorized(data_5m, data_4h):
    """4시간봉 EMA 추세 + ATR 트레일링"""
    print("[V55 v14] 4h EMA trend-following ...")
    pd.set_option('future.no_silent_downcasting', True)

    # ── 4시간봉 지표 계산 ──
    df = data_4h.copy()
    c, h, l = df['Close'], df['High'], df['Low']

    # EMA 크로스오버
    df['ema20'] = EMAIndicator(c, 20).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()

    # ADX (추세 강도)
    adx_ind = ADXIndicator(h, l, c, 14)
    df['adx'] = adx_ind.adx()

    # RSI (과열 필터)
    df['rsi'] = RSIIndicator(c, 14).rsi()

    # ATR (스탑 및 변동성)
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()
    df['atr_pct'] = df['atr'] / c * 100

    # EMA 기울기 (모멘텀 확인)
    df['ema20_slope'] = df['ema20'] - df['ema20'].shift(3)

    # ══════════════════════════════════════
    # 롱 진입: EMA 골든크로스 + 추세 확인
    # ══════════════════════════════════════
    ema_bull = df['ema20'] > df['ema50']
    ema_bull_prev = df['ema20'].shift(1) <= df['ema50'].shift(1)

    # 과열 필터: RSI > 65이면 고점 매수 위험 → 진입 금지
    not_overbought = df['rsi'] < 65

    # 가격 확장 필터: 가격이 EMA50 대비 5% 이상 위면 진입 금지
    not_extended = (c / df['ema50'] - 1) * 100 < 5.0

    safe_to_enter = not_overbought & not_extended

    # 방법 1: 크로스오버 시점에 진입
    cross_buy = ema_bull & ema_bull_prev & safe_to_enter

    # 방법 2: 추세 중 눌림목 진입 (이미 상승추세 + RSI 눌림)
    pullback_buy = (
        ema_bull &
        (df['ema20_slope'] > 0) &
        (df['rsi'] < 45) &
        (df['rsi'].shift(1) >= 45) &
        (df['adx'] > 20) &
        safe_to_enter
    )

    df['raw_buy'] = cross_buy | pullback_buy

    # ══════════════════════════════════════
    # 숏 진입: EMA 데드크로스 + 추세 확인
    # ══════════════════════════════════════
    ema_bear = df['ema20'] < df['ema50']
    ema_bear_prev = df['ema20'].shift(1) >= df['ema50'].shift(1)

    cross_short = ema_bear & ema_bear_prev  # 데드크로스 발생

    pullback_short = (
        ema_bear &
        (df['ema20_slope'] < 0) &
        (df['rsi'] > 55) &
        (df['rsi'].shift(1) <= 55) &
        (df['adx'] > 20)
    )

    df['raw_short'] = pd.Series(False, index=df.index)  # 롱 온리 (숏 비활성화)

    # ══════════════════════════════════════
    # 청산: 데드크로스만 (과열 청산 제거 - 추세 타기)
    # ══════════════════════════════════════
    df['raw_sell'] = (df['ema20'] < df['ema50'])  # 데드크로스만
    df['raw_cover'] = (
        (df['ema20'] > df['ema50']) |          # 골든크로스 → 숏 청산
        (df['rsi'] < 20)                        # 극단 과매도
    )

    # shift 1봉 (lookahead 방지)
    for sig in ['buy', 'short', 'sell', 'cover']:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    df['Open'] = data_4h['Open']
    df['trend_bull'] = ema_bull.shift(1).fillna(False)
    df['trend_bear'] = ema_bear.shift(1).fillna(False)

    df['Index_Num'] = range(len(df))
    return df
