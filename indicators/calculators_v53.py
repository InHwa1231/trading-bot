import pandas as pd
import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator

ATR_STOP_MULT = 2.0
ATR_MIN       = 0.08
ATR_MAX       = 0.60


def calc_daily_regime_shifted(daily_df):
    ddf = daily_df.copy()
    c   = ddf['Close']
    e20 = EMAIndicator(c, 20).ema_indicator()
    e50 = EMAIndicator(c, 50).ema_indicator()
    adx = ADXIndicator(ddf['High'], ddf['Low'], c, 14).adx()

    def get_regime(r):
        if pd.isna(r.adx) or r.adx < 20: return "RANGE"
        if r.e20 > r.e50:                 return "BULL"
        if r.e20 < r.e50:                 return "BEAR"
        return "RANGE"

    ddf['e20'], ddf['e50'], ddf['adx'] = e20, e50, adx
    ddf['regime_raw'] = ddf.apply(get_regime, axis=1)
    # 전날 종가 기준 국면 → 당일 적용 (lookahead 제거)
    ddf['regime'] = ddf['regime_raw'].shift(1).fillna('RANGE')
    ddf['Date']   = ddf.index.date
    return ddf[['Date', 'regime']]


def prep_data_vectorized(data_btc, daily_df, data_15m, data_1h):
    print("⚡ [V53] 지표 계산 중...")
    pd.set_option('future.no_silent_downcasting', True)

    df = data_btc.copy()
    c, h, l, v  = df['Close'], df['High'], df['Low'], df['Volume']
    df['Open']  = data_btc['Open']
    df['Date']  = df.index.date

    # ── 1시간봉 추세 (shift 적용 → lookahead 제거) ──
    if not data_1h.empty:
        d1h        = data_1h.copy()
        d1h['e20'] = EMAIndicator(d1h['Close'], 20).ema_indicator()
        d1h['e50'] = EMAIndicator(d1h['Close'], 50).ema_indicator()
        # 1h 봉도 확정된 이전 봉 기준으로 판단
        d1h['h1_bull'] = (d1h['e20'] > d1h['e50']).shift(1, fill_value=False)
        d1h['h1_bear'] = (d1h['e20'] < d1h['e50']).shift(1, fill_value=False)
        h1 = d1h[['h1_bull', 'h1_bear']].reindex(df.index, method='ffill')
        df['h1_bull'] = h1['h1_bull'].fillna(False).astype(bool)
        df['h1_bear'] = h1['h1_bear'].fillna(False).astype(bool)
    else:
        df['h1_bull'] = df['h1_bear'] = False

    # ── 5분봉 지표 ──
    df['ema9']  = EMAIndicator(c, 9).ema_indicator()
    df['ema21'] = EMAIndicator(c, 21).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()
    df['rsi']   = RSIIndicator(c, 14).rsi()

    obv               = OnBalanceVolumeIndicator(c, v).on_balance_volume()
    df['obv_ema']     = EMAIndicator(obv, 20).ema_indicator()
    df['obv_rising']  = obv > df['obv_ema']
    df['obv_falling'] = obv < df['obv_ema']

    df['atr']     = AverageTrueRange(h, l, c, 14).average_true_range()
    df['atr_pct'] = df['atr'] / c * 100
    df['atr_ok']  = (df['atr_pct'] >= ATR_MIN) & (df['atr_pct'] <= ATR_MAX)

    bb             = BollingerBands(c, 20, 2)
    df['bb_up']   = bb.bollinger_hband()
    df['bb_dn']   = bb.bollinger_lband()
    df['bb_mid']  = bb.bollinger_mavg()
    df['bb_wide'] = ((df['bb_up'] - df['bb_dn']) / df['bb_mid']) > 0.015

    df['bull_candle'] = c > df['Open']
    df['bear_candle'] = c < df['Open']

    # ── 최근 N봉 고점/저점 대비 위치 (진입 필터 강화용) ──
    df['recent_high_20'] = h.rolling(20).max()
    df['recent_low_20']  = l.rolling(20).min()
    # 숏: 고점 대비 충분히 하락한 경우만 (횡보 구간 필터)
    df['below_recent_high'] = (c / df['recent_high_20'] - 1) * 100 < -0.2
    # 롱: 저점 대비 충분히 반등한 경우만
    df['above_recent_low']  = (c / df['recent_low_20']  - 1) * 100 >  0.2

    # ── 원시 진입/청산 조건 ──
    df['raw_buy_bull'] = (
        df['h1_bull'] &
        (df['ema9'] > df['ema21']) & (df['ema21'] > df['ema50']) &
        (l <= df['ema21'] * 1.002) & (c > df['ema9']) &
        df['obv_rising'] &
        (df['rsi'] > 45) & (df['rsi'] < 65) &
        df['atr_ok'] &
        df['above_recent_low']          # 추가 필터
    )
    df['raw_sell_bull'] = (df['rsi'] > 75) | (c < df['ema50'])

    df['raw_short_bear'] = (
        df['h1_bear'] &
        (df['ema9'] < df['ema21']) & (df['ema21'] < df['ema50']) &
        (h >= df['ema21'] * 0.998) & (c < df['ema9']) &
        df['obv_falling'] &
        (df['rsi'] < 55) & (df['rsi'] > 35) &
        df['atr_ok'] &
        df['below_recent_high']         # 추가 필터: 횡보 구간 손절 방지
    )
    df['raw_cover_bear'] = (df['rsi'] < 25) | (c > df['ema50'])

    is_range_macro         = ~df['h1_bull'] & ~df['h1_bear']
    df['raw_buy_range']    = (
        is_range_macro &
        (c.shift(1) < df['bb_dn']) & (c > df['bb_dn']) &
        df['bull_candle'] & (df['rsi'] < 40) & df['bb_wide'] & df['atr_ok']
    )
    df['raw_short_range']  = (
        is_range_macro &
        (c.shift(1) > df['bb_up']) & (c < df['bb_up']) &
        df['bear_candle'] & (df['rsi'] > 60) & df['bb_wide'] & df['atr_ok']
    )
    df['raw_sell_range']   = c >= df['bb_mid']
    df['raw_cover_range']  = c <= df['bb_mid']

    df['bias'] = np.where(df['h1_bull'], 'LONG_BIAS',
                 np.where(df['h1_bear'], 'SHORT_BIAS', 'NEUTRAL'))

    # ── shift 1회만 적용 (이중 shift 버그 제거) ──
    entry_signals = ['buy_bull', 'short_bear', 'buy_range', 'short_range']
    exit_signals  = ['sell_bull', 'cover_bear', 'sell_range', 'cover_range']

    for sig in entry_signals + exit_signals:
        df[sig] = df[f'raw_{sig}'].shift(1, fill_value=False)

    # 현재봉 상태 판단 (shift 불필요)
    df['is_up_bull'] = df['ema9'] > df['ema50']
    df['is_dn_bear'] = df['ema9'] < df['ema50']

    # ── 일봉 국면 병합 ──
    regime_df   = calc_daily_regime_shifted(daily_df)
    df          = df.merge(regime_df, on='Date', how='left')
    df.index    = data_btc.index
    df['regime'] = df['regime'].ffill().fillna('RANGE')
    df['Index_Num'] = range(len(df))

    return df