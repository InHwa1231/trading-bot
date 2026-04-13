"""
Parker Brooks 스타일 지표 계산기
VWAP + EMA + Volume Profile 기반

- VWAP: 일간 리셋 (4h 봉 기준 6봉마다)
- EMA: 20/50 추세 판단
- Volume Profile: 20봉 롤링, POC/VAH/VAL 계산
"""
import pandas as pd
import numpy as np
from ta.trend import EMAIndicator, ADXIndicator
from ta.volatility import AverageTrueRange


def calc_volume_profile(highs, lows, closes, volumes, n_bins=20):
    """
    롤링 윈도우 Volume Profile 계산
    Returns: POC, VAH, VAL 시리즈
    """
    poc_series = pd.Series(np.nan, index=closes.index)
    vah_series = pd.Series(np.nan, index=closes.index)
    val_series = pd.Series(np.nan, index=closes.index)

    lookback = 20  # 20봉 (약 3.3일)

    for idx in range(lookback, len(closes)):
        window_high = highs.iloc[idx - lookback:idx]
        window_low = lows.iloc[idx - lookback:idx]
        window_close = closes.iloc[idx - lookback:idx]
        window_vol = volumes.iloc[idx - lookback:idx]

        price_min = window_low.min()
        price_max = window_high.max()

        if price_max == price_min or price_max == 0:
            continue

        # 가격대별 볼륨 분배
        bin_edges = np.linspace(price_min, price_max, n_bins + 1)
        bin_volumes = np.zeros(n_bins)

        for j in range(len(window_close)):
            # 각 캔들의 볼륨을 해당 가격 범위에 분배
            c_low = window_low.iloc[j]
            c_high = window_high.iloc[j]
            c_vol = window_vol.iloc[j]

            for b in range(n_bins):
                bin_low = bin_edges[b]
                bin_high = bin_edges[b + 1]
                # 캔들 범위와 빈 범위의 겹침 비율
                overlap_low = max(c_low, bin_low)
                overlap_high = min(c_high, bin_high)
                if overlap_high > overlap_low:
                    candle_range = c_high - c_low if c_high > c_low else 1
                    overlap_ratio = (overlap_high - overlap_low) / candle_range
                    bin_volumes[b] += c_vol * overlap_ratio

        # POC: 최대 볼륨 가격대
        poc_bin = np.argmax(bin_volumes)
        poc_price = (bin_edges[poc_bin] + bin_edges[poc_bin + 1]) / 2
        poc_series.iloc[idx] = poc_price

        # Value Area: POC에서 양방향으로 70% 볼륨 포함
        total_vol = bin_volumes.sum()
        if total_vol == 0:
            continue

        va_target = total_vol * 0.70
        va_vol = bin_volumes[poc_bin]
        va_low_bin = poc_bin
        va_high_bin = poc_bin

        while va_vol < va_target and (va_low_bin > 0 or va_high_bin < n_bins - 1):
            expand_low = bin_volumes[va_low_bin - 1] if va_low_bin > 0 else 0
            expand_high = bin_volumes[va_high_bin + 1] if va_high_bin < n_bins - 1 else 0

            if expand_low >= expand_high and va_low_bin > 0:
                va_low_bin -= 1
                va_vol += bin_volumes[va_low_bin]
            elif va_high_bin < n_bins - 1:
                va_high_bin += 1
                va_vol += bin_volumes[va_high_bin]
            elif va_low_bin > 0:
                va_low_bin -= 1
                va_vol += bin_volumes[va_low_bin]
            else:
                break

        vah_series.iloc[idx] = bin_edges[va_high_bin + 1]
        val_series.iloc[idx] = bin_edges[va_low_bin]

    return poc_series, vah_series, val_series


def calc_vwap_daily(closes, highs, lows, volumes, index):
    """
    일간 리셋 VWAP (4h = 6봉마다 리셋)
    크립토는 00:00 UTC 기준으로 리셋
    """
    vwap = pd.Series(np.nan, index=index)
    typical_price = (highs + lows + closes) / 3

    cum_tp_vol = 0.0
    cum_vol = 0.0
    prev_date = None

    for i in range(len(closes)):
        cur_date = index[i].date()

        # 날짜 변경 시 리셋
        if cur_date != prev_date:
            cum_tp_vol = 0.0
            cum_vol = 0.0
            prev_date = cur_date

        cum_tp_vol += typical_price.iloc[i] * volumes.iloc[i]
        cum_vol += volumes.iloc[i]

        if cum_vol > 0:
            vwap.iloc[i] = cum_tp_vol / cum_vol

    return vwap


def calc_signals_for_asset(data_4h, asset_name="BTC"):
    """Parker Brooks 스타일 시그널 생성"""
    pd.set_option('future.no_silent_downcasting', True)

    df = data_4h.copy()
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume']

    # ── EMA ──
    df['ema20'] = EMAIndicator(c, 20).ema_indicator()
    df['ema50'] = EMAIndicator(c, 50).ema_indicator()
    df['ema9'] = EMAIndicator(c, 9).ema_indicator()

    # ── ADX & ATR ──
    adx_ind = ADXIndicator(h, l, c, 14)
    df['adx'] = adx_ind.adx()
    df['atr'] = AverageTrueRange(h, l, c, 14).average_true_range()

    # ── VWAP (일간 리셋) ──
    df['vwap'] = calc_vwap_daily(c, h, l, v, df.index)

    # ── Volume Profile ──
    df['poc'], df['vah'], df['val'] = calc_volume_profile(h, l, c, v)

    # ── 추세 판단 ──
    ema_bull = df['ema20'] > df['ema50']
    ema_bear = df['ema20'] < df['ema50']

    # ── VWAP 기반 방향성 ──
    price_above_vwap = c > df['vwap']
    price_below_vwap = c < df['vwap']

    # ── Volume Profile 기반 지지/저항 ──
    # 가격이 VAL 근처 (하단 지지)
    near_val = (c - df['val']).abs() / c * 100 < 1.5  # 가격의 1.5% 이내
    # 가격이 POC 근처 (중심 지지/저항)
    near_poc = (c - df['poc']).abs() / c * 100 < 1.0  # 가격의 1.0% 이내
    # 가격이 VAH 근처 (상단 저항)
    near_vah = (c - df['vah']).abs() / c * 100 < 1.5

    # ── 볼륨 확인 ──
    vol_ma = v.rolling(20).mean()
    vol_above_avg = v > vol_ma * 0.8  # 평균의 80% 이상

    # ═══════════════════════════════════
    # 롱 시그널: VWAP 위 + EMA 상승 + VAL/POC 지지
    # ═══════════════════════════════════
    # 타입1: VWAP 위에서 VAL 지지 반등 (트렌드 내 풀백)
    long_type1 = (
        ema_bull &                    # EMA 상승 추세
        price_above_vwap &            # VWAP 위
        near_val &                    # VAL 근처 (지지)
        (c > df['ema9']) &            # 단기 EMA 위 (반등 확인)
        vol_above_avg
    )

    # 타입2: POC 지지 + VWAP 위 (강한 지지 영역)
    long_type2 = (
        ema_bull &
        price_above_vwap &
        near_poc &                    # POC 근처 (강한 지지)
        (c > c.shift(1)) &           # 양봉 (반등)
        (df['adx'] > 20)
    )

    # 타입3: VAH 돌파 (브레이크아웃)
    long_type3 = (
        ema_bull &
        price_above_vwap &
        (c > df['vah']) &             # VAH 돌파
        (c.shift(1) <= df['vah'].shift(1)) &  # 이전봉은 VAH 이하
        (df['adx'] > 22) &
        vol_above_avg
    )

    df['raw_long_buy'] = long_type1 | long_type2 | long_type3
    df['raw_long_exit'] = (
        (c < df['vwap']) &            # VWAP 아래로 이탈
        (c < df['poc'])               # POC도 이탈
    ) | ema_bear                      # 또는 EMA 데드크로스

    # ═══════════════════════════════════
    # 숏 시그널: VWAP 아래 + EMA 하락 + VAH/POC 저항
    # ═══════════════════════════════════
    # 타입1: VWAP 아래에서 VAH 저항 거부 (하락 추세 내 반등 매도)
    short_type1 = (
        ema_bear &
        price_below_vwap &
        near_vah &                    # VAH 근처 (저항)
        (c < df['ema9']) &            # 단기 EMA 아래 (거부 확인)
        vol_above_avg
    )

    # 타입2: POC 저항 + VWAP 아래 (강한 저항 영역)
    short_type2 = (
        ema_bear &
        price_below_vwap &
        near_poc &
        (c < c.shift(1)) &           # 음봉 (거부)
        (df['adx'] > 20)
    )

    # 타입3: VAL 붕괴 (브레이크다운)
    short_type3 = (
        ema_bear &
        price_below_vwap &
        (c < df['val']) &             # VAL 붕괴
        (c.shift(1) >= df['val'].shift(1)) &
        (df['adx'] > 22) &
        vol_above_avg
    )

    df['raw_short_sell'] = short_type1 | short_type2 | short_type3
    df['raw_short_exit'] = (
        (c > df['vwap']) &
        (c > df['poc'])
    ) | ema_bull

    # shift 1봉 (lookahead 방지)
    df['long_buy'] = df['raw_long_buy'].shift(1, fill_value=False)
    df['long_exit'] = df['raw_long_exit'].shift(1, fill_value=False)
    df['short_sell'] = df['raw_short_sell'].shift(1, fill_value=False)
    df['short_exit'] = df['raw_short_exit'].shift(1, fill_value=False)

    df['Open'] = data_4h['Open']
    df['trend_bull'] = ema_bull.shift(1).fillna(False)
    df['trend_bear'] = ema_bear.shift(1).fillna(False)
    df['Index_Num'] = range(len(df))
    df['asset'] = asset_name

    return df
