import pandas as pd
from data.collector import get_binance_data, get_binance_daily_data
from indicators.calculators_v54 import prep_data_vectorized, ATR_STOP_MULT

# ── 수익 확보 파라미터 ──
PARTIAL_EXIT_TRIGGER = 3.5    # [V54] 5.0% → 3.5% (더 일찍 이익 확보)
PARTIAL_EXIT_RATIO   = 0.5
MIN_PROFIT_FOR_EXIT  = 1.0    # [V54] 1.5% → 1.0% (조건 익절 문턱 낮춤)
ATR_STOP_HARDCAP     = -6.0   # 마진 기준 최대 손실
BE_OFFSET_PCT        = 0.10   # [V54] 0.15% → 0.10% (분할 후 더 빠른 손익분기)
MIN_HOLD_CANDLES     = 4      # [V54] 6봉 → 4봉 (더 빠른 조건 익절 허용)

# ── 쿨다운 기본값 (봉 단위, 5분봉 기준) ──
COOLDOWN_TREND = 96           # 8시간
COOLDOWN_RANGE = 96           # 8시간

# ── 레버리지 ──
LEV_MAX_TREND = 5             # V55: 8 → 5 (수수료 절감)
LEV_MAX_RANGE = 4             # V55: 6 → 4
LEV_MIN       = 3

# ── 리스크 관리 ──
MAX_DAILY_LOSS_PCT   = -5.0   # [V54] -4.0% → -5.0% (약간 더 여유)
MAX_CONSEC_STOP      = 2      # [V54] 3 → 2 (더 일찍 포지션 축소 시작)

# ── [V54] 방향별 연속 손절 기반 쿨다운 배수 ──
# 2연속: 2배, 3연속: 4배, 5연속: 12배 (장기 휴식)
COOLDOWN_MULT_2 = 2
COOLDOWN_MULT_3 = 4
COOLDOWN_MULT_5 = 12


def calc_dynamic_leverage(atr_pct, regime_base):
    base_lev = LEV_MAX_RANGE if regime_base == "RANGE" else LEV_MAX_TREND
    if atr_pct < 0.15:   factor = 1.0
    elif atr_pct < 0.30: factor = 0.8
    elif atr_pct < 0.50: factor = 0.6
    else:                factor = 0.4
    return max(LEV_MIN, int(base_lev * factor))


def calc_pos_ratio(lev, consecutive_stop):
    """
    [V54] 포지션 비율: 원금의 10% 기준
    - 연속 손절 시 단계적으로 축소
    - 목표: 월 3~4% 수익률, 리스크 제어
    """
    base = 0.20   # V55: 10% → 20% (수수료 절감으로 리스크 여력 확보)

    if consecutive_stop >= 6:
        base = 0.08   # 6연속+ : 60% 축소
    elif consecutive_stop >= 4:
        base = 0.12   # 4연속+ : 40% 축소
    elif consecutive_stop >= MAX_CONSEC_STOP:
        base = 0.16   # 2연속+ : 20% 축소
    return base


def calc_stop_price(entry_price, atr_pct, leverage, direction):
    """
    stop distance = max(ATR 기반, 레버리지 역산)
    목표 실질 손실 ~2% (마진 기준) → 가격 기준 = 2% / leverage
    """
    target_loss_pct = 2.0
    atr_based_dist  = atr_pct * ATR_STOP_MULT / 100
    lev_based_dist  = (target_loss_pct / leverage) / 100
    stop_dist       = max(atr_based_dist, lev_based_dist)

    if direction == "long":
        stop     = entry_price * (1 - stop_dist)
        hardcap  = entry_price * (1 + ATR_STOP_HARDCAP / leverage / 100)
        return max(stop, hardcap)
    else:
        stop     = entry_price * (1 + stop_dist)
        hardcap  = entry_price * (1 - ATR_STOP_HARDCAP / leverage / 100)
        return min(stop, hardcap)


def _get_adaptive_cooldown(cooldown_base, directional_consec):
    """
    [V54] 방향별 연속 손절에 따른 적응형 쿨다운
    - 2연속: 2배 (2~4시간)
    - 3~4연속: 4배 (4~8시간)
    - 5연속+: 12배 (12~24시간 강제 휴식)
    """
    if directional_consec >= 5:
        return cooldown_base * COOLDOWN_MULT_5
    elif directional_consec >= 3:
        return cooldown_base * COOLDOWN_MULT_3
    elif directional_consec >= 2:
        return cooldown_base * COOLDOWN_MULT_2
    return cooldown_base


def _update_stats(stats, side, ret, entry_regime, is_stop, is_trail, is_cond):
    stats["total"] += 1
    if side == "long": stats["long_total"] += 1
    else:              stats["short_total"] += 1

    rk = entry_regime if entry_regime in stats["regimes"] else "RANGE"
    stats["regimes"][rk]["total"] += 1

    if ret > 0.5:
        stats["win"] += 1
        stats["regimes"][rk]["win"] += 1
        stats["gross_profit"] += ret
        stats["max_win"] = max(stats["max_win"], ret)
        if side == "long": stats["long_win"] += 1
        else:              stats["short_win"] += 1
    elif ret < -0.5:
        stats["loss"] += 1
        stats["regimes"][rk]["loss"] += 1
        stats["gross_loss"] += abs(ret)
        stats["max_loss"] = max(stats["max_loss"], abs(ret))
    else:
        stats["breakeven"] += 1

    if is_stop:    stats["stop_count"] += 1
    elif is_trail: stats["trailing_count"] += 1
    elif is_cond:  stats["cond_exit_count"] += 1


def run_portfolio_backtest(initial_budget=100_000):
    cash      = initial_budget
    fee_rate  = 0.0002

    # ── 롱 상태 ──
    long_shares = long_avg_price = long_margin_used = 0.0
    long_leverage    = 1
    long_last_idx    = long_last_exit_idx = -9999
    long_max_return  = 0.0
    long_stop_price  = None
    long_partial_done   = False
    long_entry_regime   = "RANGE"

    # ── 숏 상태 ──
    short_shares = short_avg_price = short_margin_used = 0.0
    short_leverage   = 1
    short_last_idx   = short_last_exit_idx = -9999
    short_max_return = 0.0
    short_stop_price = None
    short_partial_done  = False
    short_entry_regime  = "RANGE"

    total_fees_paid  = 0.0
    history          = []
    monthly_pnl_dict = {}
    peak_equity      = initial_budget

    # ── [V54] 연속 손절 추적: 전체(포지션 크기용) + 방향별(쿨다운용) ──
    consecutive_stop  = 0    # 전체 (포지션 비율 결정)
    long_consec_stop  = 0    # 롱 방향별 (롱 쿨다운 결정)
    short_consec_stop = 0    # 숏 방향별 (숏 쿨다운 결정)

    # ── 일일 손실 추적 ──
    daily_loss_tracker = {}

    stats = {
        "total": 0, "win": 0, "loss": 0, "breakeven": 0,
        "long_total": 0, "long_win": 0,
        "short_total": 0, "short_win": 0,
        "stop_count": 0, "trailing_count": 0,
        "cond_exit_count": 0, "partial_exit_count": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "max_win": 0.0, "max_loss": 0.0, "max_drawdown": 0.0,
        "daily_block_count": 0,
        "regimes": {
            "BULL":  {"total": 0, "win": 0, "loss": 0},
            "BEAR":  {"total": 0, "win": 0, "loss": 0},
            "RANGE": {"total": 0, "win": 0, "loss": 0},
        }
    }

    # ── 데이터 로드 ──
    daily_df = get_binance_daily_data(
        symbol="BTC/USDT",
        start_date="2022-10-01 00:00:00",
        end_date="2026-04-09 23:59:59"
    )
    data_btc = get_binance_data(
        symbol="BTC/USDT", interval="5m",
        start_date="2023-04-01 00:00:00",
        end_date="2026-04-09 23:59:59"
    )
    data_15m = get_binance_data(
        symbol="BTC/USDT", interval="15m",
        start_date="2023-03-01 00:00:00",
        end_date="2026-04-09 23:59:59"
    )
    data_1h  = get_binance_data(
        symbol="BTC/USDT", interval="1h",
        start_date="2023-03-01 00:00:00",
        end_date="2026-04-09 23:59:59"
    )

    if data_btc.empty:
        return [initial_budget], 0.0, stats, {}

    df = prep_data_vectorized(data_btc, daily_df, data_15m, data_1h)

    print(f"\n[V54] 10% 포지션 | 적응형 쿨다운 | 강화된 레짐 | 15분봉 필터")
    print(f"{'시각':<12} | {'국면':<6} | {'행동':<7} | {'가격':>9} | {'수익률':>6} | {'수수료':>5} | {'현금':>9} | 비고")
    print("-" * 112)

    for row in df.itertuples():
        i = row.Index_Num
        if i < 300:
            continue

        cur_date  = row.Index.date()
        cur_ym    = (row.Index.year, row.Index.month)
        time_str  = row.Index.strftime('%y-%m-%d %H:%M')
        cur_close = row.Close
        cur_open  = row.Open
        cur_high  = row.High
        cur_low   = row.Low
        regime    = row.regime
        atr_pct   = row.atr_pct

        # [V54] 트레일링 파라미터 (더 이른 익절)
        trail_trig = 2.0 if regime == "RANGE" else 2.5   # V53: 2.5/3.0
        trail_drop = 0.6 if regime == "RANGE" else 0.9   # V53: 0.8/1.2

        # [V54] 기본 쿨다운
        cooldown_base = COOLDOWN_RANGE if regime == "RANGE" else COOLDOWN_TREND

        # [V54] 방향별 적응형 쿨다운 계산
        long_cooldown  = _get_adaptive_cooldown(cooldown_base, long_consec_stop)
        short_cooldown = _get_adaptive_cooldown(cooldown_base, short_consec_stop)

        # 신호 (이미 1봉 shift 완료)
        is_buy   = row.buy_bull   if regime == "BULL"  else (row.buy_range   if regime == "RANGE" else False)
        is_short = row.short_bear if regime == "BEAR"  else (row.short_range if regime == "RANGE" else False)
        is_sell  = row.sell_bull  if regime == "BULL"  else (row.sell_range  if regime == "RANGE" else False)
        is_cover = row.cover_bear if regime == "BEAR"  else (row.cover_range if regime == "RANGE" else False)

        # ── 일일 손실 한도 체크 ──
        daily_loss_today = daily_loss_tracker.get(cur_date, 0.0)
        current_equity   = cash
        if long_shares  > 0: current_equity += long_margin_used  + long_shares  * (cur_close - long_avg_price)
        if short_shares > 0: current_equity += short_margin_used + short_shares * (short_avg_price - cur_close)
        ref_equity       = max(peak_equity, initial_budget)
        daily_loss_pct   = daily_loss_today / ref_equity * 100
        entry_blocked    = daily_loss_pct < MAX_DAILY_LOSS_PCT

        # ══════════════════════════════════
        # 롱 청산
        # ══════════════════════════════════
        if long_shares > 0:
            hit_stop   = long_stop_price and cur_low <= long_stop_price
            exec_price = long_stop_price if hit_stop else cur_close
            long_ret   = (long_shares * (exec_price - long_avg_price)) / long_margin_used * 100
            candles_held = i - long_last_idx

            if long_ret > 0:
                long_max_return = max(long_max_return, long_ret)

            is_stop = long_ret <= ATR_STOP_HARDCAP or hit_stop

            # 분할 청산 (V54: 3.5% 트리거)
            if not long_partial_done and long_ret >= PARTIAL_EXIT_TRIGGER and not is_stop:
                p_qty = long_shares * PARTIAL_EXIT_RATIO
                fee_p = p_qty * cur_close * fee_rate
                pnl_p = p_qty * (cur_close - long_avg_price)
                total_fees_paid += fee_p
                cash += long_margin_used * PARTIAL_EXIT_RATIO + (pnl_p - fee_p)
                long_shares      -= p_qty
                long_margin_used *= (1 - PARTIAL_EXIT_RATIO)
                long_partial_done = True
                long_stop_price   = long_avg_price * (1 + BE_OFFSET_PCT / 100)
                stats["partial_exit_count"] += 1
                pr = pnl_p / (long_margin_used / (1 - PARTIAL_EXIT_RATIO)) * 100
                print(f"{time_str} | {regime:<6} | L-PART | {cur_close:>9,.0f} | {pr:>5.1f}% | {fee_p:>4.0f} | {cash:>9,.0f} | 분할50%")
                long_ret = (long_shares * (cur_close - long_avg_price)) / long_margin_used * 100

            is_trailing  = (long_max_return >= trail_trig
                            and long_ret <= long_max_return - trail_drop)
            is_cond_exit = (is_sell and long_ret >= MIN_PROFIT_FOR_EXIT
                            and candles_held >= MIN_HOLD_CANDLES)

            if is_stop or is_trailing or is_cond_exit:
                fp  = long_stop_price if (hit_stop and is_stop) else cur_close
                fee = long_shares * fp * fee_rate
                net = long_shares * (fp - long_avg_price) - fee
                total_fees_paid += fee
                cash += long_margin_used + net
                long_ret = (long_shares * (fp - long_avg_price)) / long_margin_used * 100

                remark = "안전손절" if is_stop else "트레일링" if is_trailing else "조건익절"
                _update_stats(stats, "long", long_ret,
                              long_entry_regime, is_stop, is_trailing, is_cond_exit)

                monthly_pnl_dict[cur_ym] = monthly_pnl_dict.get(cur_ym, 0.0) + net
                if net < 0:
                    consecutive_stop  += 1
                    long_consec_stop  += 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consecutive_stop = 0
                    long_consec_stop = 0   # 롱 이익 → 롱 연속손절 리셋

                long_last_exit_idx = i
                print(f"{time_str} | {regime:<6} | L-EXIT | {fp:>9,.0f} | {long_ret:>5.1f}% | {fee:>4.0f} | {cash:>9,.0f} | {remark}")
                long_shares = long_avg_price = long_margin_used = long_max_return = 0.0
                long_stop_price   = None
                long_partial_done = False

        # ══════════════════════════════════
        # 숏 청산
        # ══════════════════════════════════
        if short_shares > 0:
            hit_stop_s   = short_stop_price and cur_high >= short_stop_price
            exec_price_s = short_stop_price if hit_stop_s else cur_close
            short_ret    = (short_shares * (short_avg_price - exec_price_s)) / short_margin_used * 100
            candles_held_s = i - short_last_idx

            if short_ret > 0:
                short_max_return = max(short_max_return, short_ret)

            is_short_stop = short_ret <= ATR_STOP_HARDCAP or hit_stop_s

            # 분할 청산 (V54: 3.5% 트리거)
            if not short_partial_done and short_ret >= PARTIAL_EXIT_TRIGGER and not is_short_stop:
                p_qty_s = short_shares * PARTIAL_EXIT_RATIO
                fee_ps  = p_qty_s * cur_close * fee_rate
                pnl_ps  = p_qty_s * (short_avg_price - cur_close)
                total_fees_paid += fee_ps
                cash += short_margin_used * PARTIAL_EXIT_RATIO + (pnl_ps - fee_ps)
                short_shares      -= p_qty_s
                short_margin_used *= (1 - PARTIAL_EXIT_RATIO)
                short_partial_done = True
                short_stop_price   = short_avg_price * (1 - BE_OFFSET_PCT / 100)
                stats["partial_exit_count"] += 1
                pr_s = pnl_ps / (short_margin_used / (1 - PARTIAL_EXIT_RATIO)) * 100
                print(f"{time_str} | {regime:<6} | S-PART | {cur_close:>9,.0f} | {pr_s:>5.1f}% | {fee_ps:>4.0f} | {cash:>9,.0f} | 분할50%")
                short_ret = (short_shares * (short_avg_price - cur_close)) / short_margin_used * 100

            is_short_trail = (short_max_return >= trail_trig
                              and short_ret <= short_max_return - trail_drop)
            is_short_cond  = (is_cover and short_ret >= MIN_PROFIT_FOR_EXIT
                              and candles_held_s >= MIN_HOLD_CANDLES)

            if is_short_stop or is_short_trail or is_short_cond:
                fps = short_stop_price if (hit_stop_s and is_short_stop) else cur_close
                fee = short_shares * fps * fee_rate
                net = short_shares * (short_avg_price - fps) - fee
                total_fees_paid += fee
                cash += short_margin_used + net
                short_ret = (short_shares * (short_avg_price - fps)) / short_margin_used * 100

                remark = "안전손절" if is_short_stop else "트레일링" if is_short_trail else "조건익절"
                _update_stats(stats, "short", short_ret,
                              short_entry_regime, is_short_stop, is_short_trail, is_short_cond)

                monthly_pnl_dict[cur_ym] = monthly_pnl_dict.get(cur_ym, 0.0) + net
                if net < 0:
                    consecutive_stop   += 1
                    short_consec_stop  += 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consecutive_stop  = 0
                    short_consec_stop = 0   # 숏 이익 → 숏 연속손절 리셋

                short_last_exit_idx = i
                print(f"{time_str} | {regime:<6} | S-EXIT | {fps:>9,.0f} | {short_ret:>5.1f}% | {fee:>4.0f} | {cash:>9,.0f} | {remark}")
                short_shares = short_avg_price = short_margin_used = short_max_return = 0.0
                short_stop_price   = None
                short_partial_done = False

        # 자산 / MDD 추적
        current_equity = cash
        if long_shares  > 0: current_equity += long_margin_used  + long_shares  * (cur_close - long_avg_price)
        if short_shares > 0: current_equity += short_margin_used + short_shares * (short_avg_price - cur_close)
        if current_equity > peak_equity:
            peak_equity = current_equity
        dd = (current_equity - peak_equity) / peak_equity * 100
        stats["max_drawdown"] = min(stats["max_drawdown"], dd)

        # ══════════════════════════════════
        # 롱 진입
        # ══════════════════════════════════
        if is_buy and short_shares == 0 and long_shares == 0 and (i - long_last_exit_idx) >= long_cooldown:
            if entry_blocked:
                stats["daily_block_count"] += 1
        can_enter_long = (
            is_buy
            and short_shares == 0
            and long_shares  == 0
            and (i - long_last_exit_idx) >= long_cooldown   # [V54] 방향별 적응형 쿨다운
            and not entry_blocked
        )
        if can_enter_long:
            leverage   = calc_dynamic_leverage(atr_pct, regime)
            pos_ratio  = calc_pos_ratio(leverage, consecutive_stop)
            margin_use = current_equity * pos_ratio

            if cash > margin_use * (1 + leverage * fee_rate):
                notional        = margin_use * leverage
                entry_exec      = cur_open
                fee             = notional * fee_rate
                total_fees_paid += fee
                long_avg_price   = entry_exec
                long_shares      = notional / entry_exec
                long_leverage    = leverage
                long_entry_regime = regime
                cash            -= (margin_use + fee)
                long_margin_used = margin_use
                long_last_idx    = i
                long_stop_price  = calc_stop_price(entry_exec, atr_pct, leverage, "long")
                stop_pct         = (entry_exec - long_stop_price) / entry_exec * 100
                lc_tag           = f" [롱연속:{long_consec_stop}]" if long_consec_stop > 0 else ""
                print(f"{time_str} | {regime:<6} | L-LONG | {entry_exec:>9,.0f} |        | {fee:>4.0f} | {cash:>9,.0f} | 롱(lev:{leverage}x,stop-{stop_pct:.2f}%){lc_tag}")

        # ══════════════════════════════════
        # 숏 진입
        # ══════════════════════════════════
        if is_short and long_shares == 0 and short_shares == 0 and (i - short_last_exit_idx) >= short_cooldown:
            if entry_blocked:
                stats["daily_block_count"] += 1
        can_enter_short = (
            is_short
            and long_shares  == 0
            and short_shares == 0
            and (i - short_last_exit_idx) >= short_cooldown  # [V54] 방향별 적응형 쿨다운
            and not entry_blocked
        )
        if can_enter_short:
            leverage   = calc_dynamic_leverage(atr_pct, regime)
            pos_ratio  = calc_pos_ratio(leverage, consecutive_stop)
            margin_use = current_equity * pos_ratio

            if cash > margin_use * (1 + leverage * fee_rate):
                notional         = margin_use * leverage
                entry_exec       = cur_open
                fee              = notional * fee_rate
                total_fees_paid  += fee
                short_avg_price   = entry_exec
                short_shares      = notional / entry_exec
                short_leverage    = leverage
                short_entry_regime = regime
                cash             -= (margin_use + fee)
                short_margin_used  = margin_use
                short_last_idx     = i
                short_stop_price   = calc_stop_price(entry_exec, atr_pct, leverage, "short")
                stop_pct_s         = (short_stop_price - entry_exec) / entry_exec * 100
                sc_tag             = f" [숏연속:{short_consec_stop}]" if short_consec_stop > 0 else ""
                print(f"{time_str} | {regime:<6} | S-SHORT| {entry_exec:>9,.0f} |        | {fee:>4.0f} | {cash:>9,.0f} | 숏(lev:{leverage}x,stop+{stop_pct_s:.2f}%){sc_tag}")

        history.append(current_equity)

    # ── 강제 청산 ──
    final_price = df['Close'].iloc[-1]
    if long_shares > 0:
        pnl  = long_shares * (final_price - long_avg_price)
        fee  = long_shares * final_price * fee_rate
        cash += long_margin_used + pnl - fee
        history.append(cash)
    if short_shares > 0:
        pnl  = short_shares * (short_avg_price - final_price)
        fee  = short_shares * final_price * fee_rate
        cash += short_margin_used + pnl - fee
        history.append(cash)

    return history if history else [initial_budget], total_fees_paid, stats, monthly_pnl_dict
