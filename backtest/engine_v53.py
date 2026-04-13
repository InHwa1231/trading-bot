import pandas as pd
from data.collector import get_binance_data, get_binance_daily_data
from indicators.calculators_v53 import prep_data_vectorized, ATR_STOP_MULT

# ── 수익 확보 파라미터 ──
PARTIAL_EXIT_TRIGGER = 5.0    # 분할청산 트리거 (마진 기준 %)
PARTIAL_EXIT_RATIO   = 0.5
MIN_PROFIT_FOR_EXIT  = 1.5    # 조건 익절 최소 수익
ATR_STOP_HARDCAP     = -6.0   # 마진 기준 최대 손실
BE_OFFSET_PCT        = 0.15   # 분할 후 손익분기 오프셋
MIN_HOLD_CANDLES     = 6      # 최소 보유 봉 수

# ── 쿨다운 (봉 단위, 5분봉 기준) ──
COOLDOWN_TREND = 12           # 1시간
COOLDOWN_RANGE = 24           # 2시간

# ── 레버리지 ──
LEV_MAX_TREND = 8
LEV_MAX_RANGE = 5
LEV_MIN       = 3

# ── 리스크 관리 ──
MAX_DAILY_LOSS_PCT   = -4.0   # 일일 최대 손실 (자산 대비 %)
MAX_CONSEC_STOP      = 3      # 연속 손절 이 횟수 이상이면 규모 축소


def calc_dynamic_leverage(atr_pct, regime_base):
    base_lev = LEV_MAX_RANGE if regime_base == "RANGE" else LEV_MAX_TREND
    if atr_pct < 0.15:   factor = 1.0
    elif atr_pct < 0.30: factor = 0.8
    elif atr_pct < 0.50: factor = 0.6
    else:                factor = 0.4
    return max(LEV_MIN, int(base_lev * factor))


def calc_pos_ratio(lev, consecutive_stop):
    """연속 손절 횟수에 따라 포지션 비율 축소"""
    base = 0.60 if lev >= 8 else (0.70 if lev >= 6 else 0.80)
    if consecutive_stop >= MAX_CONSEC_STOP + 1:
        base *= 0.40    # 4연속 이상: 60% 축소
    elif consecutive_stop >= MAX_CONSEC_STOP:
        base *= 0.60    # 3연속: 40% 축소
    elif consecutive_stop >= 2:
        base *= 0.80    # 2연속: 20% 축소
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
    consecutive_stop = 0      # 연속 손절 카운터

    # ── 일일 손실 추적 ──
    daily_loss_tracker = {}   # {date: 누적 손실액}

    stats = {
        "total": 0, "win": 0, "loss": 0, "breakeven": 0,
        "long_total": 0, "long_win": 0,
        "short_total": 0, "short_win": 0,
        "stop_count": 0, "trailing_count": 0,
        "cond_exit_count": 0, "partial_exit_count": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "max_win": 0.0, "max_loss": 0.0, "max_drawdown": 0.0,
        "daily_block_count": 0,   # 일일 한도로 차단된 횟수
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

    print(f"\n[V53] lookahead 제거 + 리스크 관리 강화 (일일한도/연속손절 축소/진입필터)")
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

        # 트레일링 파라미터
        trail_trig = 2.5 if regime == "RANGE" else 3.0
        trail_drop = 0.8 if regime == "RANGE" else 1.2
        cooldown   = COOLDOWN_RANGE if regime == "RANGE" else COOLDOWN_TREND

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

            # 분할 청산
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
                    consecutive_stop += 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consecutive_stop = 0

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

            # 분할 청산
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
                    consecutive_stop += 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consecutive_stop = 0

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
        if is_buy and short_shares == 0 and long_shares == 0 and (i - long_last_exit_idx) >= cooldown:
            if entry_blocked:
                stats["daily_block_count"] += 1
        can_enter_long = (
            is_buy
            and short_shares == 0
            and long_shares  == 0
            and (i - long_last_exit_idx) >= cooldown
            and not entry_blocked          # 일일 한도 체크
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
                consec_tag       = f" [연속손절:{consecutive_stop}]" if consecutive_stop > 0 else ""
                print(f"{time_str} | {regime:<6} | L-LONG | {entry_exec:>9,.0f} |        | {fee:>4.0f} | {cash:>9,.0f} | 롱(lev:{leverage}x,stop-{stop_pct:.2f}%){consec_tag}")

        # ══════════════════════════════════
        # 숏 진입
        # ══════════════════════════════════
        if is_short and long_shares == 0 and short_shares == 0 and (i - short_last_exit_idx) >= cooldown:
            if entry_blocked:
                stats["daily_block_count"] += 1
        can_enter_short = (
            is_short
            and long_shares  == 0
            and short_shares == 0
            and (i - short_last_exit_idx) >= cooldown
            and not entry_blocked          # 일일 한도 체크
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
                consec_tag         = f" [연속손절:{consecutive_stop}]" if consecutive_stop > 0 else ""
                print(f"{time_str} | {regime:<6} | S-SHORT| {entry_exec:>9,.0f} |        | {fee:>4.0f} | {cash:>9,.0f} | 숏(lev:{leverage}x,stop+{stop_pct_s:.2f}%){consec_tag}")

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