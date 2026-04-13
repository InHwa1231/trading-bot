"""
V55 백테스트 엔진 - 4h EMA 추세추종 (v14)

핵심:
- 4h EMA20/50 크로스오버 + 눌림목 진입
- 추세 보유 (며칠~주 단위)
- ATR*3 트레일링 스탑
- 레버리지 3-5x, 포지션 35%
- 거래 극소 → 수수료 최소
"""
import pandas as pd
from data.collector import get_binance_data
from indicators.calculators_v55 import prep_data_vectorized

# ── 파라미터 ──
ATR_TRAIL_MULT = 4.0         # ATR*4 (최적 검증됨)
COOLDOWN = 6                 # 24시간 (4h 봉 6개)
LEVERAGE_BULL = 4            # 4x (최적 검증)
LEVERAGE_BEAR = 3            # (비활성)
MAX_DAILY_LOSS_PCT = -5.0
POS_BASE = 0.42              # 42% (PF 1.79 에지로 소폭 증가)
MAX_HOLD = 120               # 20일 (4h * 120 = 480h = 20일)
PARTIAL_TRIGGER = 8.0        # 마진 8% 수익 시 분할
PARTIAL_RATIO = 0.4          # 40% 분할


def calc_pos(consec_stop):
    if consec_stop >= 3:
        return POS_BASE * 0.5
    elif consec_stop >= 2:
        return POS_BASE * 0.7
    return POS_BASE


def _update_stats(stats, side, ret, exit_type, regime="RANGE"):
    stats["total"] += 1
    if side == "long": stats["long_total"] += 1
    else: stats["short_total"] += 1

    is_win = ret > 0.3
    if is_win:
        stats["win"] += 1
        stats["gross_profit"] += ret
        stats["max_win"] = max(stats["max_win"], ret)
        if side == "long": stats["long_win"] += 1
        else: stats["short_win"] += 1
    elif ret < -0.3:
        stats["loss"] += 1
        stats["gross_loss"] += abs(ret)
        stats["max_loss"] = max(stats["max_loss"], abs(ret))
    else:
        stats["breakeven"] += 1

    stats[f"{exit_type}_count"] += 1
    rd = stats["regimes"][regime]
    rd["total"] += 1
    if is_win: rd["win"] += 1
    elif ret < -0.3: rd["loss"] += 1


def run_portfolio_backtest(initial_budget=100_000):
    cash = initial_budget
    fee_rate = 0.0002

    shares = avg_price = margin_used = 0.0
    side = None
    entry_idx = last_exit_idx = -9999
    stop_price = None
    best_price = 0.0
    entry_regime = None
    partial_done = False

    total_fees = 0.0
    history = []
    monthly_pnl = {}
    peak_equity = initial_budget
    consec_stop = 0
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
            "BULL": {"total": 0, "win": 0, "loss": 0},
            "BEAR": {"total": 0, "win": 0, "loss": 0},
            "RANGE": {"total": 0, "win": 0, "loss": 0},
        }
    }

    # ── 데이터 로드 (4h만 필요) ──
    data_5m = get_binance_data(
        symbol="BTC/USDT", interval="5m",
        start_date="2023-04-01 00:00:00",
        end_date="2026-04-09 23:59:59"
    )
    data_4h = get_binance_data(
        symbol="BTC/USDT", interval="4h",
        start_date="2023-01-01 00:00:00",
        end_date="2026-04-09 23:59:59"
    )

    if data_4h.empty:
        return [initial_budget], 0.0, stats, {}

    df = prep_data_vectorized(data_5m, data_4h)

    print(f"\n[V55 v14] 4h EMA trend | trail ATR*{ATR_TRAIL_MULT} | partial@{PARTIAL_TRIGGER}%")
    print(f"{'time':<14} | {'d':<1} | {'action':<7} | {'price':>9} | {'ret':>7} | {'fee':>6} | {'cash':>11} | note")
    print("-" * 105)

    for row in df.itertuples():
        i = row.Index_Num
        if i < 60:
            continue

        cur_date = row.Index.date()
        cur_ym = (row.Index.year, row.Index.month)
        time_str = row.Index.strftime('%y-%m-%d %H:%M')
        cur_close = row.Close
        cur_open = row.Open
        cur_high = row.High
        cur_low = row.Low
        atr_val = row.atr

        is_buy = bool(row.buy)
        is_short = bool(row.short)
        is_sell = bool(row.sell)
        is_cover = bool(row.cover)

        is_bull = bool(row.trend_bull)
        is_bear = bool(row.trend_bear)
        cur_regime = "BULL" if is_bull else ("BEAR" if is_bear else "RANGE")

        daily_loss_today = daily_loss_tracker.get(cur_date, 0.0)
        current_equity = cash
        if shares > 0 and side == "long":
            current_equity += margin_used + shares * (cur_close - avg_price)
        elif shares > 0 and side == "short":
            current_equity += margin_used + shares * (avg_price - cur_close)
        ref_equity = max(peak_equity, initial_budget)
        entry_blocked = (daily_loss_today / ref_equity * 100) < MAX_DAILY_LOSS_PCT

        # ══════════════════════════════════
        # 포지션 관리 (트레일링 스탑 + 조건 청산)
        # ══════════════════════════════════
        if shares > 0:
            candles_held = i - entry_idx
            lev = LEVERAGE_BULL if side == "long" else LEVERAGE_BEAR

            if side == "long":
                # 트레일링 스탑 상향
                if cur_high > best_price:
                    best_price = cur_high
                new_stop = best_price - atr_val * ATR_TRAIL_MULT
                if new_stop > stop_price:
                    stop_price = new_stop

                hit_stop = cur_low <= stop_price
                margin_ret = (cur_close - avg_price) / avg_price * 100 * lev

                # 분할 익절
                if not partial_done and margin_ret >= PARTIAL_TRIGGER:
                    p_qty = shares * PARTIAL_RATIO
                    fee = p_qty * cur_close * fee_rate
                    pnl = p_qty * (cur_close - avg_price)
                    total_fees += fee
                    cash += margin_used * PARTIAL_RATIO + (pnl - fee)
                    shares -= p_qty
                    margin_used *= (1 - PARTIAL_RATIO)
                    partial_done = True
                    stats["partial_exit_count"] += 1
                    # 스탑을 손익분기로
                    be = avg_price * 1.002
                    if be > stop_price: stop_price = be
                    pr = pnl / (margin_used / (1 - PARTIAL_RATIO)) * 100
                    print(f"{time_str} | L | PART   | {cur_close:>9,.0f} | {pr:>6.1f}% | {fee:>5.0f} | {cash:>11,.0f} | 40%@{PARTIAL_TRIGGER}%")

                if hit_stop or (is_sell and candles_held >= 3) or candles_held >= MAX_HOLD:
                    fp = stop_price if hit_stop else cur_close
                    fee = shares * fp * fee_rate
                    net = shares * (fp - avg_price) - fee
                    final_ret = (shares * (fp - avg_price)) / margin_used * 100 if margin_used > 0 else 0
                    total_fees += fee
                    cash += margin_used + net

                    if hit_stop:
                        etype = "trailing" if final_ret > 0 else "stop"
                        remark = "TRAIL" if final_ret > 0 else "STOP"
                    elif candles_held >= MAX_HOLD:
                        etype = "cond_exit"; remark = "TIME"
                    else:
                        etype = "cond_exit"; remark = "SIGNAL"

                    _update_stats(stats, "long", final_ret, etype, entry_regime)
                    monthly_pnl[cur_ym] = monthly_pnl.get(cur_ym, 0.0) + net
                    if net < 0:
                        consec_stop += 1
                        daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                    else:
                        consec_stop = 0

                    last_exit_idx = i
                    print(f"{time_str} | L | EXIT   | {fp:>9,.0f} | {final_ret:>6.1f}% | {fee:>5.0f} | {cash:>11,.0f} | {remark} ({candles_held}bars)")
                    shares = avg_price = margin_used = best_price = 0.0
                    stop_price = None; side = None; entry_regime = None; partial_done = False

            elif side == "short":
                if cur_low < best_price:
                    best_price = cur_low
                new_stop = best_price + atr_val * ATR_TRAIL_MULT
                if new_stop < stop_price:
                    stop_price = new_stop

                hit_stop = cur_high >= stop_price
                margin_ret = (avg_price - cur_close) / avg_price * 100 * lev

                if not partial_done and margin_ret >= PARTIAL_TRIGGER:
                    p_qty = shares * PARTIAL_RATIO
                    fee = p_qty * cur_close * fee_rate
                    pnl = p_qty * (avg_price - cur_close)
                    total_fees += fee
                    cash += margin_used * PARTIAL_RATIO + (pnl - fee)
                    shares -= p_qty
                    margin_used *= (1 - PARTIAL_RATIO)
                    partial_done = True
                    stats["partial_exit_count"] += 1
                    be = avg_price * 0.998
                    if be < stop_price: stop_price = be
                    pr = pnl / (margin_used / (1 - PARTIAL_RATIO)) * 100
                    print(f"{time_str} | S | PART   | {cur_close:>9,.0f} | {pr:>6.1f}% | {fee:>5.0f} | {cash:>11,.0f} | 40%@{PARTIAL_TRIGGER}%")

                if hit_stop or (is_cover and candles_held >= 3) or candles_held >= MAX_HOLD:
                    fp = stop_price if hit_stop else cur_close
                    fee = shares * fp * fee_rate
                    net = shares * (avg_price - fp) - fee
                    final_ret = (shares * (avg_price - fp)) / margin_used * 100 if margin_used > 0 else 0
                    total_fees += fee
                    cash += margin_used + net

                    if hit_stop:
                        etype = "trailing" if final_ret > 0 else "stop"
                        remark = "TRAIL" if final_ret > 0 else "STOP"
                    elif candles_held >= MAX_HOLD:
                        etype = "cond_exit"; remark = "TIME"
                    else:
                        etype = "cond_exit"; remark = "SIGNAL"

                    _update_stats(stats, "short", final_ret, etype, entry_regime)
                    monthly_pnl[cur_ym] = monthly_pnl.get(cur_ym, 0.0) + net
                    if net < 0:
                        consec_stop += 1
                        daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                    else:
                        consec_stop = 0

                    last_exit_idx = i
                    print(f"{time_str} | S | EXIT   | {fp:>9,.0f} | {final_ret:>6.1f}% | {fee:>5.0f} | {cash:>11,.0f} | {remark} ({candles_held}bars)")
                    shares = avg_price = margin_used = best_price = 0.0
                    stop_price = None; side = None; entry_regime = None; partial_done = False

        # 자산 / MDD
        current_equity = cash
        if shares > 0 and side == "long":
            current_equity += margin_used + shares * (cur_close - avg_price)
        elif shares > 0 and side == "short":
            current_equity += margin_used + shares * (avg_price - cur_close)
        if current_equity > peak_equity:
            peak_equity = current_equity
        dd = (current_equity - peak_equity) / peak_equity * 100
        stats["max_drawdown"] = min(stats["max_drawdown"], dd)

        # ══════════════════════════════════
        # 롱 진입
        # ══════════════════════════════════
        if is_buy and shares == 0 and (i - last_exit_idx) >= COOLDOWN and not entry_blocked:
            pos = calc_pos(consec_stop)
            lev = LEVERAGE_BULL
            margin_use = current_equity * pos

            if cash > margin_use * (1 + lev * fee_rate):
                notional = margin_use * lev
                entry_exec = cur_open
                fee = notional * fee_rate
                total_fees += fee
                avg_price = entry_exec
                shares = notional / entry_exec
                side = "long"
                cash -= (margin_use + fee)
                margin_used = margin_use
                entry_idx = i
                entry_regime = cur_regime
                best_price = cur_high
                stop_price = entry_exec - atr_val * ATR_TRAIL_MULT
                partial_done = False
                stop_pct = (entry_exec - stop_price) / entry_exec * 100
                cs = f" [cs:{consec_stop}]" if consec_stop > 0 else ""
                print(f"{time_str} | L | LONG   | {entry_exec:>9,.0f} | {'':>7} | {fee:>5.0f} | {cash:>11,.0f} | lev:{lev}x,stop-{stop_pct:.1f}%{cs}")

        # ══════════════════════════════════
        # 숏 진입
        # ══════════════════════════════════
        if is_short and shares == 0 and (i - last_exit_idx) >= COOLDOWN and not entry_blocked:
            pos = calc_pos(consec_stop)
            lev = LEVERAGE_BEAR
            margin_use = current_equity * pos

            if cash > margin_use * (1 + lev * fee_rate):
                notional = margin_use * lev
                entry_exec = cur_open
                fee = notional * fee_rate
                total_fees += fee
                avg_price = entry_exec
                shares = notional / entry_exec
                side = "short"
                cash -= (margin_use + fee)
                margin_used = margin_use
                entry_idx = i
                entry_regime = cur_regime
                best_price = cur_low
                stop_price = entry_exec + atr_val * ATR_TRAIL_MULT
                partial_done = False
                stop_pct = (stop_price - entry_exec) / entry_exec * 100
                cs = f" [cs:{consec_stop}]" if consec_stop > 0 else ""
                print(f"{time_str} | S | SHORT  | {entry_exec:>9,.0f} | {'':>7} | {fee:>5.0f} | {cash:>11,.0f} | lev:{lev}x,stop+{stop_pct:.1f}%{cs}")

        history.append(current_equity)

    # 강제 청산
    if shares > 0:
        final_price = df['Close'].iloc[-1]
        if side == "long":
            pnl = shares * (final_price - avg_price)
        else:
            pnl = shares * (avg_price - final_price)
        fee = shares * final_price * fee_rate
        cash += margin_used + pnl - fee
        history.append(cash)

    return history if history else [initial_budget], total_fees, stats, monthly_pnl
