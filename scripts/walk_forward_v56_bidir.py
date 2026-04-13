"""
V56-bidir Walk-Forward 검증

IS (In-Sample): 2023-01 ~ 2024-06 (18개월) - 파라미터 훈련 구간
OOS (Out-of-Sample): 2024-07 ~ 2026-04 (21개월) - 검증 구간

롱+숏 양방향 전략의 과적합 여부 판단
"""
import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pandas as pd
from data.collector import get_binance_data
from indicators.calculators_v56_bidir import calc_signals_for_asset

# ── 롱 파라미터 (V56-plus 동일) ──
ATR_TRAIL_MULT_EMA = 4.0
ATR_TRAIL_MULT_DON = 3.0
COOLDOWN = 3
MAX_HOLD = 120
PARTIAL_TRIGGER = 6.0
PARTIAL_RATIO = 0.4
MAX_DAILY_LOSS_PCT = -4.0
MAX_POSITIONS = 3
MAX_TOTAL_EXPOSURE = 0.90

# ── 숏 파라미터 ──
SHORT_ATR_TRAIL_MULT_EMA = 3.5
SHORT_ATR_TRAIL_MULT_DON = 2.5
SHORT_COOLDOWN = 4
SHORT_MAX_HOLD = 60
SHORT_PARTIAL_TRIGGER = 5.0
SHORT_PARTIAL_RATIO = 0.5
SHORT_POS_MULT = 0.6
SHORT_MAX_POSITIONS = 2

ASSETS = {
    "BTC":  {"symbol": "BTC/USDT",  "pos_base": 0.38},
    "ETH":  {"symbol": "ETH/USDT",  "pos_base": 0.32},
    "SOL":  {"symbol": "SOL/USDT",  "pos_base": 0.25},
}

LONG_PAIRS = {
    ("BTC", "EMA"), ("ETH", "EMA"), ("ETH", "DON"), ("SOL", "DON"),
}
SHORT_PAIRS = {
    ("BTC", "EMA"), ("ETH", "EMA"),
}

STRATEGIES = {
    "EMA": {"trail_mult_long": ATR_TRAIL_MULT_EMA, "trail_mult_short": SHORT_ATR_TRAIL_MULT_EMA},
    "DON": {"trail_mult_long": ATR_TRAIL_MULT_DON, "trail_mult_short": SHORT_ATR_TRAIL_MULT_DON},
}


def calc_dynamic_leverage_long(adx_val):
    if pd.isna(adx_val) or adx_val < 25:
        return 4
    elif adx_val < 35:
        return 5
    return 6


def calc_dynamic_leverage_short(adx_val):
    if pd.isna(adx_val) or adx_val < 25:
        return 3
    elif adx_val < 35:
        return 3
    return 4


def calc_dynamic_pos(base_pos, adx_val, consec_stop):
    if not pd.isna(adx_val) and adx_val > 30:
        adx_mult = 1.15
    elif not pd.isna(adx_val) and adx_val > 25:
        adx_mult = 1.0
    else:
        adx_mult = 0.85
    if consec_stop >= 3:
        stop_mult = 0.5
    elif consec_stop >= 2:
        stop_mult = 0.7
    else:
        stop_mult = 1.0
    return base_pos * adx_mult * stop_mult


class Position:
    def __init__(self, asset, strategy, side, shares, avg_price,
                 margin_used, stop_price, best_price, entry_idx,
                 leverage, trail_mult):
        self.asset = asset
        self.strategy = strategy
        self.side = side
        self.shares = shares
        self.avg_price = avg_price
        self.margin_used = margin_used
        self.stop_price = stop_price
        self.best_price = best_price
        self.entry_idx = entry_idx
        self.leverage = leverage
        self.trail_mult = trail_mult
        self.partial_done = False


def run_period_backtest(asset_data, timeline, initial_budget, label=""):
    cash = initial_budget
    fee_rate = 0.0002
    positions = []
    total_fees = 0.0
    history = []
    monthly_pnl = {}
    peak_equity = initial_budget
    consec_stop = {}
    daily_loss_tracker = {}
    last_exit = {}

    stats = {
        "total": 0, "win": 0, "loss": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "max_drawdown": 0.0, "by_strat": {},
        "long_count": 0, "short_count": 0,
    }

    for ts in timeline:
        cur_date = ts.date()
        cur_ym = (ts.year, ts.month)

        daily_loss_today = daily_loss_tracker.get(cur_date, 0.0)
        ref_equity = max(peak_equity, initial_budget)
        entry_blocked = (daily_loss_today / ref_equity * 100) < MAX_DAILY_LOSS_PCT

        # ── 포지션 관리 (롱+숏) ──
        closed = []
        for pos in positions[:]:
            if ts not in asset_data[pos.asset].index:
                continue
            row = asset_data[pos.asset].loc[ts]
            i = int(row['Index_Num'])
            cur_close, cur_high, cur_low = row['Close'], row['High'], row['Low']
            atr_val = row['atr']
            candles_held = i - pos.entry_idx

            max_hold = MAX_HOLD if pos.side == "long" else SHORT_MAX_HOLD
            p_trigger = PARTIAL_TRIGGER if pos.side == "long" else SHORT_PARTIAL_TRIGGER
            p_ratio = PARTIAL_RATIO if pos.side == "long" else SHORT_PARTIAL_RATIO

            # 청산 시그널
            if pos.side == "long":
                is_exit = bool(row['ema_sell']) if pos.strategy == "EMA" else bool(row['don_sell'])
            else:
                is_exit = bool(row['ema_cover']) if pos.strategy == "EMA" else bool(row['don_cover'])

            # 트레일링 스탑
            if pos.side == "long":
                if cur_high > pos.best_price:
                    pos.best_price = cur_high
                new_stop = pos.best_price - atr_val * pos.trail_mult
                if new_stop > pos.stop_price:
                    pos.stop_price = new_stop
                hit_stop = cur_low <= pos.stop_price
                margin_ret = (cur_close - pos.avg_price) / pos.avg_price * 100 * pos.leverage
            else:
                if cur_low < pos.best_price:
                    pos.best_price = cur_low
                new_stop = pos.best_price + atr_val * pos.trail_mult
                if new_stop < pos.stop_price:
                    pos.stop_price = new_stop
                hit_stop = cur_high >= pos.stop_price
                margin_ret = (pos.avg_price - cur_close) / pos.avg_price * 100 * pos.leverage

            # 분할 익절
            if not pos.partial_done and margin_ret >= p_trigger:
                p_qty = pos.shares * p_ratio
                fee = p_qty * cur_close * fee_rate
                if pos.side == "long":
                    pnl = p_qty * (cur_close - pos.avg_price)
                else:
                    pnl = p_qty * (pos.avg_price - cur_close)
                total_fees += fee
                cash += pos.margin_used * p_ratio + (pnl - fee)
                pos.shares -= p_qty
                pos.margin_used *= (1 - p_ratio)
                pos.partial_done = True
                if pos.side == "long":
                    be = pos.avg_price * 1.002
                    if be > pos.stop_price:
                        pos.stop_price = be
                else:
                    be = pos.avg_price * 0.998
                    if be < pos.stop_price:
                        pos.stop_price = be

            if hit_stop or (is_exit and candles_held >= 3) or candles_held >= max_hold:
                fp = pos.stop_price if hit_stop else cur_close
                fee = pos.shares * fp * fee_rate
                if pos.side == "long":
                    net = pos.shares * (fp - pos.avg_price) - fee
                    final_ret = (pos.shares * (fp - pos.avg_price)) / pos.margin_used * 100 if pos.margin_used > 0 else 0
                else:
                    net = pos.shares * (pos.avg_price - fp) - fee
                    final_ret = (pos.shares * (pos.avg_price - fp)) / pos.margin_used * 100 if pos.margin_used > 0 else 0

                total_fees += fee
                cash += pos.margin_used + net

                stats["total"] += 1
                key = f"{pos.asset}_{pos.strategy}_{pos.side}"
                if key not in stats["by_strat"]:
                    stats["by_strat"][key] = {"total": 0, "win": 0, "loss": 0, "profit": 0.0, "loss_amt": 0.0}
                sd = stats["by_strat"][key]
                sd["total"] += 1

                if final_ret > 0.3:
                    stats["win"] += 1
                    stats["gross_profit"] += final_ret
                    sd["win"] += 1
                    sd["profit"] += final_ret
                elif final_ret < -0.3:
                    stats["loss"] += 1
                    stats["gross_loss"] += abs(final_ret)
                    sd["loss"] += 1
                    sd["loss_amt"] += abs(final_ret)

                monthly_pnl[cur_ym] = monthly_pnl.get(cur_ym, 0.0) + net
                if net < 0:
                    consec_stop[pos.asset] = consec_stop.get(pos.asset, 0) + 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consec_stop[pos.asset] = 0
                last_exit[(pos.asset, pos.strategy, pos.side)] = i
                closed.append(pos)

        for p in closed:
            positions.remove(p)

        # 자산 평가
        current_equity = cash
        for pos in positions:
            if ts in asset_data[pos.asset].index:
                row = asset_data[pos.asset].loc[ts]
                if pos.side == "long":
                    current_equity += pos.margin_used + pos.shares * (row['Close'] - pos.avg_price)
                else:
                    current_equity += pos.margin_used + pos.shares * (pos.avg_price - row['Close'])

        if current_equity > peak_equity:
            peak_equity = current_equity
        dd = (current_equity - peak_equity) / peak_equity * 100
        stats["max_drawdown"] = min(stats["max_drawdown"], dd)

        # ── 신규 진입 ──
        if entry_blocked or len(positions) >= MAX_POSITIONS:
            history.append(current_equity)
            continue

        btc_bullish = True
        btc_bearish = False
        if "BTC" in asset_data and ts in asset_data["BTC"].index:
            btc_row = asset_data["BTC"].loc[ts]
            btc_bullish = bool(btc_row['trend_bull'])
            btc_bearish = bool(btc_row['trend_bear'])

        total_margin_used = sum(p.margin_used for p in positions)
        margin_available = current_equity * MAX_TOTAL_EXPOSURE - total_margin_used
        active_keys = {(p.asset, p.strategy, p.side) for p in positions}
        short_positions = [p for p in positions if p.side == "short"]

        for aname, acfg in ASSETS.items():
            if aname not in asset_data or ts not in asset_data[aname].index:
                continue
            row = asset_data[aname].loc[ts]
            i = int(row['Index_Num'])
            if i < 60:
                continue

            cur_open, cur_high, cur_low = row['Open'], row['High'], row['Low']
            atr_val, adx_val = row['atr'], row['adx']

            for sname, scfg in STRATEGIES.items():
                if len(positions) >= MAX_POSITIONS:
                    break

                # 롱 진입
                if btc_bullish and (aname, sname) in LONG_PAIRS:
                    if (aname, sname, "long") not in active_keys:
                        lei = last_exit.get((aname, sname, "long"), -9999)
                        if (i - lei) >= COOLDOWN:
                            is_buy = bool(row['ema_buy']) if sname == "EMA" else bool(row['don_buy'])
                            if is_buy and len(positions) < MAX_POSITIONS:
                                lev = calc_dynamic_leverage_long(adx_val)
                                cs = consec_stop.get(aname, 0)
                                pos_pct = calc_dynamic_pos(acfg["pos_base"], adx_val, cs)
                                margin_use = current_equity * pos_pct
                                if margin_use > margin_available:
                                    margin_use = margin_available * 0.9
                                if margin_use >= current_equity * 0.05 and cash >= margin_use * (1 + lev * fee_rate):
                                    notional = margin_use * lev
                                    entry_exec = cur_open
                                    fee = notional * fee_rate
                                    total_fees += fee
                                    shares = notional / entry_exec
                                    stop_price = entry_exec - atr_val * scfg["trail_mult_long"]
                                    cash -= (margin_use + fee)
                                    margin_available -= margin_use
                                    positions.append(Position(
                                        aname, sname, "long", shares, entry_exec,
                                        margin_use, stop_price, cur_high, i, lev, scfg["trail_mult_long"]
                                    ))
                                    active_keys.add((aname, sname, "long"))
                                    stats["long_count"] += 1

                # 숏 진입
                if btc_bearish and (aname, sname) in SHORT_PAIRS:
                    if len(short_positions) >= SHORT_MAX_POSITIONS:
                        continue
                    if (aname, sname, "short") not in active_keys:
                        lei = last_exit.get((aname, sname, "short"), -9999)
                        if (i - lei) >= SHORT_COOLDOWN:
                            is_short = bool(row['ema_short']) if sname == "EMA" else bool(row.get('don_short', False))
                            if is_short and len(positions) < MAX_POSITIONS:
                                lev = calc_dynamic_leverage_short(adx_val)
                                cs = consec_stop.get(aname, 0)
                                pos_pct = calc_dynamic_pos(acfg["pos_base"], adx_val, cs) * SHORT_POS_MULT
                                margin_use = current_equity * pos_pct
                                if margin_use > margin_available:
                                    margin_use = margin_available * 0.9
                                if margin_use >= current_equity * 0.05 and cash >= margin_use * (1 + lev * fee_rate):
                                    notional = margin_use * lev
                                    entry_exec = cur_open
                                    fee = notional * fee_rate
                                    total_fees += fee
                                    shares = notional / entry_exec
                                    stop_price = entry_exec + atr_val * scfg["trail_mult_short"]
                                    cash -= (margin_use + fee)
                                    margin_available -= margin_use
                                    positions.append(Position(
                                        aname, sname, "short", shares, entry_exec,
                                        margin_use, stop_price, cur_low, i, lev, scfg["trail_mult_short"]
                                    ))
                                    active_keys.add((aname, sname, "short"))
                                    short_positions.append(positions[-1])
                                    stats["short_count"] += 1

        history.append(current_equity)

    # 강제 청산
    if positions:
        for pos in positions:
            df = asset_data[pos.asset]
            fp = df['Close'].iloc[-1]
            if pos.side == "long":
                pnl = pos.shares * (fp - pos.avg_price)
            else:
                pnl = pos.shares * (pos.avg_price - fp)
            fee = pos.shares * fp * fee_rate
            cash += pos.margin_used + pnl - fee
        current_equity = cash
        history.append(current_equity)

    return history, stats, monthly_pnl


def main():
    print("=" * 70)
    print("  V56-bidir Walk-Forward 검증 (롱+숏 양방향)")
    print("  IS: 2023-01 ~ 2024-06 | OOS: 2024-07 ~ 2026-04")
    print("=" * 70)

    print("\n[데이터 로딩]")
    asset_data_full = {}
    for aname, acfg in ASSETS.items():
        d4h = get_binance_data(
            symbol=acfg["symbol"], interval="4h",
            start_date="2023-01-01 00:00:00",
            end_date="2026-04-09 23:59:59"
        )
        if d4h.empty:
            continue
        df = calc_signals_for_asset(d4h, aname)
        asset_data_full[aname] = df
        print(f"  {aname}: {len(df)}봉")

    # IS/OOS 분할
    is_cutoff = pd.Timestamp("2024-07-01")

    asset_data_is = {}
    asset_data_oos = {}
    for aname, df in asset_data_full.items():
        asset_data_is[aname] = df[df.index < is_cutoff]
        asset_data_oos[aname] = df[df.index >= is_cutoff]

    is_indices = set()
    for df in asset_data_is.values():
        is_indices.update(df.index.tolist())
    is_timeline = sorted(is_indices)

    oos_indices = set()
    for df in asset_data_oos.values():
        oos_indices.update(df.index.tolist())
    oos_timeline = sorted(oos_indices)

    print(f"\n  IS: {len(is_timeline)}봉 | OOS: {len(oos_timeline)}봉")

    # IS 실행
    print("\n[IS 백테스트 실행중...]")
    is_hist, is_stats, is_monthly = run_period_backtest(
        asset_data_is, is_timeline, 100_000, "IS"
    )

    # OOS 실행
    print("[OOS 백테스트 실행중...]")
    oos_hist, oos_stats, oos_monthly = run_period_backtest(
        asset_data_oos, oos_timeline, 100_000, "OOS"
    )

    # 결과 계산
    is_final = is_hist[-1] if is_hist else 100_000
    oos_final = oos_hist[-1] if oos_hist else 100_000
    is_ret = (is_final / 100_000 - 1) * 100
    oos_ret = (oos_final / 100_000 - 1) * 100
    is_months = len(is_monthly)
    oos_months = len(oos_monthly)
    is_monthly_avg = is_ret / max(is_months, 1)
    oos_monthly_avg = oos_ret / max(oos_months, 1)

    is_pf = is_stats["gross_profit"] / is_stats["gross_loss"] if is_stats["gross_loss"] > 0 else float('inf')
    oos_pf = oos_stats["gross_profit"] / oos_stats["gross_loss"] if oos_stats["gross_loss"] > 0 else float('inf')

    is_wr = is_stats["win"] / max(is_stats["total"], 1) * 100
    oos_wr = oos_stats["win"] / max(oos_stats["total"], 1) * 100

    print("\n" + "=" * 70)
    print("  V56-bidir Walk-Forward 검증 결과 (롱+숏)")
    print("=" * 70)
    print(f"{'':>20} | {'IS (23.01-24.06)':>20} | {'OOS (24.07-26.04)':>20}")
    print("-" * 65)
    print(f"  {'최종 자산':>16} | ${is_final:>18,.0f} | ${oos_final:>18,.0f}")
    print(f"  {'총 수익률':>16} | {is_ret:>18.1f}% | {oos_ret:>18.1f}%")
    print(f"  {'월평균 수익률':>14} | {is_monthly_avg:>18.2f}% | {oos_monthly_avg:>18.2f}%")
    print(f"  {'거래 수':>16} | {is_stats['total']:>18} | {oos_stats['total']:>18}")
    print(f"  {'  롱':>18} | {is_stats['long_count']:>18} | {oos_stats['long_count']:>18}")
    print(f"  {'  숏':>18} | {is_stats['short_count']:>18} | {oos_stats['short_count']:>18}")
    print(f"  {'승률':>18} | {is_wr:>17.1f}% | {oos_wr:>17.1f}%")
    print(f"  {'PF':>18} | {is_pf:>18.2f} | {oos_pf:>18.2f}")
    print(f"  {'MDD':>18} | {is_stats['max_drawdown']:>17.1f}% | {oos_stats['max_drawdown']:>17.1f}%")

    # 과적합 판단
    pf_ratio = oos_pf / is_pf if is_pf > 0 else 0
    monthly_ratio = oos_monthly_avg / is_monthly_avg if is_monthly_avg > 0 else 0

    print(f"\n  [과적합 판단]")
    print(f"  OOS/IS PF 비율:     {pf_ratio:.2f} (1.0에 가까울수록 좋음, >0.7 양호)")
    print(f"  OOS/IS 월수익 비율: {monthly_ratio:.2f} (1.0에 가까울수록 좋음, >0.7 양호)")

    if pf_ratio >= 0.8:
        verdict = "[PASS] 과적합 징후 없음 (매우 양호)"
    elif pf_ratio >= 0.6:
        verdict = "[OK] 경미한 과적합 (사용 가능, 모니터링 필요)"
    elif pf_ratio >= 0.4:
        verdict = "[WARN] 과적합 의심 (주의 필요)"
    else:
        verdict = "[FAIL] 심각한 과적합 (사용 위험)"

    print(f"  판정: {verdict}")

    # V56-plus WF 결과와 비교
    print(f"\n  [V56-plus(롱only) Walk-Forward 비교]")
    print(f"    V56-plus:  OOS/IS PF비율 1.15 | OOS PF 2.75 | OOS 월 7.69%")
    print(f"    V56-bidir: OOS/IS PF비율 {pf_ratio:.2f} | OOS PF {oos_pf:.2f} | OOS 월 {oos_monthly_avg:.2f}%")

    # 숏 전략 별도 분석
    print(f"\n  [숏 전략 IS vs OOS 비교]")
    all_short_keys = set()
    for key in list(is_stats["by_strat"].keys()) + list(oos_stats["by_strat"].keys()):
        if "_short" in key:
            all_short_keys.add(key)

    for key in sorted(all_short_keys):
        is_sd = is_stats["by_strat"].get(key, {"total": 0, "win": 0, "profit": 0.0, "loss_amt": 0.0})
        oos_sd = oos_stats["by_strat"].get(key, {"total": 0, "win": 0, "profit": 0.0, "loss_amt": 0.0})
        is_spf = is_sd["profit"] / is_sd["loss_amt"] if is_sd["loss_amt"] > 0 else float('inf')
        oos_spf = oos_sd["profit"] / oos_sd["loss_amt"] if oos_sd["loss_amt"] > 0 else float('inf')
        is_swr = is_sd["win"] / max(is_sd["total"], 1) * 100
        oos_swr = oos_sd["win"] / max(oos_sd["total"], 1) * 100
        print(f"    {key}:")
        print(f"      IS:  {is_sd['total']}건 | 승률 {is_swr:.0f}% | PF {is_spf:.2f}")
        print(f"      OOS: {oos_sd['total']}건 | 승률 {oos_swr:.0f}% | PF {oos_spf:.2f}")

    # OOS 전략별 전체
    print(f"\n  [OOS 전략별 상세]")
    for key, sd in sorted(oos_stats["by_strat"].items()):
        wr = sd['win'] / max(sd['total'], 1) * 100
        spf = sd['profit'] / sd['loss_amt'] if sd['loss_amt'] > 0 else float('inf')
        print(f"    {key}: {sd['total']}건 | 승률 {wr:.0f}% | PF {spf:.2f}")

    # OOS 월별
    print(f"\n  [OOS 월별 손익]")
    for ym in sorted(oos_monthly.keys()):
        mp = oos_monthly[ym]
        pct = mp / 100_000 * 100
        print(f"    {ym[0]}-{ym[1]:02d}: ${mp:>+10,.0f} ({pct:>+.1f}%)")


if __name__ == "__main__":
    main()
