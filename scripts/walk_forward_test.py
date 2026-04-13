"""
Walk-Forward 검증 스크립트

과적합 여부를 판단하기 위해:
1. In-Sample (IS):  2023-01 ~ 2024-06 (18개월) - 파라미터가 최적화된 구간
2. Out-of-Sample (OOS): 2024-07 ~ 2026-04 (21개월) - 미래 데이터
3. 전체 기간: 2023-01 ~ 2026-04 (39개월) - 기존 결과

IS와 OOS의 PF, 승률, 월수익률이 비슷하면 → 과적합 아님
OOS에서 급격히 나빠지면 → 과적합
"""
import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pandas as pd
from data.collector import get_binance_data
from indicators.calculators_v56 import calc_signals_for_asset

# V56 엔진 파라미터 그대로 사용
ATR_TRAIL_MULT_EMA = 4.0
ATR_TRAIL_MULT_DON = 3.0
COOLDOWN = 6
MAX_HOLD = 120
PARTIAL_TRIGGER = 8.0
PARTIAL_RATIO = 0.4
MAX_DAILY_LOSS_PCT = -4.0
MAX_POSITIONS = 2
MAX_TOTAL_EXPOSURE = 0.85
FEE_RATE = 0.0002

ASSETS = {
    "BTC": {"symbol": "BTC/USDT", "pos_base": 0.38},
    "ETH": {"symbol": "ETH/USDT", "pos_base": 0.32},
    "SOL": {"symbol": "SOL/USDT", "pos_base": 0.25},
}

ALLOWED_PAIRS = {
    ("BTC", "EMA"), ("ETH", "EMA"), ("ETH", "DON"), ("SOL", "DON"),
}

STRATEGIES = {
    "EMA": {"trail_mult": ATR_TRAIL_MULT_EMA},
    "DON": {"trail_mult": ATR_TRAIL_MULT_DON},
}


def calc_dynamic_leverage(adx_val):
    if pd.isna(adx_val) or adx_val < 25:
        return 3
    return 4


def calc_dynamic_pos(base_pos, adx_val, consec_stop):
    adx_mult = 1.15 if (not pd.isna(adx_val) and adx_val > 30) else (1.0 if (not pd.isna(adx_val) and adx_val > 25) else 0.85)
    stop_mult = 0.5 if consec_stop >= 3 else (0.7 if consec_stop >= 2 else 1.0)
    return base_pos * adx_mult * stop_mult


class Position:
    __slots__ = ['asset','strategy','side','shares','avg_price','margin_used',
                 'stop_price','best_price','entry_idx','leverage','trail_mult','partial_done']
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)
        self.partial_done = False


def run_backtest_period(asset_data, start_date, end_date, initial_budget=100_000):
    """특정 기간만 백테스트"""
    cash = initial_budget
    positions = []
    total_fees = 0.0
    history = []
    monthly_pnl = {}
    peak_equity = initial_budget
    consec_stop = {}
    daily_loss_tracker = {}
    last_exit = {}

    stats = {
        "total": 0, "win": 0, "loss": 0, "breakeven": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "max_drawdown": 0.0, "by_strat": {},
        "stop_count": 0, "trailing_count": 0,
        "cond_exit_count": 0, "partial_exit_count": 0,
    }

    # 통합 타임라인
    all_indices = set()
    for df in asset_data.values():
        all_indices.update(df.index.tolist())
    timeline = sorted([ts for ts in all_indices if start_date <= ts <= end_date])

    for ts in timeline:
        cur_date = ts.date()
        cur_ym = (ts.year, ts.month)

        daily_loss_today = daily_loss_tracker.get(cur_date, 0.0)
        ref_equity = max(peak_equity, initial_budget)
        entry_blocked = (daily_loss_today / ref_equity * 100) < MAX_DAILY_LOSS_PCT

        # 포지션 관리
        closed = []
        for pos in positions[:]:
            if ts not in asset_data[pos.asset].index:
                continue
            row = asset_data[pos.asset].loc[ts]
            i = int(row['Index_Num'])
            cur_close, cur_high, cur_low = row['Close'], row['High'], row['Low']
            atr_val = row['atr']
            candles_held = i - pos.entry_idx

            if pos.strategy == "EMA":
                is_exit_signal = bool(row['ema_sell'])
            else:
                is_exit_signal = bool(row['don_sell'])

            if pos.side == "long":
                if cur_high > pos.best_price:
                    pos.best_price = cur_high
                new_stop = pos.best_price - atr_val * pos.trail_mult
                if new_stop > pos.stop_price:
                    pos.stop_price = new_stop

                hit_stop = cur_low <= pos.stop_price
                margin_ret = (cur_close - pos.avg_price) / pos.avg_price * 100 * pos.leverage

                if not pos.partial_done and margin_ret >= PARTIAL_TRIGGER:
                    p_qty = pos.shares * PARTIAL_RATIO
                    fee = p_qty * cur_close * FEE_RATE
                    pnl = p_qty * (cur_close - pos.avg_price)
                    total_fees += fee
                    cash += pos.margin_used * PARTIAL_RATIO + (pnl - fee)
                    pos.shares -= p_qty
                    pos.margin_used *= (1 - PARTIAL_RATIO)
                    pos.partial_done = True
                    stats["partial_exit_count"] += 1
                    be = pos.avg_price * 1.002
                    if be > pos.stop_price:
                        pos.stop_price = be

                if hit_stop or (is_exit_signal and candles_held >= 3) or candles_held >= MAX_HOLD:
                    fp = pos.stop_price if hit_stop else cur_close
                    fee = pos.shares * fp * FEE_RATE
                    net = pos.shares * (fp - pos.avg_price) - fee
                    final_ret = (pos.shares * (fp - pos.avg_price)) / pos.margin_used * 100 if pos.margin_used > 0 else 0
                    total_fees += fee
                    cash += pos.margin_used + net

                    if hit_stop:
                        etype = "trailing" if final_ret > 0 else "stop"
                    elif candles_held >= MAX_HOLD:
                        etype = "cond_exit"
                    else:
                        etype = "cond_exit"

                    # stats
                    stats["total"] += 1
                    key = f"{pos.asset}_{pos.strategy}"
                    if key not in stats["by_strat"]:
                        stats["by_strat"][key] = {"total":0,"win":0,"loss":0,"profit":0.0,"loss_amt":0.0}
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
                    else:
                        stats["breakeven"] += 1
                    stats[f"{etype}_count"] += 1

                    monthly_pnl[cur_ym] = monthly_pnl.get(cur_ym, 0.0) + net
                    akey = pos.asset
                    if net < 0:
                        consec_stop[akey] = consec_stop.get(akey, 0) + 1
                        daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                    else:
                        consec_stop[akey] = 0
                    last_exit[(pos.asset, pos.strategy)] = i
                    closed.append(pos)

        for p in closed:
            positions.remove(p)

        # 자산 계산
        current_equity = cash
        for pos in positions:
            if ts in asset_data[pos.asset].index:
                r = asset_data[pos.asset].loc[ts]
                if pos.side == "long":
                    current_equity += pos.margin_used + pos.shares * (r['Close'] - pos.avg_price)
        if current_equity > peak_equity:
            peak_equity = current_equity
        dd = (current_equity - peak_equity) / peak_equity * 100
        stats["max_drawdown"] = min(stats["max_drawdown"], dd)

        # 진입
        if entry_blocked or len(positions) >= MAX_POSITIONS:
            history.append(current_equity)
            continue

        # BTC 추세 필터
        btc_bullish = True
        if "BTC" in asset_data and ts in asset_data["BTC"].index:
            btc_row = asset_data["BTC"].loc[ts]
            btc_bullish = bool(btc_row['trend_bull'])
        if not btc_bullish:
            history.append(current_equity)
            continue

        total_margin_used = sum(p.margin_used for p in positions)
        margin_available = current_equity * MAX_TOTAL_EXPOSURE - total_margin_used

        for aname, acfg in ASSETS.items():
            if aname not in asset_data or ts not in asset_data[aname].index:
                continue
            row = asset_data[aname].loc[ts]
            i = int(row['Index_Num'])
            if i < 60:
                continue

            active_keys = {(p.asset, p.strategy) for p in positions}

            for sname, scfg in STRATEGIES.items():
                if (aname, sname) not in ALLOWED_PAIRS:
                    continue
                if (aname, sname) in active_keys:
                    continue
                if len(positions) >= MAX_POSITIONS:
                    break

                lei = last_exit.get((aname, sname), -9999)
                if (i - lei) < COOLDOWN:
                    continue

                is_buy = bool(row['ema_buy']) if sname == "EMA" else bool(row['don_buy'])
                if not is_buy:
                    continue

                adx_val = row['adx']
                lev = calc_dynamic_leverage(adx_val)
                cs = consec_stop.get(aname, 0)
                pos_pct = calc_dynamic_pos(acfg["pos_base"], adx_val, cs)
                margin_use = current_equity * pos_pct

                if margin_use > margin_available:
                    margin_use = margin_available * 0.9
                if margin_use < current_equity * 0.05:
                    continue
                if cash < margin_use * (1 + lev * FEE_RATE):
                    continue

                notional = margin_use * lev
                entry_exec = row['Open']
                fee = notional * FEE_RATE
                total_fees += fee
                shares = notional / entry_exec
                stop_price = entry_exec - row['atr'] * scfg["trail_mult"]

                cash -= (margin_use + fee)
                margin_available -= margin_use

                new_pos = Position(
                    asset=aname, strategy=sname, side="long",
                    shares=shares, avg_price=entry_exec,
                    margin_used=margin_use, stop_price=stop_price,
                    best_price=row['High'], entry_idx=i,
                    leverage=lev, trail_mult=scfg["trail_mult"]
                )
                positions.append(new_pos)

        history.append(current_equity)

    # 강제 청산
    if positions:
        for pos in positions:
            df = asset_data[pos.asset]
            valid = df.loc[:end_date]
            if not valid.empty:
                fp = valid['Close'].iloc[-1]
                pnl = pos.shares * (fp - pos.avg_price)
                fee = pos.shares * fp * FEE_RATE
                cash += pos.margin_used + pnl - fee
        current_equity = cash
        history.append(current_equity)

    return history if history else [initial_budget], total_fees, stats, monthly_pnl


def print_period_result(label, history, stats, monthly_pnl, initial, months):
    final = history[-1]
    total_ret = (final - initial) / initial * 100
    monthly_ret = ((1 + total_ret / 100) ** (1 / months) - 1) * 100 if total_ret > -100 and months > 0 else 0
    gl = stats['gross_loss']
    pf = stats['gross_profit'] / gl if gl > 0 else float('inf')
    wr = stats['win'] / stats['total'] * 100 if stats['total'] > 0 else 0
    pos_m = sum(1 for v in monthly_pnl.values() if v > 0)
    total_m = len(monthly_pnl)

    aw = stats['gross_profit'] / stats['win'] if stats['win'] > 0 else 0
    al = stats['gross_loss'] / stats['loss'] if stats['loss'] > 0 else 0

    print(f"\n{'='*70}")
    print(f"  [{label}] ({months}개월)")
    print(f"  최종자산: ${final:,.0f} | 수익률: {total_ret:.1f}% | 월수익: {monthly_ret:.2f}%")
    print(f"  MDD: {stats['max_drawdown']:.1f}% | PF: {pf:.2f} | 승률: {wr:.1f}%")
    print(f"  거래: {stats['total']}건 (월 {stats['total']/months:.1f}건) | 양수달: {pos_m}/{total_m}")
    if stats['win'] > 0:
        print(f"  평균수익: +{aw:.1f}% | 평균손실: -{al:.1f}%")
    print(f"  {'─'*66}")
    for key, sd in sorted(stats['by_strat'].items()):
        t, w = sd['total'], sd['win']
        spf = sd['profit'] / sd['loss_amt'] if sd['loss_amt'] > 0 else float('inf')
        print(f"  {key:<10}: {t:>3}건 | 승률 {w/t*100 if t>0 else 0:.0f}% | PF {spf:.2f}")
    print(f"{'='*70}")


def main():
    print("[Walk-Forward 검증] 데이터 로딩...")

    # 전체 기간 데이터 로드 (한 번만)
    asset_data = {}
    for aname, acfg in ASSETS.items():
        d4h = get_binance_data(
            symbol=acfg["symbol"], interval="4h",
            start_date="2023-01-01 00:00:00",
            end_date="2026-04-09 23:59:59"
        )
        if d4h.empty:
            print(f"  {aname}: 데이터 없음")
            continue
        df = calc_signals_for_asset(d4h, aname)
        asset_data[aname] = df
        print(f"  {aname}: {len(df)}봉")

    # 기간 정의
    is_start = pd.Timestamp("2023-01-01")
    is_end   = pd.Timestamp("2024-06-30 23:59:59")
    oos_start = pd.Timestamp("2024-07-01")
    oos_end   = pd.Timestamp("2026-04-09 23:59:59")
    full_start = pd.Timestamp("2023-01-01")
    full_end   = pd.Timestamp("2026-04-09 23:59:59")

    # 1. In-Sample (IS): 2023-01 ~ 2024-06
    print("\n" + "="*70)
    print(" [1/3] In-Sample 구간 테스트 (파라미터 최적화 구간)")
    h1, f1, s1, m1 = run_backtest_period(asset_data, is_start, is_end, 100_000)
    print_period_result("IN-SAMPLE (2023.01~2024.06)", h1, s1, m1, 100_000, 18)

    # 2. Out-of-Sample (OOS): 2024-07 ~ 2026-04
    print("\n [2/3] Out-of-Sample 구간 테스트 (미래 검증)")
    h2, f2, s2, m2 = run_backtest_period(asset_data, oos_start, oos_end, 100_000)
    print_period_result("OUT-OF-SAMPLE (2024.07~2026.04)", h2, s2, m2, 100_000, 21)

    # 3. 전체 기간
    print("\n [3/3] 전체 기간 테스트 (기존 결과 재현)")
    h3, f3, s3, m3 = run_backtest_period(asset_data, full_start, full_end, 100_000)
    print_period_result("FULL PERIOD (2023.01~2026.04)", h3, s3, m3, 100_000, 39)

    # 비교 요약
    def get_metrics(h, s, mp, months):
        final = h[-1]
        tr = (final - 100000) / 100000 * 100
        mr = ((1+tr/100)**(1/months)-1)*100 if tr > -100 and months > 0 else 0
        gl = s['gross_loss']
        pf = s['gross_profit']/gl if gl > 0 else 0
        wr = s['win']/s['total']*100 if s['total']>0 else 0
        return tr, mr, s['max_drawdown'], pf, wr, s['total']

    is_m = get_metrics(h1, s1, m1, 18)
    oos_m = get_metrics(h2, s2, m2, 21)
    full_m = get_metrics(h3, s3, m3, 39)

    print(f"\n{'='*70}")
    print(f"  [Walk-Forward 비교 요약]")
    print(f"  {'':>20} | {'IS (학습)':>12} | {'OOS (검증)':>12} | {'전체':>12}")
    print(f"  {'─'*62}")
    print(f"  {'월수익률':>18} | {is_m[1]:>11.2f}% | {oos_m[1]:>11.2f}% | {full_m[1]:>11.2f}%")
    print(f"  {'MDD':>18} | {is_m[2]:>11.1f}% | {oos_m[2]:>11.1f}% | {full_m[2]:>11.1f}%")
    print(f"  {'PF':>18} | {is_m[3]:>12.2f} | {oos_m[3]:>12.2f} | {full_m[3]:>12.2f}")
    print(f"  {'승률':>18} | {is_m[4]:>11.1f}% | {oos_m[4]:>11.1f}% | {full_m[4]:>11.1f}%")
    print(f"  {'거래수':>18} | {is_m[5]:>12} | {oos_m[5]:>12} | {full_m[5]:>12}")
    print(f"  {'─'*62}")

    # 과적합 판단
    pf_ratio = oos_m[3] / is_m[3] if is_m[3] > 0 else 0
    wr_diff = oos_m[4] - is_m[4]
    mr_diff = oos_m[1] - is_m[1]

    print(f"\n  [과적합 판단]")
    print(f"  PF 비율 (OOS/IS): {pf_ratio:.2f}  (0.7 이상이면 양호)")
    print(f"  승률 차이 (OOS-IS): {wr_diff:+.1f}%  (+-10% 이내면 양호)")
    print(f"  월수익 차이 (OOS-IS): {mr_diff:+.2f}%  (+-1% 이내면 양호)")

    if pf_ratio >= 0.7 and abs(wr_diff) <= 10 and abs(mr_diff) <= 1.5:
        print(f"\n  >>> 결론: 과적합 징후 낮음 - OOS 성과가 IS와 유사")
    elif pf_ratio >= 0.5:
        print(f"\n  >>> 결론: 경미한 과적합 - OOS 성과 하락 있지만 여전히 수익성")
    else:
        print(f"\n  >>> 결론: 심각한 과적합 - OOS에서 전략 무효화")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
