"""
V57-v3 백테스트 엔진 - 검증 자산 선별 + 고레버리지 집중

목표: 일 0.5-1% (월 15-30%)
접근법:
- 4h 타임프레임 (검증됨)
- 수익성 좋은 자산-전략 쌍만 선별 (PF>1.0 기준)
- 레버리지: 6-10x (ADX 기반, 강추세 집중)
- 최대 동시 4포지션
- 쿨다운 8h로 빈번한 재진입
- 스탑 더 타이트 (빠른 손절 + 빈번한 재진입)
"""
import pandas as pd
from data.collector import get_binance_data
from indicators.calculators_v57 import calc_signals_v57_4h

# ── 글로벌 파라미터 ──
ATR_TRAIL_MULT_EMA = 3.0        # 타이트 스탑 (빠른 손절→빠른 재진입)
ATR_TRAIL_MULT_DON = 2.5
COOLDOWN = 2                     # 8시간 쿨다운 (2봉)
MAX_HOLD = 80                    # 약 13일 최대 보유
PARTIAL_TRIGGER = 5.0            # 마진 5% 수익 시 분할
PARTIAL_RATIO = 0.5              # 50% 분할 (V56은 40%)
MAX_DAILY_LOSS_PCT = -5.0
MAX_POSITIONS = 4
MAX_TOTAL_EXPOSURE = 0.95

# ── 자산별 설정 (수익성 검증된 6개) ──
ASSETS = {
    "BTC":  {"symbol": "BTC/USDT",  "pos_base": 0.24, "enabled": True},
    "ETH":  {"symbol": "ETH/USDT",  "pos_base": 0.22, "enabled": True},
    "SOL":  {"symbol": "SOL/USDT",  "pos_base": 0.20, "enabled": True},
    "LINK": {"symbol": "LINK/USDT", "pos_base": 0.18, "enabled": True},
    "AVAX": {"symbol": "AVAX/USDT", "pos_base": 0.16, "enabled": True},
    "DOGE": {"symbol": "DOGE/USDT", "pos_base": 0.16, "enabled": True},
}

# ── 전략-자산 허용 조합 (수익성 검증된 쌍만) ──
ALLOWED_PAIRS = {
    ("BTC", "EMA"),    # PF 2.06
    ("ETH", "EMA"),    # PF 2.67
    ("ETH", "DON"),    # PF 1.18
    ("SOL", "EMA"),    # PF 1.10
    ("LINK", "DON"),   # PF 4.33
    ("LINK", "EMA"),   # PF 1.06
    ("AVAX", "DON"),   # PF 1.45
    ("DOGE", "EMA"),   # PF 1.06
}

STRATEGIES = {
    "EMA": {"trail_mult": ATR_TRAIL_MULT_EMA},
    "DON": {"trail_mult": ATR_TRAIL_MULT_DON},
}


def calc_dynamic_leverage(adx_val):
    """ADX 기반 동적 레버리지 (6-10x)"""
    if pd.isna(adx_val) or adx_val < 22:
        return 6
    elif adx_val < 30:
        return 7
    elif adx_val < 40:
        return 9
    else:
        return 10


def calc_dynamic_pos(base_pos, adx_val, consec_stop):
    """동적 포지션 사이징 - 강추세에 공격적"""
    if not pd.isna(adx_val) and adx_val > 35:
        adx_mult = 1.3
    elif not pd.isna(adx_val) and adx_val > 25:
        adx_mult = 1.1
    else:
        adx_mult = 0.80
    if consec_stop >= 3:
        stop_mult = 0.4
    elif consec_stop >= 2:
        stop_mult = 0.6
    else:
        stop_mult = 1.0
    return base_pos * adx_mult * stop_mult


def _update_stats(stats, ret, exit_type, asset, strategy):
    stats["total"] += 1
    key = f"{asset}_{strategy}"
    if key not in stats["by_strat"]:
        stats["by_strat"][key] = {"total": 0, "win": 0, "loss": 0, "profit": 0.0, "loss_amt": 0.0}
    sd = stats["by_strat"][key]
    sd["total"] += 1

    if ret > 0.3:
        stats["win"] += 1
        stats["gross_profit"] += ret
        stats["max_win"] = max(stats["max_win"], ret)
        sd["win"] += 1
        sd["profit"] += ret
    elif ret < -0.3:
        stats["loss"] += 1
        stats["gross_loss"] += abs(ret)
        stats["max_loss"] = max(stats["max_loss"], abs(ret))
        sd["loss"] += 1
        sd["loss_amt"] += abs(ret)
    else:
        stats["breakeven"] += 1
    stats[f"{exit_type}_count"] += 1


class Position:
    __slots__ = [
        'asset', 'strategy', 'shares', 'avg_price',
        'margin_used', 'stop_price', 'best_price', 'entry_idx',
        'leverage', 'trail_mult', 'partial_done'
    ]

    def __init__(self, asset, strategy, shares, avg_price,
                 margin_used, stop_price, best_price, entry_idx,
                 leverage, trail_mult):
        self.asset = asset
        self.strategy = strategy
        self.shares = shares
        self.avg_price = avg_price
        self.margin_used = margin_used
        self.stop_price = stop_price
        self.best_price = best_price
        self.entry_idx = entry_idx
        self.leverage = leverage
        self.trail_mult = trail_mult
        self.partial_done = False


def run_portfolio_backtest(initial_budget=100_000):
    cash = initial_budget
    fee_rate = 0.0002  # 메이커 0.02%

    positions = []
    total_fees = 0.0
    history = []
    monthly_pnl = {}
    daily_pnl = {}
    peak_equity = initial_budget
    consec_stop = {}
    daily_loss_tracker = {}
    last_exit = {}

    stats = {
        "total": 0, "win": 0, "loss": 0, "breakeven": 0,
        "stop_count": 0, "trailing_count": 0,
        "cond_exit_count": 0, "partial_exit_count": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "max_win": 0.0, "max_loss": 0.0, "max_drawdown": 0.0,
        "daily_block_count": 0,
        "by_strat": {},
    }

    # ── 데이터 로드 (4h) ──
    print("[V57-v2] 8자산 4h 데이터 로딩...")
    asset_data = {}
    for aname, acfg in ASSETS.items():
        if not acfg["enabled"]:
            continue
        d4h = get_binance_data(
            symbol=acfg["symbol"], interval="4h",
            start_date="2023-01-01 00:00:00",
            end_date="2026-04-09 23:59:59"
        )
        if d4h.empty:
            print(f"  {aname}: 데이터 없음")
            continue
        df = calc_signals_v57_4h(d4h, aname)
        asset_data[aname] = df
        print(f"  {aname}: {len(df)}봉 로드")

    if not asset_data:
        return [initial_budget], 0.0, stats, {}, {}

    # ── 통합 타임라인 ──
    all_indices = set()
    for df in asset_data.values():
        all_indices.update(df.index.tolist())
    timeline = sorted(all_indices)

    print(f"\n[V57-v2] 4h | EMA+돈치안 | {len(asset_data)}자산 | 레버리지5-8x | 5포지션")
    print(f"{'time':<14} | {'A':<4} | {'S':<3} | {'action':<7} | {'price':>9} | {'ret':>7} | {'cash':>11} | note")
    print("-" * 110)

    for ts in timeline:
        cur_date = ts.date()
        cur_ym = (ts.year, ts.month)
        time_str = ts.strftime('%y-%m-%d %H:%M')

        daily_loss_today = daily_loss_tracker.get(cur_date, 0.0)
        ref_equity = max(peak_equity, initial_budget)
        entry_blocked = (daily_loss_today / ref_equity * 100) < MAX_DAILY_LOSS_PCT

        # ══════════════════════════════════
        # 포지션 관리
        # ══════════════════════════════════
        closed_positions = []
        for pos in positions[:]:
            if ts not in asset_data[pos.asset].index:
                continue
            row = asset_data[pos.asset].loc[ts]
            i = int(row['Index_Num'])
            cur_close = row['Close']
            cur_high = row['High']
            cur_low = row['Low']
            atr_val = row['atr']
            candles_held = i - pos.entry_idx

            if pos.strategy == "EMA":
                is_exit_signal = bool(row['ema_sell'])
            else:
                is_exit_signal = bool(row['don_sell'])

            # 트레일링 스탑
            if cur_high > pos.best_price:
                pos.best_price = cur_high
            new_stop = pos.best_price - atr_val * pos.trail_mult
            if new_stop > pos.stop_price:
                pos.stop_price = new_stop

            hit_stop = cur_low <= pos.stop_price
            margin_ret = (cur_close - pos.avg_price) / pos.avg_price * 100 * pos.leverage

            # 분할 익절
            if not pos.partial_done and margin_ret >= PARTIAL_TRIGGER:
                p_qty = pos.shares * PARTIAL_RATIO
                fee = p_qty * cur_close * fee_rate
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
                fee = pos.shares * fp * fee_rate
                net = pos.shares * (fp - pos.avg_price) - fee
                final_ret = (pos.shares * (fp - pos.avg_price)) / pos.margin_used * 100 if pos.margin_used > 0 else 0
                total_fees += fee
                cash += pos.margin_used + net

                if hit_stop:
                    etype = "trailing" if final_ret > 0 else "stop"
                    remark = "TRAIL" if final_ret > 0 else "STOP"
                elif candles_held >= MAX_HOLD:
                    etype = "cond_exit"; remark = "TIME"
                else:
                    etype = "cond_exit"; remark = "SIG"

                _update_stats(stats, final_ret, etype, pos.asset, pos.strategy)
                monthly_pnl[cur_ym] = monthly_pnl.get(cur_ym, 0.0) + net
                daily_pnl[cur_date] = daily_pnl.get(cur_date, 0.0) + net

                if net < 0:
                    consec_stop[pos.asset] = consec_stop.get(pos.asset, 0) + 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consec_stop[pos.asset] = 0

                last_exit[(pos.asset, pos.strategy)] = i
                print(f"{time_str} | {pos.asset:<4} | {pos.strategy:<3} | EXIT   | {fp:>9,.1f} | {final_ret:>6.1f}% | {cash:>11,.0f} | {remark}({candles_held*4}h)")
                closed_positions.append(pos)

        for p in closed_positions:
            positions.remove(p)

        # 현재 자산
        current_equity = cash
        for pos in positions:
            if ts in asset_data[pos.asset].index:
                row = asset_data[pos.asset].loc[ts]
                current_equity += pos.margin_used + pos.shares * (row['Close'] - pos.avg_price)

        if current_equity > peak_equity:
            peak_equity = current_equity
        dd = (current_equity - peak_equity) / peak_equity * 100
        stats["max_drawdown"] = min(stats["max_drawdown"], dd)

        # ══════════════════════════════════
        # 신규 진입
        # ══════════════════════════════════
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
        active_keys = {(p.asset, p.strategy) for p in positions}

        for aname, acfg in ASSETS.items():
            if not acfg["enabled"] or aname not in asset_data:
                continue
            if ts not in asset_data[aname].index:
                continue

            row = asset_data[aname].loc[ts]
            i = int(row['Index_Num'])
            if i < 60:
                continue

            cur_open = row['Open']
            cur_high = row['High']
            atr_val = row['atr']
            adx_val = row['adx']

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

                if sname == "EMA":
                    is_buy = bool(row['ema_buy'])
                else:
                    is_buy = bool(row['don_buy'])

                if not is_buy:
                    continue

                lev = calc_dynamic_leverage(adx_val)
                cs = consec_stop.get(aname, 0)
                pos_pct = calc_dynamic_pos(acfg["pos_base"], adx_val, cs)
                margin_use = current_equity * pos_pct

                if margin_use > margin_available:
                    margin_use = margin_available * 0.9
                if margin_use < current_equity * 0.03:
                    continue
                if cash < margin_use * (1 + lev * fee_rate):
                    continue

                notional = margin_use * lev
                entry_exec = cur_open
                fee = notional * fee_rate
                total_fees += fee
                shares = notional / entry_exec
                stop_price = entry_exec - atr_val * scfg["trail_mult"]

                cash -= (margin_use + fee)
                margin_available -= margin_use

                new_pos = Position(
                    asset=aname, strategy=sname,
                    shares=shares, avg_price=entry_exec,
                    margin_used=margin_use, stop_price=stop_price,
                    best_price=cur_high, entry_idx=i,
                    leverage=lev, trail_mult=scfg["trail_mult"]
                )
                positions.append(new_pos)
                active_keys.add((aname, sname))

                stop_pct = (entry_exec - stop_price) / entry_exec * 100
                cs_str = f" [cs:{cs}]" if cs > 0 else ""
                print(f"{time_str} | {aname:<4} | {sname:<3} | LONG   | {entry_exec:>9,.1f} | {'':>7} | {cash:>11,.0f} | lev:{lev}x,stp-{stop_pct:.1f}%{cs_str}")

        history.append(current_equity)

    # 강제 청산
    if positions:
        for pos in positions:
            df = asset_data[pos.asset]
            fp = df['Close'].iloc[-1]
            pnl = pos.shares * (fp - pos.avg_price)
            fee = pos.shares * fp * fee_rate
            cash += pos.margin_used + pnl - fee
        current_equity = cash
        history.append(current_equity)

    return history if history else [initial_budget], total_fees, stats, monthly_pnl, daily_pnl
