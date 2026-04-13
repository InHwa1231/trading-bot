"""
Parker Brooks 5분봉 스캘핑 백테스트 엔진
VWAP + EMA + Volume Profile | 롱+숏 양방향

5분봉 스캘핑 특화 파라미터:
- 트레일링 스탑: ATR * 2.5 (타이트)
- 최대 보유: 36봉 (3시간)
- 쿨다운: 12봉 (1시간)
- 레버리지: 3-5x
- 수수료: 0.04% (테이커 기준)
"""
import pandas as pd
import numpy as np
from data.collector import get_binance_data
from indicators.calculators_parker_5m import calc_signals_for_asset

# ── 파라미터 (5분봉 스캘핑) ──
ATR_TRAIL_MULT = 2.5            # 타이트한 스탑
COOLDOWN = 12                   # 1시간 쿨다운
MAX_HOLD = 36                   # 최대 3시간 보유
PARTIAL_TRIGGER = 3.0           # 마진 3% 수익 시 분할
PARTIAL_RATIO = 0.5             # 50% 분할 (빠른 확보)
MAX_DAILY_LOSS_PCT = -3.0       # 일일 최대 손실 3%
MAX_POSITIONS = 2               # 스캘핑은 2포지션
MAX_TOTAL_EXPOSURE = 0.80

# ── 자산별 설정 ──
ASSETS = {
    "BTC":  {"symbol": "BTC/USDT",  "pos_base": 0.35, "enabled": True},
    "ETH":  {"symbol": "ETH/USDT",  "pos_base": 0.30, "enabled": True},
    "SOL":  {"symbol": "SOL/USDT",  "pos_base": 0.25, "enabled": True},
}


def calc_dynamic_leverage(adx_val, side="long"):
    if side == "long":
        if pd.isna(adx_val) or adx_val < 25:
            return 3
        elif adx_val < 35:
            return 4
        return 5
    else:
        if pd.isna(adx_val) or adx_val < 25:
            return 2
        elif adx_val < 35:
            return 3
        return 4


def calc_dynamic_pos(base_pos, adx_val, consec_stop, side="long"):
    if not pd.isna(adx_val) and adx_val > 30:
        adx_mult = 1.1
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
    pos = base_pos * adx_mult * stop_mult
    if side == "short":
        pos *= 0.7
    return pos


class Position:
    __slots__ = [
        'asset', 'side', 'shares', 'avg_price',
        'margin_used', 'stop_price', 'best_price', 'entry_idx',
        'leverage', 'trail_mult', 'partial_done'
    ]

    def __init__(self, asset, side, shares, avg_price,
                 margin_used, stop_price, best_price, entry_idx,
                 leverage, trail_mult):
        self.asset = asset
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


def run_portfolio_backtest(initial_budget=100_000):
    cash = initial_budget
    fee_rate = 0.0004  # 테이커 수수료 0.04%

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
        "by_strat": {},
        "long_count": 0, "short_count": 0,
    }

    # ── 데이터 로드 (5분봉 3년) ──
    print("[Parker-5m] 3자산 5분봉 데이터 로딩 (3년치, 시간 소요)...")
    asset_data = {}
    for aname, acfg in ASSETS.items():
        if not acfg["enabled"]:
            continue
        d5m = get_binance_data(
            symbol=acfg["symbol"], interval="5m",
            start_date="2023-01-01 00:00:00",
            end_date="2026-04-09 23:59:59"
        )
        if d5m.empty:
            print(f"  {aname}: 데이터 없음")
            continue
        print(f"  {aname}: {len(d5m)}봉 로드완료, 시그널 계산중...")
        df = calc_signals_for_asset(d5m, aname)
        asset_data[aname] = df
        print(f"  {aname}: 시그널 계산 완료")

    if not asset_data:
        return [initial_budget], 0.0, stats, {}, {}

    # 통합 타임라인
    all_indices = set()
    for df in asset_data.values():
        all_indices.update(df.index.tolist())
    timeline = sorted(all_indices)

    total_bars = len(timeline)
    print(f"\n[Parker-5m] 5분봉 | VWAP+EMA+VP | {len(asset_data)}자산 | 롱+숏 | {total_bars:,}봉")
    print(f"  수수료: 0.04% (테이커) | 레버리지 2-5x | 최대{MAX_POSITIONS}포지션")

    # 로그 샘플링 (5분봉은 너무 많아서 진입/청산만 출력)
    log_count = 0

    for ts in timeline:
        cur_date = ts.date()
        cur_ym = (ts.year, ts.month)

        daily_loss_today = daily_loss_tracker.get(cur_date, 0.0)
        ref_equity = max(peak_equity, initial_budget)
        entry_blocked = (daily_loss_today / ref_equity * 100) < MAX_DAILY_LOSS_PCT

        # ── 포지션 관리 ──
        closed_positions = []
        for pos in positions[:]:
            if ts not in asset_data[pos.asset].index:
                continue
            row = asset_data[pos.asset].loc[ts]
            i = int(row['Index_Num'])
            cur_close, cur_high, cur_low = row['Close'], row['High'], row['Low']
            atr_val = row['atr']
            candles_held = i - pos.entry_idx

            if pos.side == "long":
                is_exit_signal = bool(row['long_exit'])
            else:
                is_exit_signal = bool(row['short_exit'])

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
            if not pos.partial_done and margin_ret >= PARTIAL_TRIGGER:
                p_qty = pos.shares * PARTIAL_RATIO
                fee = p_qty * cur_close * fee_rate
                if pos.side == "long":
                    pnl = p_qty * (cur_close - pos.avg_price)
                else:
                    pnl = p_qty * (pos.avg_price - cur_close)
                total_fees += fee
                cash += pos.margin_used * PARTIAL_RATIO + (pnl - fee)
                pos.shares -= p_qty
                pos.margin_used *= (1 - PARTIAL_RATIO)
                pos.partial_done = True
                stats["partial_exit_count"] += 1
                if pos.side == "long":
                    be = pos.avg_price * 1.001
                    if be > pos.stop_price:
                        pos.stop_price = be
                else:
                    be = pos.avg_price * 0.999
                    if be < pos.stop_price:
                        pos.stop_price = be

            if hit_stop or (is_exit_signal and candles_held >= 3) or candles_held >= MAX_HOLD:
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

                if hit_stop:
                    etype = "trailing" if final_ret > 0 else "stop"
                elif candles_held >= MAX_HOLD:
                    etype = "cond_exit"
                else:
                    etype = "cond_exit"

                stats["total"] += 1
                stats[f"{etype}_count"] += 1
                key = f"{pos.asset}_{pos.side}"
                if key not in stats["by_strat"]:
                    stats["by_strat"][key] = {"total": 0, "win": 0, "loss": 0, "profit": 0.0, "loss_amt": 0.0}
                sd = stats["by_strat"][key]
                sd["total"] += 1

                if final_ret > 0.3:
                    stats["win"] += 1
                    stats["gross_profit"] += final_ret
                    stats["max_win"] = max(stats["max_win"], final_ret)
                    sd["win"] += 1
                    sd["profit"] += final_ret
                elif final_ret < -0.3:
                    stats["loss"] += 1
                    stats["gross_loss"] += abs(final_ret)
                    stats["max_loss"] = max(stats["max_loss"], abs(final_ret))
                    sd["loss"] += 1
                    sd["loss_amt"] += abs(final_ret)
                else:
                    stats["breakeven"] += 1

                monthly_pnl[cur_ym] = monthly_pnl.get(cur_ym, 0.0) + net
                daily_pnl[cur_date] = daily_pnl.get(cur_date, 0.0) + net

                if net < 0:
                    consec_stop[pos.asset] = consec_stop.get(pos.asset, 0) + 1
                    daily_loss_tracker[cur_date] = daily_loss_tracker.get(cur_date, 0.0) + net
                else:
                    consec_stop[pos.asset] = 0

                last_exit[(pos.asset, pos.side)] = i
                closed_positions.append(pos)

        for p in closed_positions:
            positions.remove(p)

        # 자산 평가 (매 100봉마다만 기록 - 메모리 절약)
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

        # 히스토리는 매 288봉(1일)마다 기록 (메모리 절약)
        bar_idx = timeline.index(ts) if len(history) == 0 else -1
        if len(history) == 0 or (len(timeline) > 0 and timeline.index(ts) % 288 == 0):
            history.append(current_equity)

        # ── 신규 진입 ──
        if entry_blocked or len(positions) >= MAX_POSITIONS:
            continue

        # BTC 추세 필터
        btc_bullish = True
        btc_bearish = False
        if "BTC" in asset_data and ts in asset_data["BTC"].index:
            btc_row = asset_data["BTC"].loc[ts]
            btc_bullish = bool(btc_row['trend_bull'])
            btc_bearish = bool(btc_row['trend_bear'])

        total_margin_used = sum(p.margin_used for p in positions)
        margin_available = current_equity * MAX_TOTAL_EXPOSURE - total_margin_used
        active_keys = {(p.asset, p.side) for p in positions}

        for aname, acfg in ASSETS.items():
            if not acfg["enabled"] or aname not in asset_data:
                continue
            if ts not in asset_data[aname].index:
                continue
            if len(positions) >= MAX_POSITIONS:
                break

            row = asset_data[aname].loc[ts]
            i = int(row['Index_Num'])
            if i < 100:
                continue

            cur_open = row['Open']
            cur_high, cur_low = row['High'], row['Low']
            atr_val, adx_val = row['atr'], row['adx']

            # 롱
            if btc_bullish and (aname, "long") not in active_keys:
                lei = last_exit.get((aname, "long"), -9999)
                if (i - lei) >= COOLDOWN and bool(row['long_buy']):
                    lev = calc_dynamic_leverage(adx_val, "long")
                    cs = consec_stop.get(aname, 0)
                    pos_pct = calc_dynamic_pos(acfg["pos_base"], adx_val, cs, "long")
                    margin_use = current_equity * pos_pct
                    if margin_use > margin_available:
                        margin_use = margin_available * 0.9
                    if margin_use >= current_equity * 0.05 and cash >= margin_use * (1 + lev * fee_rate):
                        notional = margin_use * lev
                        entry_exec = cur_open
                        fee = notional * fee_rate
                        total_fees += fee
                        shares = notional / entry_exec
                        stop_price = entry_exec - atr_val * ATR_TRAIL_MULT
                        cash -= (margin_use + fee)
                        margin_available -= margin_use
                        positions.append(Position(
                            aname, "long", shares, entry_exec, margin_use,
                            stop_price, cur_high, i, lev, ATR_TRAIL_MULT
                        ))
                        active_keys.add((aname, "long"))
                        stats["long_count"] += 1

            # 숏
            if btc_bearish and (aname, "short") not in active_keys:
                lei = last_exit.get((aname, "short"), -9999)
                if (i - lei) >= COOLDOWN and bool(row['short_sell']):
                    lev = calc_dynamic_leverage(adx_val, "short")
                    cs = consec_stop.get(aname, 0)
                    pos_pct = calc_dynamic_pos(acfg["pos_base"], adx_val, cs, "short")
                    margin_use = current_equity * pos_pct
                    if margin_use > margin_available:
                        margin_use = margin_available * 0.9
                    if margin_use >= current_equity * 0.05 and cash >= margin_use * (1 + lev * fee_rate):
                        notional = margin_use * lev
                        entry_exec = cur_open
                        fee = notional * fee_rate
                        total_fees += fee
                        shares = notional / entry_exec
                        stop_price = entry_exec + atr_val * ATR_TRAIL_MULT
                        cash -= (margin_use + fee)
                        margin_available -= margin_use
                        positions.append(Position(
                            aname, "short", shares, entry_exec, margin_use,
                            stop_price, cur_low, i, lev, ATR_TRAIL_MULT
                        ))
                        active_keys.add((aname, "short"))
                        stats["short_count"] += 1

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

    # 최종 equity 기록
    if history and history[-1] != current_equity:
        history.append(current_equity)

    return history if history else [initial_budget], total_fees, stats, monthly_pnl, daily_pnl
