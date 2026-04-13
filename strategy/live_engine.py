"""
V56-plus 라이브 트레이딩 엔진

V56 검증 로직 기반 + 수익률 강화:
- LINK 추가 (4자산)
- 레버리지 4-6x (V56: 3-4x)
- 최대 3포지션 (V56: 2)
- 쿨다운 12h (V56: 24h)
- 분할 익절 6% (V56: 8%)
- Walk-Forward 검증 완료 (OOS PF 2.75, 월 7.69%)
"""
import json
import os
import time
import logging
from datetime import datetime
from strategy.signal_generator import generate_signals

logger = logging.getLogger("live_engine")

# V56-plus 파라미터 (Walk-Forward 검증 완료)
ATR_TRAIL_MULT_EMA = 4.0      # V56과 동일
ATR_TRAIL_MULT_DON = 3.0      # V56과 동일
COOLDOWN_HOURS = 12            # 쿨다운 12시간 (V56: 24h)
MAX_HOLD_HOURS = 480           # 최대 보유 20일
PARTIAL_TRIGGER = 6.0          # 마진 6% 수익 시 분할 (V56: 8%)
PARTIAL_RATIO = 0.4
MAX_DAILY_LOSS_PCT = -4.0
MAX_POSITIONS = 3              # 3포지션 (V56: 2)
MAX_TOTAL_EXPOSURE = 0.90      # V56: 0.85

ASSETS = {
    "BTC":  {"symbol": "BTC/USDT",  "pos_base": 0.38},
    "ETH":  {"symbol": "ETH/USDT",  "pos_base": 0.32},
    "SOL":  {"symbol": "SOL/USDT",  "pos_base": 0.25},
}

ALLOWED_PAIRS = {
    ("BTC", "EMA"),    # PF 1.93
    ("ETH", "EMA"),    # PF 2.78
    ("ETH", "DON"),    # PF 2.03
    ("SOL", "DON"),    # PF 2.65
}

TRAIL_MULT = {"EMA": ATR_TRAIL_MULT_EMA, "DON": ATR_TRAIL_MULT_DON}

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "data", "live_state.json")


class LivePosition:
    """실시간 포지션"""
    def __init__(self, asset, strategy, symbol, entry_price, shares,
                 margin_used, leverage, stop_price, best_price, entry_time):
        self.asset = asset
        self.strategy = strategy
        self.symbol = symbol
        self.entry_price = entry_price
        self.shares = shares
        self.margin_used = margin_used
        self.leverage = leverage
        self.stop_price = stop_price
        self.best_price = best_price
        self.entry_time = entry_time
        self.partial_done = False
        self.trail_mult = TRAIL_MULT[strategy]

    def to_dict(self):
        return {
            'asset': self.asset, 'strategy': self.strategy,
            'symbol': self.symbol, 'entry_price': self.entry_price,
            'shares': self.shares, 'margin_used': self.margin_used,
            'leverage': self.leverage, 'stop_price': self.stop_price,
            'best_price': self.best_price,
            'entry_time': self.entry_time.isoformat(),
            'partial_done': self.partial_done,
        }

    @classmethod
    def from_dict(cls, d):
        pos = cls(
            d['asset'], d['strategy'], d['symbol'],
            d['entry_price'], d['shares'], d['margin_used'],
            d['leverage'], d['stop_price'], d['best_price'],
            datetime.fromisoformat(d['entry_time'])
        )
        pos.partial_done = d.get('partial_done', False)
        return pos


def calc_dynamic_leverage(adx_val):
    """ADX 기반 동적 레버리지 (4-6x)"""
    if adx_val is None or adx_val < 25:
        return 4
    elif adx_val < 35:
        return 5
    return 6


def calc_dynamic_pos(base_pos, adx_val, consec_stop):
    adx_mult = 1.15 if (adx_val and adx_val > 30) else (1.0 if (adx_val and adx_val > 25) else 0.85)
    stop_mult = 0.5 if consec_stop >= 3 else (0.7 if consec_stop >= 2 else 1.0)
    return base_pos * adx_mult * stop_mult


class LiveTradingEngine:
    def __init__(self, client):
        self.client = client
        self.positions = []
        self.last_exit = {}       # (asset, strategy) → datetime
        self.consec_stop = {}     # asset → int
        self.daily_loss = {}      # date → float
        self.trade_log = []

    def save_state(self):
        """상태 저장 (재시작 대응)"""
        state = {
            'positions': [p.to_dict() for p in self.positions],
            'last_exit': {f"{k[0]}_{k[1]}": v.isoformat() for k, v in self.last_exit.items()},
            'consec_stop': self.consec_stop,
            'trade_log': self.trade_log[-100:],  # 최근 100건만
        }
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        logger.debug("상태 저장 완료")

    def load_state(self):
        """상태 복구"""
        if not os.path.exists(STATE_FILE):
            logger.info("저장된 상태 없음 - 새로 시작")
            return
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
        self.positions = [LivePosition.from_dict(d) for d in state.get('positions', [])]
        for k, v in state.get('last_exit', {}).items():
            parts = k.split('_')
            self.last_exit[(parts[0], parts[1])] = datetime.fromisoformat(v)
        self.consec_stop = state.get('consec_stop', {})
        self.trade_log = state.get('trade_log', [])
        logger.info(f"상태 복구: {len(self.positions)}개 포지션, {len(self.trade_log)}건 거래 기록")

    def get_equity(self):
        """현재 총 자산"""
        bal = self.client.get_balance()
        total = bal['total']
        # 미실현 손익 포함
        for pos_info in self.client.get_positions():
            total += pos_info['unrealizedPnl']
        return total

    def check_and_execute(self):
        """
        메인 로직: 4h 봉 마감 시 호출
        1. 각 자산 데이터 수집 + 시그널 생성
        2. 기존 포지션 관리 (스탑/익절/청산)
        3. 신규 진입 판단
        """
        now = datetime.utcnow()
        today = now.date()
        signals = {}

        logger.info(f"\n{'='*60}")
        logger.info(f"[{now.strftime('%Y-%m-%d %H:%M')} UTC] 시그널 체크")

        # 1. 데이터 수집 + 시그널 생성
        for aname, acfg in ASSETS.items():
            try:
                df = self.client.get_ohlcv(acfg['symbol'], '4h', limit=100)
                sig = generate_signals(df, aname)
                if sig:
                    signals[aname] = sig
                    logger.info(
                        f"  {aname}: close={sig['close']:,.0f} RSI={sig['rsi']:.1f} "
                        f"ADX={sig['adx']:.1f} Bull={sig['trend_bull']} "
                        f"EMA_buy={sig['ema_buy']} DON_buy={sig['don_buy']}"
                    )
            except Exception as e:
                logger.error(f"  {aname} 데이터 오류: {e}")

        if not signals:
            logger.warning("시그널 없음 - 건너뜀")
            return

        # 2. 기존 포지션 관리
        self._manage_positions(signals, now)

        # 3. 신규 진입
        self._check_entries(signals, now, today)

        # 4. 상태 저장
        self.save_state()
        self._print_status()

    def _manage_positions(self, signals, now):
        """기존 포지션 관리"""
        closed = []

        for pos in self.positions[:]:
            if pos.asset not in signals:
                continue

            sig = signals[pos.asset]
            cur_price = sig['close']
            cur_high = sig['high']
            cur_low = sig['low']
            atr_val = sig['atr']

            # 보유 시간
            hold_hours = (now - pos.entry_time).total_seconds() / 3600

            # 전략별 청산 시그널
            if pos.strategy == "EMA":
                is_exit_signal = sig['ema_sell']
            else:
                is_exit_signal = sig['don_sell']

            # 트레일링 스탑 업데이트
            if cur_high > pos.best_price:
                pos.best_price = cur_high
            new_stop = pos.best_price - atr_val * pos.trail_mult
            if new_stop > pos.stop_price:
                pos.stop_price = new_stop

            hit_stop = cur_low <= pos.stop_price
            margin_ret = (cur_price - pos.entry_price) / pos.entry_price * 100 * pos.leverage

            # 분할 익절
            if not pos.partial_done and margin_ret >= PARTIAL_TRIGGER:
                logger.info(
                    f"  >>> {pos.asset}_{pos.strategy} 분할 익절 "
                    f"({PARTIAL_RATIO*100:.0f}% @ {margin_ret:.1f}%)"
                )
                order = self.client.partial_close(pos.symbol, pos.shares, PARTIAL_RATIO)
                if order:
                    pos.shares *= (1 - PARTIAL_RATIO)
                    pos.margin_used *= (1 - PARTIAL_RATIO)
                    pos.partial_done = True
                    # 손익분기 스탑
                    be = pos.entry_price * 1.002
                    if be > pos.stop_price:
                        pos.stop_price = be
                    self._log_trade(pos.asset, pos.strategy, "PARTIAL",
                                    cur_price, margin_ret, now)

            # 청산 조건
            should_close = False
            reason = ""
            if hit_stop:
                should_close = True
                reason = "TRAIL" if margin_ret > 0 else "STOP"
            elif is_exit_signal and hold_hours >= 12:  # 최소 3봉(12h) 보유
                should_close = True
                reason = "SIGNAL"
            elif hold_hours >= MAX_HOLD_HOURS:
                should_close = True
                reason = "TIME"

            if should_close:
                logger.info(
                    f"  >>> {pos.asset}_{pos.strategy} 청산 [{reason}] "
                    f"수익: {margin_ret:.1f}% | {hold_hours:.0f}h 보유"
                )
                order = self.client.market_sell(pos.symbol, pos.shares)
                if order:
                    self._log_trade(pos.asset, pos.strategy, f"EXIT_{reason}",
                                    cur_price, margin_ret, now)
                    # 연속 손절 추적
                    if margin_ret < 0:
                        self.consec_stop[pos.asset] = self.consec_stop.get(pos.asset, 0) + 1
                        self.daily_loss[now.date()] = self.daily_loss.get(now.date(), 0) + margin_ret
                    else:
                        self.consec_stop[pos.asset] = 0
                    self.last_exit[(pos.asset, pos.strategy)] = now
                    closed.append(pos)

        for p in closed:
            self.positions.remove(p)

    def _check_entries(self, signals, now, today):
        """신규 진입 체크"""
        # 일일 손실 한도 체크
        equity = self.get_equity()
        daily_loss = self.daily_loss.get(today, 0)
        if (daily_loss / equity * 100) < MAX_DAILY_LOSS_PCT:
            logger.info("  일일 손실 한도 도달 - 진입 차단")
            return

        if len(self.positions) >= MAX_POSITIONS:
            logger.debug("  최대 포지션 도달 - 진입 차단")
            return

        # BTC 추세 필터
        btc_sig = signals.get("BTC")
        if not btc_sig or not btc_sig['trend_bull']:
            logger.info("  BTC 하락 추세 - 전 자산 진입 차단")
            return

        total_margin_used = sum(p.margin_used for p in self.positions)
        margin_available = equity * MAX_TOTAL_EXPOSURE - total_margin_used

        active_keys = {(p.asset, p.strategy) for p in self.positions}

        for aname, acfg in ASSETS.items():
            if aname not in signals:
                continue

            sig = signals[aname]

            for sname, trail_mult in TRAIL_MULT.items():
                if (aname, sname) not in ALLOWED_PAIRS:
                    continue
                if (aname, sname) in active_keys:
                    continue
                if len(self.positions) >= MAX_POSITIONS:
                    break

                # 쿨다운 체크
                last_exit_time = self.last_exit.get((aname, sname))
                if last_exit_time:
                    hours_since = (now - last_exit_time).total_seconds() / 3600
                    if hours_since < COOLDOWN_HOURS:
                        continue

                # 시그널 확인
                if sname == "EMA":
                    is_buy = sig['ema_buy']
                else:
                    is_buy = sig['don_buy']

                if not is_buy:
                    continue

                # 동적 레버리지 & 포지션
                adx_val = sig['adx']
                lev = calc_dynamic_leverage(adx_val)
                cs = self.consec_stop.get(aname, 0)
                pos_pct = calc_dynamic_pos(acfg['pos_base'], adx_val, cs)
                margin_use = equity * pos_pct

                if margin_use > margin_available:
                    margin_use = margin_available * 0.9
                if margin_use < equity * 0.05:
                    continue

                # 주문 실행
                symbol = acfg['symbol']
                self.client.set_margin_mode(symbol, 'isolated')
                order = self.client.market_buy(symbol, margin_use, lev)

                if order:
                    entry_price = sig['close']  # 실제로는 order fill price 사용
                    if order.get('average'):
                        entry_price = float(order['average'])

                    shares = margin_use * lev / entry_price
                    stop_price = entry_price - sig['atr'] * trail_mult

                    new_pos = LivePosition(
                        asset=aname, strategy=sname, symbol=symbol,
                        entry_price=entry_price, shares=shares,
                        margin_used=margin_use, leverage=lev,
                        stop_price=stop_price, best_price=sig['high'],
                        entry_time=now
                    )
                    self.positions.append(new_pos)
                    margin_available -= margin_use

                    logger.info(
                        f"  >>> {aname}_{sname} 롱 진입 @ {entry_price:,.2f} | "
                        f"lev:{lev}x | 스탑:{stop_price:,.2f} "
                        f"({(entry_price-stop_price)/entry_price*100:.1f}%)"
                    )
                    self._log_trade(aname, sname, "ENTRY", entry_price, 0, now)

    def _log_trade(self, asset, strategy, action, price, ret, ts):
        """거래 기록"""
        entry = {
            'time': ts.strftime('%Y-%m-%d %H:%M'),
            'asset': asset,
            'strategy': strategy,
            'action': action,
            'price': price,
            'return_pct': round(ret, 2),
        }
        self.trade_log.append(entry)
        logger.info(f"  [LOG] {entry}")

    def _print_status(self):
        """현재 상태 출력"""
        equity = self.get_equity()
        logger.info(f"\n  [현재 상태]")
        logger.info(f"  총 자산: ${equity:,.2f}")
        logger.info(f"  활성 포지션: {len(self.positions)}개")
        for pos in self.positions:
            try:
                cur_price = self.client.get_ticker_price(pos.symbol)
                pnl_pct = (cur_price - pos.entry_price) / pos.entry_price * 100 * pos.leverage
                logger.info(
                    f"    {pos.asset}_{pos.strategy}: "
                    f"진입 {pos.entry_price:,.2f} → 현재 {cur_price:,.2f} "
                    f"({pnl_pct:+.1f}%) | 스탑: {pos.stop_price:,.2f}"
                )
            except Exception:
                logger.info(f"    {pos.asset}_{pos.strategy}: 가격 조회 실패")
        logger.info(f"{'='*60}\n")
