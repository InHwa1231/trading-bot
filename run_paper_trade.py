"""
V56-plus 페이퍼 트레이딩 실행기

바이낸스 선물 테스트넷에서 V56-plus 전략을 실시간 실행
- V56 검증 로직 + LINK추가 + 레버리지4-6x + 3포지션
- 4h 봉 마감 시마다 시그널 체크 (00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC)
- 포지션 관리 (트레일링 스탑, 분할 익절 6%)
- 중간에 1시간마다 스탑 체크 (급락 대응)
- Walk-Forward 검증 완료 (OOS PF 2.75, 월 7.69%)

사용법:
  1. .env에 BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_SECRET 설정
  2. python run_paper_trade.py
  3. Ctrl+C로 중지 (상태 자동 저장, 재시작 시 복구)
"""
import sys
import os
import time
import logging
import schedule
from datetime import datetime, timezone

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from broker.binance_client import BinanceFuturesClient
from strategy.live_engine import LiveTradingEngine

# ── 로깅 설정 ──
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"paper_trade_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("main")


def run_signal_check(engine):
    """4h 봉 마감 시 전체 시그널 체크 + 진입/청산"""
    try:
        logger.info("\n" + "=" * 60)
        logger.info("[4H 봉 마감] 전체 시그널 체크 시작")
        engine.check_and_execute()
    except Exception as e:
        logger.error(f"시그널 체크 오류: {e}", exc_info=True)


def run_stop_check(engine):
    """중간 스탑 체크 (급락 대응) - 포지션 관리만"""
    if not engine.positions:
        return
    try:
        logger.info("[스탑 체크] 포지션 모니터링")
        from strategy.signal_generator import generate_signals

        for pos in engine.positions[:]:
            try:
                df = engine.client.get_ohlcv(pos.symbol, '4h', limit=100)
                sig = generate_signals(df, pos.asset)
                if not sig:
                    continue

                cur_price = sig['close']
                if cur_price <= pos.stop_price:
                    margin_ret = (cur_price - pos.entry_price) / pos.entry_price * 100 * pos.leverage
                    reason = "TRAIL" if margin_ret > 0 else "STOP"
                    logger.info(
                        f"  >>> {pos.asset}_{pos.strategy} 스탑 히트! "
                        f"[{reason}] {margin_ret:.1f}%"
                    )
                    order = engine.client.market_sell(pos.symbol, pos.shares)
                    if order:
                        now = datetime.now(timezone.utc).replace(tzinfo=None)
                        engine._log_trade(pos.asset, pos.strategy, f"EXIT_{reason}",
                                          cur_price, margin_ret, now)
                        if margin_ret < 0:
                            engine.consec_stop[pos.asset] = engine.consec_stop.get(pos.asset, 0) + 1
                        else:
                            engine.consec_stop[pos.asset] = 0
                        engine.last_exit[(pos.asset, pos.strategy)] = now
                        engine.positions.remove(pos)
                        engine.save_state()
            except Exception as e:
                logger.error(f"  스탑 체크 오류 {pos.asset}: {e}")
    except Exception as e:
        logger.error(f"스탑 체크 전체 오류: {e}", exc_info=True)


def main():
    logger.info("=" * 60)
    logger.info("  V56-plus 페이퍼 트레이딩 시작")
    logger.info("  전략: EMA 추세추종 + 돈치안 돌파")
    logger.info("  자산: BTC, ETH, SOL")
    logger.info("  동적 레버리지: 4-6x (ADX 기반)")
    logger.info("  최대 3포지션 | 쿨다운 12h | 분할익절 6%")
    logger.info("  BTC 추세 필터: 하락장 진입 차단")
    logger.info("=" * 60)

    # 클라이언트 초기화
    try:
        client = BinanceFuturesClient(testnet=True)
    except ValueError as e:
        logger.error(str(e))
        logger.info("\n[설정 방법]")
        logger.info("1. https://testnet.binancefuture.com ���속")
        logger.info("2. 로그인 후 API Key 생성")
        logger.info("3. .env 파일에 추가:")
        logger.info("   BINANCE_TESTNET_API_KEY=your_key_here")
        logger.info("   BINANCE_TESTNET_SECRET=your_secret_here")
        return

    # 잔고 확인
    balance = client.get_balance()
    logger.info(f"\n  테스트넷 잔고: ${balance['total']:,.2f} USDT")
    logger.info(f"  사용가능: ${balance['free']:,.2f} | 사용중: ${balance['used']:,.2f}")

    # 엔진 초기화
    engine = LiveTradingEngine(client)
    engine.load_state()

    # 기존 포지션 확인
    exchange_positions = client.get_positions()
    if exchange_positions:
        logger.info(f"\n  거래소 활성 포지션:")
        for p in exchange_positions:
            logger.info(f"    {p['symbol']}: {p['side']} {p['contracts']} @ {p['entryPrice']:,.2f}")

    # 즉시 첫 체크 실��
    logger.info("\n[초기 실행] 현재 시그널 체크...")
    run_signal_check(engine)

    # 스케줄 설정
    # 4h 봉 마감: 00:05, 04:05, 08:05, 12:05, 16:05, 20:05 UTC (5분 여유)
    for hour in ['00:05', '04:05', '08:05', '12:05', '16:05', '20:05']:
        schedule.every().day.at(hour).do(run_signal_check, engine)

    # 중간 스탑 체크: 매시간
    schedule.every(1).hours.do(run_stop_check, engine)

    logger.info("\n[스케줄 등록 완료]")
    logger.info("  4h 시그널 체크: 00:05, 04:05, 08:05, 12:05, 16:05, 20:05 UTC")
    logger.info("  스탑 체크: 매 1시간")
    logger.info("  Ctrl+C로 중지 (상태 자동 저장)\n")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("\n[중지] 상태 저장 중...")
        engine.save_state()
        logger.info("[완료] 안전하게 종료되었습니다.")


if __name__ == "__main__":
    main()
