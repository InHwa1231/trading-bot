"""
Binance Futures Testnet 클라이언트

바이낸스 선물 테스트넷을 통한 페이퍼 트레이딩
- Testnet API: https://testnet.binancefuture.com
- 실거래소와 동일한 API 구조, 가상 자금 사용
"""
import ccxt
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("binance_client")


class BinanceFuturesClient:
    def __init__(self, testnet=True):
        api_key = os.getenv("BINANCE_TESTNET_API_KEY", "")
        api_secret = os.getenv("BINANCE_TESTNET_SECRET", "")

        if not api_key or not api_secret:
            raise ValueError(
                "BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_SECRET 가 .env에 없습니다.\n"
                "https://testnet.binancefuture.com 에서 발급 후 .env에 추가하세요."
            )

        if testnet:
            # USDT-M 선물 전용 + 테스트넷 URL 직접 설정
            self.exchange = ccxt.binanceusdm({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
                'options': {
                    'fetchCurrencies': False,
                },
            })
            # 테스트넷 URL 오버라이드
            testnet_base = 'https://testnet.binancefuture.com'
            for key in list(self.exchange.urls['api'].keys()):
                url = self.exchange.urls['api'][key]
                if isinstance(url, str) and 'fapi' in url:
                    self.exchange.urls['api'][key] = url.replace(
                        'https://fapi.binance.com', testnet_base
                    )
        else:
            self.exchange = ccxt.binanceusdm({
                'apiKey': api_key,
                'secret': api_secret,
                'enableRateLimit': True,
            })

        logger.info(f"Binance Futures {'Testnet' if testnet else 'Live'} 연결")

    def _sym(self, symbol):
        """심볼 형식 변환: BTC/USDT → BTC/USDT:USDT"""
        if ':' not in symbol:
            return symbol + ':USDT'
        return symbol

    def get_balance(self):
        """USDT 잔고 조회"""
        balance = self.exchange.fetch_balance()
        usdt = balance.get('USDT', {})
        return {
            'total': float(usdt.get('total', 0)),
            'free': float(usdt.get('free', 0)),
            'used': float(usdt.get('used', 0)),
        }

    def get_positions(self):
        """열린 포지션 조회"""
        positions = self.exchange.fetch_positions()
        active = []
        for p in positions:
            amt = float(p.get('contracts', 0))
            if amt != 0:
                active.append({
                    'symbol': p['symbol'],
                    'side': p['side'],
                    'contracts': amt,
                    'entryPrice': float(p.get('entryPrice') or 0),
                    'unrealizedPnl': float(p.get('unrealizedPnl') or 0),
                    'leverage': int(p.get('leverage') or 1),
                    'notional': float(p.get('notional') or 0),
                })
        return active

    def set_leverage(self, symbol, leverage):
        """레버리지 설정"""
        symbol = self._sym(symbol)
        try:
            self.exchange.set_leverage(leverage, symbol)
            logger.info(f"{symbol} 레버리지 {leverage}x 설정")
        except Exception as e:
            logger.warning(f"레버리지 설정 실패 {symbol}: {e}")

    def set_margin_mode(self, symbol, mode='isolated'):
        """마진 모드 설정 (isolated/cross)"""
        symbol = self._sym(symbol)
        try:
            self.exchange.set_margin_mode(mode, symbol)
            logger.info(f"{symbol} 마진모드 {mode} 설정")
        except Exception as e:
            # 이미 설정된 경우 무시
            if 'No need to change' not in str(e):
                logger.warning(f"마진모드 설정 실패 {symbol}: {e}")

    def market_buy(self, symbol, amount_usdt, leverage):
        """시장가 롱 진입"""
        symbol = self._sym(symbol)
        self.set_leverage(symbol, leverage)
        ticker = self.exchange.fetch_ticker(symbol)
        price = ticker['last']
        # 계약 수량 계산 (USDT 기준)
        notional = amount_usdt * leverage
        quantity = notional / price

        # 바이낸스 수량 정밀도 적용
        market = self.exchange.market(symbol)
        quantity = self.exchange.amount_to_precision(symbol, quantity)
        quantity = float(quantity)

        if quantity <= 0:
            logger.warning(f"수량이 0 이하: {symbol} {quantity}")
            return None

        try:
            order = self.exchange.create_market_buy_order(symbol, quantity)
            logger.info(f"롱 진입: {symbol} {quantity} @ ~{price:.2f} (lev:{leverage}x)")
            return order
        except Exception as e:
            logger.error(f"롱 진입 실패 {symbol}: {e}")
            return None

    def market_sell(self, symbol, quantity):
        """시장가 롱 청산 (포지션 전량)"""
        symbol = self._sym(symbol)
        quantity = float(self.exchange.amount_to_precision(symbol, abs(quantity)))

        try:
            order = self.exchange.create_market_sell_order(
                symbol, quantity, params={'reduceOnly': True}
            )
            logger.info(f"롱 청산: {symbol} {quantity}")
            return order
        except Exception as e:
            logger.error(f"롱 청산 실패 {symbol}: {e}")
            return None

    def partial_close(self, symbol, quantity, ratio):
        """분할 청산"""
        close_qty = quantity * ratio
        return self.market_sell(symbol, close_qty)

    def get_ohlcv(self, symbol, timeframe='4h', limit=100):
        """최근 OHLCV 데이터 조회"""
        symbol = self._sym(symbol)
        ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        import pandas as pd
        df = pd.DataFrame(ohlcv, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
        df.set_index('Timestamp', inplace=True)
        return df

    def get_ticker_price(self, symbol):
        """현재가 조회"""
        symbol = self._sym(symbol)
        ticker = self.exchange.fetch_ticker(symbol)
        return ticker['last']
