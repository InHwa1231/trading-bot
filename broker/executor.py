import alpaca_trade_api as tradeapi
import os
from dotenv import load_dotenv

load_dotenv()

class AlpacaBroker:
    def __init__(self):
        self.api = tradeapi.REST(
            os.getenv("ALPACA_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY"),
            os.getenv("ALPACA_BASE_URL"),
            api_version='v2'
        )

    def get_account_info(self):
        acc = self.api.get_account()
        return float(acc.cash), float(acc.equity)

    def buy_order(self, ticker, weight, total_equity):
        # 최신 가격 가져오기
        price = float(self.api.get_latest_trade(ticker).price)
        # 넘겨받은 비중(ratio)을 바탕으로 수량 계산
        qty = int((total_equity * weight) // price)
        
        if qty > 0:
            try:
                # 시간외 거래를 위해 지정가(limit) 주문 사용 (현재가 대비 1% 여유)
                self.api.submit_order(
                    symbol=ticker, qty=qty, side='buy', type='limit',
                    limit_price=round(price * 1.01, 2), # 즉시 체결을 위해 살짝 높게
                    time_in_force='day', extended_hours=True
                )
                print(f"   >>> [주문 성공] {ticker} {qty}주 주문 완료")
            except Exception as e:
                print(f"   >>> [주문 실패] {ticker}: {e}")