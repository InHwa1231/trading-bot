def decide_strategy(cash, total_equity, analysis_results, current_positions_info):
    orders = {}
    INITIAL_BUDGET = 100000 
    UNIT_AMOUNT = INITIAL_BUDGET * 0.01 # $1,000

    print(f"\n[전략 판단] 고정 금액 증액 매수 (모든 조건 충족 시만 작동)")

    position_values = {p.symbol: float(p.current_price) * float(p.qty) for p in current_positions_info}

    for ticker in ['TQQQ', 'UPRO', 'GLD']:
        res = analysis_results.get(ticker)
        if not res: continue

        buy_score = res.get('buy_score', 0)
        current_value = position_values.get(ticker, 0)

        # [수정] 매수 조건: 4가지 지표 모두 충족(4점) 시에만 매수 실행
        if buy_score == 4:
            if current_value < UNIT_AMOUNT * 0.5: # 1회차
                target_ratio = UNIT_AMOUNT / total_equity
                print(f" > {ticker}: [1회차] 모든 조건 충족! $1,000 매수")
            elif current_value < UNIT_AMOUNT * 1.5: # 2회차
                target_ratio = (UNIT_AMOUNT * 2) / total_equity
                print(f" > {ticker}: [2회차] 모든 조건 충족! $2,000 증액 매수")
            elif current_value < UNIT_AMOUNT * 3.5: # 3회차
                target_ratio = (UNIT_AMOUNT * 3) / total_equity
                print(f" > {ticker}: [3회차] 모든 조건 충족! $3,000 증액 매수")
            else: # 4회차 이후
                target_ratio = UNIT_AMOUNT / total_equity
                print(f" > {ticker}: [추가] 모든 조건 충족! $1,000 추가 매수")
            
            orders[ticker] = target_ratio

        # 매도 조건: 3가지 지표 모두 충족 시 전량 매도
        if res.get('sell_signal') == True:
            orders[ticker] = 0
            print(f" > {ticker}: 매도 조건 3가지 모두 충족! 전량 매도")

    return orders