import sys, os, time, json
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from backtest.engine_v55 import run_portfolio_backtest

plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

VERSION  = "V55"
MONTHS   = 36
TARGET   = 3.5

RESULT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "results", "backtest_result_latest.txt")


class TeeOutput:
    """stdout을 터미널과 파일 양쪽에 동시에 출력"""
    def __init__(self, filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        self.file   = open(filepath, 'w', encoding='utf-8')
        self.stdout = sys.__stdout__
    def write(self, text):
        self.stdout.write(text)
        self.file.write(text)
    def flush(self):
        self.stdout.flush()
        self.file.flush()
    def close(self):
        self.file.close()


def print_stats(stats, initial_seed, final_assets, total_fees, monthly_pnl_dict):
    total    = stats["total"]
    win      = stats["win"]
    loss     = stats["loss"]
    win_rate = win / total * 100 if total > 0 else 0
    pf       = stats["gross_profit"] / stats["gross_loss"] if stats["gross_loss"] > 0 else float('inf')
    total_ret   = (final_assets - initial_seed) / initial_seed * 100
    monthly_ret = ((1 + total_ret/100) ** (1/MONTHS) - 1) * 100 if total_ret > -100 else 0
    mdd         = stats.get("max_drawdown", 0.0)

    aw = stats["gross_profit"] / win  if win  > 0 else 0
    al = stats["gross_loss"]   / loss if loss > 0 else 0
    rr = aw / al if al > 0 else 0

    print("\n" + "=" * 75)
    print(f"  [ BTC {VERSION} 백테스트 | 4시간봉 | 평균회귀(RSI+BB) | 롱온리 ]")
    print(f"  최종자산:  ${final_assets:,.2f}")
    print(f"  누적수익:  {total_ret:.2f}%")
    print(f"  월수익률:  {monthly_ret:.2f}%  (목표 {TARGET}%)")
    print(f"  MDD:       {mdd:.2f}%")
    print(f"  총수수료:  ${total_fees:,.2f}")
    print("-" * 75)
    print(f"  총거래:  {total}건 (월 {total/MONTHS:.1f}건)")
    print(f"  승/패:   {win}/{loss}  승률 {win_rate:.1f}%")
    print(f"  PF:      {pf:.2f}  손익비 {rr:.2f}")
    if win  > 0: print(f"  평균수익: +{aw:.2f}%")
    if loss > 0: print(f"  평균손실: -{al:.2f}%")
    print("-" * 75)
    for rk in ["BULL", "BEAR", "RANGE"]:
        rd = stats["regimes"][rk]
        rt = rd["total"]
        rw = rd["win"]
        rl = rd["loss"]
        rwr = rw / rt * 100 if rt > 0 else 0
        print(f"  {rk:<5}: 총{rt:>3}건 | 승{rw:>3} 패{rl:>3} | 승률{rwr:>5.1f}%")
    print("-" * 75)
    print(f"  손절:{stats['stop_count']}건 | 트레일:{stats['trailing_count']}건 | "
          f"조건익절:{stats['cond_exit_count']}건 | 분할:{stats['partial_exit_count']}건")
    print(f"  일일한도차단:{stats['daily_block_count']}건")
    print("=" * 75)

    pos_m = sum(1 for v in monthly_pnl_dict.values() if v > 0)
    print(f"  양수달: {pos_m}/{MONTHS}개월")

    print("\n  [월별 손익]")
    for ym, pnl in sorted(monthly_pnl_dict.items()):
        mark = "+" if pnl > 0 else "-"
        print(f"  {ym[0]}-{ym[1]:02d}  {mark} ${abs(pnl):>10,.2f}")


def start_test():
    tee = TeeOutput(RESULT_FILE)
    sys.stdout = tee

    initial_seed = 100_000
    t0 = time.time()
    history, total_fees, stats, monthly_pnl = run_portfolio_backtest(initial_budget=initial_seed)
    print(f"\n완료! ({time.time()-t0:.1f}초)")
    final = history[-1]
    print_stats(stats, initial_seed, final, total_fees, monthly_pnl)

    sys.stdout = sys.__stdout__
    tee.close()
    print(f"\n결과 저장됨: {RESULT_FILE}")

    total_ret = (final - initial_seed) / initial_seed * 100
    monthly   = ((1+total_ret/100)**(1/MONTHS)-1)*100 if total_ret > -100 else 0

    plt.figure(figsize=(14, 6))
    plt.plot(history, label=f'{VERSION} (월 {monthly:.2f}%)', color='darkblue', linewidth=1.5)
    plt.axhline(initial_seed, color='red', linestyle='--', alpha=0.6, label='원금 $100K')
    plt.title(f'BTC {VERSION} 3년 백테스트 - 누적 {total_ret:.1f}% | MDD {stats.get("max_drawdown",0):.1f}%')
    plt.ylabel('계좌 잔고 ($)')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    start_test()
    try: input(f"\n[{VERSION}] 완료. 엔터...")
    except KeyboardInterrupt: pass
