"""V57-v2 백테스트 실행 - 8자산 확장 + 중간 레버리지"""
import sys, os, time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 출력 저장
class TeeOutput:
    def __init__(self, file_path):
        self.terminal = sys.stdout
        self.log = open(file_path, 'w', encoding='utf-8')
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
os.makedirs(result_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
result_path = os.path.join(result_dir, f"V57_{timestamp}")
os.makedirs(result_path, exist_ok=True)
sys.stdout = TeeOutput(os.path.join(result_path, "backtest_log.txt"))

from backtest.engine_v57 import run_portfolio_backtest

start = time.time()
history, total_fees, stats, monthly_pnl, daily_pnl = run_portfolio_backtest(100_000)
elapsed = time.time() - start

final = history[-1]
ret = (final / 100_000 - 1) * 100
total_months = len(monthly_pnl)
monthly_avg = ret / max(total_months, 1)

# 일별 수익률 분석
if daily_pnl:
    import statistics
    daily_returns = list(daily_pnl.values())
    total_days = len(daily_returns)
    positive_days = sum(1 for d in daily_returns if d > 0)
    avg_daily_pnl = statistics.mean(daily_returns)
    avg_daily_ret = avg_daily_pnl / 100_000 * 100  # 근사치
else:
    total_days = 0
    positive_days = 0
    avg_daily_ret = 0

print("\n" + "=" * 70)
print(f"  V57-v3 백테스트 결과 - 선별 자산 + 레버리지6-10x")
print("=" * 70)
print(f"  최종 자산: ${final:,.0f} (수익률: {ret:+.1f}%)")
print(f"  총 거래: {stats['total']}건 | 승: {stats['win']} | 패: {stats['loss']} | 보합: {stats['breakeven']}")
print(f"  승률: {stats['win']/max(stats['total'],1)*100:.1f}%")

gp = stats['gross_profit']
gl = stats['gross_loss']
pf = gp / gl if gl > 0 else float('inf')
print(f"  PF: {pf:.2f} (총이익 {gp:.1f}% / 총손실 {gl:.1f}%)")
print(f"  최대 수익: +{stats['max_win']:.1f}% | 최대 손실: -{stats['max_loss']:.1f}%")
print(f"  MDD: {stats['max_drawdown']:.1f}%")
print(f"  총 수수료: ${total_fees:,.0f}")
print(f"  월평균 수익률: {monthly_avg:.2f}%")
print(f"  일평균 수익률: {avg_daily_ret:.3f}%")
print(f"  총 거래일: {total_days}일 | 수익일: {positive_days}일 ({positive_days/max(total_days,1)*100:.1f}%)")
print(f"  실행 시간: {elapsed:.1f}초")

print(f"\n  [전략별 상세]")
for key, sd in sorted(stats['by_strat'].items()):
    wr = sd['win'] / max(sd['total'], 1) * 100
    spf = sd['profit'] / sd['loss_amt'] if sd['loss_amt'] > 0 else float('inf')
    print(f"    {key}: {sd['total']}건 | 승률 {wr:.0f}% | PF {spf:.2f}")

print(f"\n  [월별 손익]")
for ym in sorted(monthly_pnl.keys()):
    mp = monthly_pnl[ym]
    print(f"    {ym[0]}-{ym[1]:02d}: ${mp:>+10,.0f}")

# 그래프
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('V57-v3 Backtest: Selected Assets + Lev 6-10x', fontsize=14, fontweight='bold')

    # 1. 자산 곡선
    axes[0, 0].plot(history, linewidth=0.8)
    axes[0, 0].axhline(y=100_000, color='gray', linestyle='--', alpha=0.5)
    axes[0, 0].set_title(f'Equity: ${final:,.0f} ({ret:+.1f}%)')
    axes[0, 0].set_ylabel('Equity ($)')

    # 2. 월별 수익
    months = sorted(monthly_pnl.keys())
    vals = [monthly_pnl[m] for m in months]
    labels = [f"{m[0]%100}-{m[1]:02d}" for m in months]
    colors = ['green' if v > 0 else 'red' for v in vals]
    axes[0, 1].bar(range(len(vals)), vals, color=colors, alpha=0.7)
    step = max(1, len(labels) // 12)
    axes[0, 1].set_xticks(range(0, len(labels), step))
    axes[0, 1].set_xticklabels([labels[i] for i in range(0, len(labels), step)], rotation=45)
    axes[0, 1].set_title(f'Monthly PnL (avg: {monthly_avg:.2f}%/mo)')
    axes[0, 1].axhline(y=0, color='black', linewidth=0.5)

    # 3. 드로다운
    peak = pd.Series(history).cummax()
    dd = (pd.Series(history) - peak) / peak * 100
    axes[1, 0].fill_between(range(len(dd)), dd, 0, alpha=0.5, color='red')
    axes[1, 0].set_title(f'Drawdown (MDD: {stats["max_drawdown"]:.1f}%)')
    axes[1, 0].set_ylabel('Drawdown (%)')

    # 4. 전략별 거래 수
    strat_names = sorted(stats['by_strat'].keys())
    strat_counts = [stats['by_strat'][k]['total'] for k in strat_names]
    strat_wins = [stats['by_strat'][k]['win'] for k in strat_names]
    x = np.arange(len(strat_names))
    axes[1, 1].bar(x - 0.15, strat_counts, 0.3, label='Total', alpha=0.7)
    axes[1, 1].bar(x + 0.15, strat_wins, 0.3, label='Win', color='green', alpha=0.7)
    axes[1, 1].set_xticks(x)
    axes[1, 1].set_xticklabels(strat_names, rotation=45, fontsize=7)
    axes[1, 1].set_title('Trades by Strategy')
    axes[1, 1].legend()

    plt.tight_layout()
    chart_path = os.path.join(result_path, "backtest_chart.png")
    plt.savefig(chart_path, dpi=150)
    print(f"\n  차트 저장: {chart_path}")
except Exception as e:
    print(f"\n  차트 생성 실패: {e}")

print("\n[완료]")
