#!/bin/bash
# ============================================
# Oracle Cloud Free Tier 배포 스크립트
# V56-plus 페이퍼 트레이딩 봇
# ============================================

echo "========================================="
echo "  V56-plus 트레이딩 봇 서버 설정"
echo "========================================="

# 1. 시스템 업데이트
echo "[1/5] 시스템 업데이트..."
sudo apt update && sudo apt upgrade -y

# 2. Python 설치
echo "[2/5] Python 설치..."
sudo apt install -y python3 python3-pip python3-venv git

# 3. 프로젝트 디렉토리 설정
echo "[3/5] 프로젝트 설정..."
mkdir -p ~/trading_bot
cd ~/trading_bot

# 가상환경 생성
python3 -m venv venv
source venv/bin/activate

# 패키지 설치
pip install --upgrade pip
pip install ccxt pandas numpy ta schedule python-dotenv

echo "[4/5] 파일 업로드 필요"
echo ""
echo "  아래 파일들을 이 서버에 업로드하세요:"
echo "  (scp 또는 FileZilla 사용)"
echo ""
echo "  ~/trading_bot/"
echo "    ├── run_paper_trade.py"
echo "    ├── .env"
echo "    ├── broker/"
echo "    │   └── binance_client.py"
echo "    ├── strategy/"
echo "    │   ├── live_engine.py"
echo "    │   └── signal_generator.py"
echo "    ├── data/"
echo "    │   └── collector.py"
echo "    └── indicators/"
echo "        ├── __init__.py"
echo "        └── calculators_v56_plus.py"
echo ""
echo "  업로드 완료 후 다음 명령어 실행:"
echo "  sudo bash ~/trading_bot/deploy/install_service.sh"
echo ""

echo "[5/5] 방화벽 설정..."
# Oracle Cloud는 기본적으로 iptables 규칙이 있음
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 22 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || true

echo ""
echo "[완료] 기본 설정 완료!"
echo "  다음 단계: 파일 업로드 → install_service.sh 실행"
