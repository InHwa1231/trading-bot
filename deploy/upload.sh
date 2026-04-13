#!/bin/bash
# ============================================
# 로컬 → Oracle Cloud 파일 업로드 스크립트
#
# 사용법:
#   bash deploy/upload.sh <서버IP> <SSH키경로>
#   예: bash deploy/upload.sh 123.45.67.89 ~/.ssh/oracle_key
# ============================================

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "사용법: bash deploy/upload.sh <서버IP> <SSH키경로>"
    echo "  예: bash deploy/upload.sh 123.45.67.89 ~/.ssh/oracle_key"
    exit 1
fi

SERVER_IP=$1
SSH_KEY=$2
REMOTE_USER="ubuntu"  # Oracle Cloud 기본 유저
REMOTE_DIR="~/trading_bot"

echo "========================================="
echo "  파일 업로드: $SERVER_IP"
echo "========================================="

# 원격 디렉토리 생성
ssh -i "$SSH_KEY" "$REMOTE_USER@$SERVER_IP" "mkdir -p $REMOTE_DIR/{broker,strategy,data,indicators,deploy,logs}"

# 핵심 파일만 업로드 (백테스트/결과 제외)
echo "[1/5] 메인 실행파일..."
scp -i "$SSH_KEY" run_paper_trade.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/"
scp -i "$SSH_KEY" requirements.txt "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/"
scp -i "$SSH_KEY" .env "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/"

echo "[2/5] broker..."
scp -i "$SSH_KEY" broker/binance_client.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/broker/"

echo "[3/5] strategy..."
scp -i "$SSH_KEY" strategy/live_engine.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/strategy/"
scp -i "$SSH_KEY" strategy/signal_generator.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/strategy/"

echo "[4/5] data & indicators..."
scp -i "$SSH_KEY" data/collector.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/data/"
scp -i "$SSH_KEY" indicators/__init__.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/indicators/"
scp -i "$SSH_KEY" indicators/calculators_v56_plus.py "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/indicators/"

echo "[5/5] deploy scripts..."
scp -i "$SSH_KEY" deploy/setup_oracle.sh "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/deploy/"
scp -i "$SSH_KEY" deploy/install_service.sh "$REMOTE_USER@$SERVER_IP:$REMOTE_DIR/deploy/"

echo ""
echo "[완료] 업로드 완료!"
echo ""
echo "  다음 단계:"
echo "  1. ssh -i $SSH_KEY $REMOTE_USER@$SERVER_IP"
echo "  2. bash ~/trading_bot/deploy/setup_oracle.sh"
echo "  3. bash ~/trading_bot/deploy/install_service.sh"
