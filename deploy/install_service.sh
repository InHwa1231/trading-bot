#!/bin/bash
# ============================================
# systemd 서비스 등록 (자동 시작 + 크래시 복구)
# ============================================

BOT_DIR="$HOME/trading_bot"
USER=$(whoami)

echo "========================================="
echo "  systemd 서비스 등록"
echo "========================================="

# .env 파일 확인
if [ ! -f "$BOT_DIR/.env" ]; then
    echo "[오류] .env 파일이 없습니다!"
    echo "  $BOT_DIR/.env 에 다음 내용을 넣으세요:"
    echo "  BINANCE_TESTNET_API_KEY=your_key"
    echo "  BINANCE_TESTNET_SECRET=your_secret"
    exit 1
fi

# run_paper_trade.py 확인
if [ ! -f "$BOT_DIR/run_paper_trade.py" ]; then
    echo "[오류] run_paper_trade.py가 없습니다!"
    echo "  프로젝트 파일을 $BOT_DIR/ 에 업로드하세요."
    exit 1
fi

# 로그 디렉토리
mkdir -p "$BOT_DIR/logs"

# systemd 서비스 파일 생성
sudo tee /etc/systemd/system/trading-bot.service > /dev/null << EOF
[Unit]
Description=V56-plus Paper Trading Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$BOT_DIR
ExecStart=$BOT_DIR/venv/bin/python $BOT_DIR/run_paper_trade.py
Restart=always
RestartSec=30
Environment=PYTHONUNBUFFERED=1

# 크래시 시 자동 재시작 (최대 5회/5분)
StartLimitIntervalSec=300
StartLimitBurst=5

# 로그
StandardOutput=append:$BOT_DIR/logs/service.log
StandardError=append:$BOT_DIR/logs/service_error.log

[Install]
WantedBy=multi-user.target
EOF

# 서비스 등록 및 시작
sudo systemctl daemon-reload
sudo systemctl enable trading-bot
sudo systemctl start trading-bot

echo ""
echo "[완료] 서비스 등록 완료!"
echo ""
echo "  상태 확인:  sudo systemctl status trading-bot"
echo "  로그 보기:  tail -f ~/trading_bot/logs/service.log"
echo "  중지:       sudo systemctl stop trading-bot"
echo "  재시작:     sudo systemctl restart trading-bot"
echo ""

# 상태 출력
sudo systemctl status trading-bot --no-pager
