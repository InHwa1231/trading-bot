# Fly.io 배포 가이드 (V56-plus 페이퍼 트레이딩 봇)

## 📋 사전 준비
- Fly.io 계정 (무료): https://fly.io
- Fly CLI 설치 완료
- API 키 준비 (BINANCE_TESTNET_API_KEY, BINANCE_TESTNET_SECRET)

---

## 🚀 배포 절차 (5분)

### **1단계: Fly.io 로그인**
```bash
fly auth login
```
- 브라우저가 열림 → Fly.io 로그인 → 허용

### **2단계: 앱 초기화 (처음 1회만)**
프로젝트 루트 디렉토리에서:
```bash
fly launch
```

**다음과 같이 응답:**
```
App name? → trading-bot-v56plus (또는 원하는 이름)
Organization? → personal (기본값)
Region? → nrt (Tokyo - 권장) 또는 sjc (San Jose)
Would you like to set up a Postgresql database? → No
Would you like to set up an Upstash Redis database? → No
Would you like to deploy now? → No (일단 설정 먼저)
```

### **3단계: 환경 변수 설정**
```bash
# 1. 시크릿 추가 (API 키)
fly secrets set BINANCE_TESTNET_API_KEY="your_api_key"
fly secrets set BINANCE_TESTNET_SECRET="your_secret_key"

# 2. 확인
fly secrets list
```

### **4단계: 배포**
```bash
fly deploy
```

**첫 배포 시간: 2-3분** (이미지 빌드 + 업로드)

---

## ✅ 배포 확인

```bash
# 1. 앱 상태 확인
fly status

# 2. 로그 실시간 보기 (가장 중요!)
fly logs

# 3. SSH로 직접 접속 (트러블슈팅)
fly ssh console
```

**예상 로그:**
```
[INFO] [4H 봉 마감] 전체 시그널 체크 시작
[INFO] BTC/USDT: 포지션 없음, 진입 신호 대기...
[INFO] 익절: BTC 포지션 50% 매도
...
```

---

## 🔄 업데이트 (코드 수정 후)

```bash
# 1. 코드 수정 → git commit
git add .
git commit -m "fix: 파라미터 조정"

# 2. Fly에 다시 배포
fly deploy

# 3. 로그 확인
fly logs
```

---

## 💰 비용 (월별)

| 항목 | 무료 크레딧 | 초과 비용 |
|------|----------|---------|
| 공유 VM (256MB) | **포함됨** | $0/월 |
| 대역폭 (1GB/월) | **포함됨** | $0.02/GB |
| **총계** | **$0/월** | **$0-1/월** |

대역폭은 API 호출(HTTPS)에만 소모 → **월 $0-1 범위**

---

## 🛑 중지 / 삭제

```bash
# 앱 중지 (자동 재시작 안 됨)
fly scale count 0

# 앱 완전 삭제
fly app delete trading-bot-v56plus
```

---

## 🐛 트러블슈팅

### **"Command not found: fly"**
```bash
# CLI 재설치
choco install flyctl -y
# 또는
scoop install flyctl
```

### **로그가 보이지 않음**
```bash
fly logs --follow  # 실시간 모드
```

### **메모리 부족 (OOM)**
```bash
# fly.toml의 memory를 512mb → 1024mb로 변경
fly deploy
```

### **API 키 잘못됨**
```bash
# 시크릿 업데이트
fly secrets set BINANCE_TESTNET_API_KEY="new_key"
fly secrets set BINANCE_TESTNET_SECRET="new_secret"

# 자동 재배포
fly restart
```

---

## 📊 모니터링

```bash
# 메트릭 확인 (CPU, 메모리, 대역폭)
fly metrics

# 자세한 상태
fly status -v
```

---

## 💡 팁

1. **로그는 매일 확인**: `fly logs` 명령어로 수익/손실 트래킹
2. **주 1회 업데이트**: 파라미터 조정 후 배포
3. **비용 모니터링**: https://fly.io/dashboard → Billing

---

**배포 성공하면 알려주세요! 🚀**
