# Fly.io V56-plus 페이퍼 트레이딩 봇
FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 업데이트
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY . .

# 로그 디렉토리 생성
RUN mkdir -p logs

# 환경변수 기본값 (실제는 fly.toml에서 설정)
ENV PYTHONUNBUFFERED=1

# 24/7 실행
CMD ["python", "run_paper_trade.py"]
