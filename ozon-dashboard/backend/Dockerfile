FROM python:3.11-slim-bookworm

WORKDIR /app

# Зависимости Chromium для Playwright (заход на ozon.ru как браузер)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 libx11-xcb1 libxcb1 \
    libxext6 libx11-6 fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install chromium

COPY . .

ENV PORT=8080
EXPOSE 8080

CMD ["python", "main.py"]
