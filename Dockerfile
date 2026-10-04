FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Tehran

WORKDIR /app

ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g ${APP_GID} app && \
    useradd -u ${APP_UID} -g app -d /app -s /usr/sbin/nologin appuser

# تنظیمات مربوط به apt-get برای پایداری بیشتر در شبکه‌های ناپایدار
RUN apt-get update -o Acquire::Retries=30 -o Acquire::http::Timeout=120 -o Acquire::ForceIPv4=true && \
    apt-get install -y --no-install-recommends -o Acquire::Retries=30 -o Acquire::http::Timeout=120 -o Acquire::ForceIPv4=true \
    gcc \
    tzdata \
    libmediainfo0v5 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# استفاده از مخزن Tsinghua (چینهوا) برای آپگرید pip و نصب پکیج‌ها
RUN pip install --no-cache-dir --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install --no-cache-dir -r requirements.txt \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    --default-timeout=120 --retries=10

COPY . .

# ساخت دایرکتوری‌های مورد نیاز بات و تنظیم سطح دسترسی
RUN mkdir -p /app/sessions /app/downloads /app/exports /app/banners /app/profile_photos && \
    chown -R appuser:app /app

USER appuser

CMD ["python", "main.py"]