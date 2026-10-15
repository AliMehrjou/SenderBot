FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Tehran

WORKDIR /app

ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g ${APP_GID} app && \
    useradd -u ${APP_UID} -g app -d /app -s /usr/sbin/nologin appuser

# تغییر مخزن پیش‌فرض Debian به Tsinghua و نصب پکیج‌ها
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update -o Acquire::Retries=5 -o Acquire::http::Timeout=30 -o Acquire::ForceIPv4=true && \
    apt-get install -y --no-install-recommends -o Acquire::Retries=5 -o Acquire::http::Timeout=30 -o Acquire::ForceIPv4=true \
    gcc \
    tzdata \
    libmediainfo0v5 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# استفاده از مخزن Aliyun (یا حذف کامل -i برای استفاده از مخزن اصلی با timeout بالا)
RUN pip install --no-cache-dir --upgrade pip -i https://mirrors.aliyun.com/pypi/simple/ && \
    pip install --no-cache-dir -r requirements.txt \
    -i https://mirrors.aliyun.com/pypi/simple/ \
    --default-timeout=120 --retries=10

COPY . .

# ساخت دایرکتوری‌های مورد نیاز بات و تنظیم سطح دسترسی
RUN mkdir -p /app/sessions /app/downloads /app/exports /app/banners /app/profile_photos && \
    chown -R appuser:app /app

USER appuser

CMD ["python", "main.py"]