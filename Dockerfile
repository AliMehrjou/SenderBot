# استفاده از ایمیج سبک پایتون
FROM python:3.11-slim

# تنظیم متغیرهای محیطی
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=UTC

# نصب ابزارهای پایه برای کامپایل پکیج‌های پایتون (بسیار مهم برای asyncmy و TgCrypto)
RUN apt-get update && apt-get install -y gcc default-libmysqlclient-dev pkg-config tzdata && rm -rf /var/lib/apt/lists/*

# تنظیم مسیر کاری داخل کانتینر
WORKDIR /app

# کپی کردن فایل نیازمندی‌ها و نصب پکیج‌ها
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# کپی کردن کل سورس کد به داخل ایمیج
COPY . .

# دستور اجرای ربات
CMD ["python", "main.py"]