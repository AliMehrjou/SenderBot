FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Tehran

WORKDIR /app

ARG APP_UID=1000
ARG APP_GID=1000
RUN groupadd -g ${APP_GID} app && \
    useradd -u ${APP_UID} -g app -d /app -s /usr/sbin/nologin appuser

RUN apt-get update -o Acquire::Retries=30 -o Acquire::http::Timeout=120 -o Acquire::ForceIPv4=true && \
    apt-get install -y --no-install-recommends -o Acquire::Retries=30 -o Acquire::http::Timeout=120 -o Acquire::ForceIPv4=true \
    gcc \
    tzdata \
    libmediainfo0v5 && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/sessions /app/downloads /app/exports /app/banners /app/profile_photos && \
    chown -R appuser:app /app

USER appuser

CMD ["python", "main.py"]