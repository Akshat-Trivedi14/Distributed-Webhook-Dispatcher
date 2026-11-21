FROM python:3.13-slim

WORKDIR /srv
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Hosting platforms inject $PORT. --proxy-headers makes the real client IP visible behind their proxy.
CMD uvicorn app.main:app_factory --factory --host 0.0.0.0 --port ${PORT:-8000} \
    --proxy-headers --forwarded-allow-ips='*'
