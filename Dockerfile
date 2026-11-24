FROM python:3.14-slim

ENV AHT_HOME=/data \
    AHT_PORT=13321 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY app.py /app/app.py

RUN useradd -u 1000 -m aht \
    && mkdir -p /data \
    && chown aht /data

VOLUME /data
USER aht
EXPOSE 13321

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s \
    CMD python3 -c "import os,urllib.request;urllib.request.urlopen(f'http://127.0.0.1:{os.environ.get(\"AHT_PORT\",\"13321\")}/health',timeout=8)"

CMD ["python3", "/app/app.py"]
