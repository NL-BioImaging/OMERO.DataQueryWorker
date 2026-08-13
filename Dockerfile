FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DQW_CACHE_DIR=/var/lib/omero-data-query-worker \
    TMPDIR=/var/lib/omero-data-query-worker/tmp

RUN groupadd --gid 10001 dataquery \
    && useradd --uid 10001 --gid dataquery --no-create-home --shell /usr/sbin/nologin dataquery \
    && mkdir -p /var/lib/omero-data-query-worker /app \
    && chown -R dataquery:dataquery /var/lib/omero-data-query-worker /app

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY scripts/smoke_inside.py ./scripts/smoke_inside.py

RUN python -m pip install --no-cache-dir . \
    && python -m pip check \
    && rm -rf /root/.cache

USER 10001:10001

VOLUME ["/var/lib/omero-data-query-worker"]
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=5 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/ready', timeout=2).read()"]

CMD ["python", "-m", "omero_data_query_worker"]
