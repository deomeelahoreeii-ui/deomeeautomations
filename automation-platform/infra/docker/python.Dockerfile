FROM python:3.14.4-slim

ARG APP_UID=1000
ARG APP_GID=1000
ARG INSTALL_OCR=false

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_SYNC=1

WORKDIR /app

RUN if [ "${INSTALL_OCR}" = "true" ]; then \
        apt-get update \
        && apt-get install --yes --no-install-recommends poppler-utils tesseract-ocr \
        && rm -rf /var/lib/apt/lists/*; \
    fi \
    && groupadd --gid "${APP_GID}" automation \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home automation \
    && pip install --no-cache-dir uv==0.11.19

COPY pyproject.toml uv.lock README.md ./
COPY apps/api ./apps/api
COPY apps/worker ./apps/worker
COPY packages ./packages
COPY alembic.ini ./alembic.ini
COPY alembic ./alembic

RUN uv sync --locked --no-dev \
    && mkdir -p /app/data \
    && chown -R automation:automation /app

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app/apps/api:/app/apps/worker:/app/packages/automation_core:/app/packages/crm_filters"

USER automation
