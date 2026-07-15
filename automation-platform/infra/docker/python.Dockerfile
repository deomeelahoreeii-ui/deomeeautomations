FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY apps/api ./apps/api
COPY apps/worker ./apps/worker
COPY packages ./packages
COPY alembic.ini ./alembic.ini
COPY alembic ./alembic

RUN uv sync --locked

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONPATH="/app/apps/api:/app/apps/worker:/app/packages/automation_core:/app/packages/crm_filters"
