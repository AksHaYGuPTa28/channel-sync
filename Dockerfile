# Demo image: control room + dashboard + its own Postgres, in one container.
# Fine for a demo where every run starts from an empty schema; not how you'd run the real thing.

FROM node:20-slim AS dashboard
WORKDIR /web
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

FROM python:3.12-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql \
    && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 app

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
COPY db db
COPY sim sim
COPY deploy/start.sh deploy/start.sh
COPY --from=dashboard /web/dist dashboard/dist

USER app
ENV HOST=0.0.0.0 \
    PORT=7860 \
    PGDATA=/home/app/pgdata \
    DATABASE_URL=postgresql://app@localhost:5432/sync \
    MAX_ACTIONS=5000 \
    MAX_DURATION=60 \
    PYTHONUNBUFFERED=1
EXPOSE 7860
CMD ["sh", "deploy/start.sh"]
