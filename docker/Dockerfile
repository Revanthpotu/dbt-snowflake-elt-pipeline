FROM python:3.11-slim

# ── System dependencies ──────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ── Python dependencies ──────────────────────────────────────────────────────
# Pin versions for reproducible builds
RUN pip install --no-cache-dir \
    dbt-core==1.8.3 \
    dbt-duckdb==1.8.1 \
    dbt-snowflake==1.8.3 \
    duckdb==1.0.0

# ── Working directory ────────────────────────────────────────────────────────
WORKDIR /usr/app

# ── Copy project files ───────────────────────────────────────────────────────
COPY dbt_project/ /usr/app/dbt_project/

# ── profiles.yml: place in dbt default location ──────────────────────────────
# The profiles.yml is already inside dbt_project/ and we point DBT_PROFILES_DIR
# to that location via the env var in docker-compose.yml

# ── Expose port for dbt docs serve ───────────────────────────────────────────
EXPOSE 8080

# ── Default command: run full pipeline + tests ───────────────────────────────
CMD ["bash", "-c", \
    "cd /usr/app/dbt_project && \
     dbt deps && \
     dbt seed && \
     dbt run && \
     dbt test && \
     echo '✅ Pipeline complete!'"]
