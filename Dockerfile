# personal-finance-helper
# Build: docker build -t personal-finance-helper .
# Run:   docker compose up

FROM python:3.11-slim

# Install system deps for psycopg2 (PostgreSQL client) and curl (healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache-friendly)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app.py pipeline.py query_layer.py ./

# data/ is gitignored and never copied — users upload via the UI
# The DB file (SQLite) lives in /app/db volume

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
