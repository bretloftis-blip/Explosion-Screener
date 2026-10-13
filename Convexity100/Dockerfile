FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 DB_PATH=/app/data/scanner.sqlite
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home scanner && mkdir -p /app/data && chown scanner:scanner /app/data
COPY scanner.py audit.py ./
USER scanner
CMD ["python", "scanner.py"]
