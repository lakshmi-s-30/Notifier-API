# worker_service/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./worker_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./worker_service/ .

# Command is set in docker-compose.yml: python worker.py