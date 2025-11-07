FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install rpyc

# Copy files
COPY worker.py .
COPY coordinator.py .

# Expose RPyC port
EXPOSE 18861

# Default command (can be overridden in docker-compose)
CMD ["python", "worker.py"]