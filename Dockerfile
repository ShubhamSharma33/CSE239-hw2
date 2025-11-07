FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir rpyc

COPY coordinator.py .
COPY worker.py .

RUN mkdir -p txt

CMD ["python", "coordinator.py"]