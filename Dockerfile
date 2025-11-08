
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir rpyc requests

# Expose port if needed (for coordinator UI or worker)
EXPOSE 18861

# Run coordinator script
CMD ["python", "coordinator.py"]
