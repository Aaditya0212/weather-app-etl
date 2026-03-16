FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Create data directory for CSV
RUN mkdir -p data

# Default: run the pipeline once
CMD ["python", "pipeline.py"]
