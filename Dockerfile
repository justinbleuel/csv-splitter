FROM python:3.10-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set environment variables
ENV PORT=8000

# Command to run the application
CMD gunicorn flask_app:app --bind 0.0.0.0:$PORT --timeout 300
