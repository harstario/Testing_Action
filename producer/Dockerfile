# Use a slim version of Python to keep the image size small
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies required for confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY producer.py .

# Environment variable to ensure output is sent straight to terminal
ENV PYTHONUNBUFFERED=1

# Command to run the producer
CMD ["python", "producer.py"]