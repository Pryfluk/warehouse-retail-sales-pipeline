# Dockerfile สำหรับ Python services
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Copy code
COPY . .

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# คำสั่ง default (จะถูก override ด้วย docker-compose)
CMD ["python", "ingest.py"]
