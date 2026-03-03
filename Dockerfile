from python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

copy requirements.txt .
run pip install --no-cache-dir -r requirements.txt

copy . .

cmd ["python", "src/collector.py"]