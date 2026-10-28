FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

RUN useradd --create-home appuser
USER appuser

# docker-compose.yml picks producer.py or consumer.py
CMD ["python", "consumer.py"]
