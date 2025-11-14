# /opt/infra/stacks/ingest/services/raw-writer/Dockerfile
FROM python:3.12-slim

RUN pip install --no-cache-dir paho-mqtt psycopg2-binary

WORKDIR /app
COPY ingestor.py /app/ingestor.py

CMD ["python", "/app/ingestor.py"]
