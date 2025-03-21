FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY data_logger.py .
COPY sensor_data.csv .

RUN pip install --no-cache-dir numpy==1.23.5 pandas==1.5.3 pymongo==4.3.3 python-dotenv==1.0.0

CMD ["python", "data_logger.py"]