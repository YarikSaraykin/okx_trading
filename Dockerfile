# Python 3.12.8
FROM apache/airflow:2.10.4-python3.12

USER root
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow

COPY requirements.txt .

USER airflow
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt