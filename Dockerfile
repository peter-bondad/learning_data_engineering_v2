FROM apache/airflow:2.9.1-python3.10

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/airflow

COPY requirements.txt .

USER airflow
RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN apt-get remove -y gcc && apt-get autoremove -y

USER airflow