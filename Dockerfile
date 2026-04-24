FROM apache/airflow:3.2.0-python3.12

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