FROM apache/airflow:2.8.1-python3.11

# Switch to root to install system packages
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && apt-get clean

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Set up environment
ENV PYTHONUNBUFFERED=1
WORKDIR /opt/airflow
