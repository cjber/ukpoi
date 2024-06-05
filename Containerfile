FROM python:3.12

ENV VIRTUAL_ENV=/usr/local

ENV DAGSTER_HOME=/opt/dagster/dagster_home/
RUN mkdir -p $DAGSTER_HOME /opt/dagster/app
COPY dagster.yaml $DAGSTER_HOME

WORKDIR /opt/dagster/app

COPY requirements.txt pyproject.toml .env ./
RUN pip install --no-cache -r requirements.txt
