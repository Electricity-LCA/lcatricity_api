FROM python:3.12.3-slim-bullseye

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Prevent Python from writing pyc files to disc
ENV PYTHONDONTWRITEBYTECODE 1
# Prevent Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED 1

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# RUN python3 -m ensurepip --upgrade
COPY ./ /usr/src/app/lcatricity_api

# For microservice background NOTE THIS MUST BE SET MANUALLY TO MATCH .env
EXPOSE 9000

ENV PYTHONPATH="/lcatricity_api"
CMD uvicorn lcatricity_api.microservice.main:app --reload --host 0.0.0.0 --port 9000