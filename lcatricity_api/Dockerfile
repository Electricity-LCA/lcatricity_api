FROM python:3.12.3-slim-bullseye

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"


RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* \

# Prevent Python from writing pyc files to disc
ENV PYTHONDONTWRITEBYTECODE 1
# Prevent Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED 1

WORKDIR /usr/src/app


ADD requirements.txt /usr/src/app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# RUN python3 -m ensurepip --upgrade
COPY ./ /usr/src/app/lcatricity_api

ENV PYTHONPATH="/lcatricity_api"
