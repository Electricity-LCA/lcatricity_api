FROM python:3.12.3-slim-bullseye

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# RUN python3 -m ensurepip --upgrade
WORKDIR /lcatricity_api/
ADD lcatricity_api/ ./src
ADD .env .
# For microservice background
EXPOSE 8000
# For streamlit dashboard
EXPOSE 8900
# For database
EXPOSE 25060
ENV PYTHONPATH="/lcatricity_api"
CMD uvicorn src.microservice.main:app --reload --host 0.0.0.0 --port 8000 --workers=3 &
CMD streamlit run src/visualization/visualize.py --server.port=8900 --server.address=0.0.0.0
