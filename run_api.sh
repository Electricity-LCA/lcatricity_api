#!/bin/bash

source .venv/bin/activate
uvicorn lcatricity_api.microservice.main:app --reload --host 0.0.0.0 --port 8000