import logging
import os
from typing import Any

import pandas as pd
import sqlalchemy as sqla
from dotenv import load_dotenv
from fastapi import FastAPI, Response
from fastapi.openapi.docs import get_swagger_ui_html
import uvicorn
from starlette import status
from starlette.responses import RedirectResponse

from lcatricity_api.data.get_common_data import load_common_data_from_db
from lcatricity_api.microservice.calculate import ImpactResultSchema, calculate_impact_df
from lcatricity_api.microservice.constants import ServerError
from lcatricity_api.microservice.generation import get_electricity_generation_df

load_dotenv()
HOST = os.getenv('ELEC_LCA_DB_HOST')
DB_NAME = os.getenv('ELEC_LCA_DB_NAME')
USER = os.getenv('ELEC_LCA_DB_LOGIN')
PASSWORD = os.getenv('ELEC_LCA_DB_PWD')
DB_PORT = os.getenv('ELEC_LCA_DB_PORT')
API_VERSION = os.getenv('ELEC_LCA_API_VERSION')

# Connect to postgres database
engine = sqla.create_engine(sqla.engine.url.URL.create(
    drivername='postgresql',
    host=HOST,
    database=DB_NAME,
    username=USER,
    password=PASSWORD
))
cache = load_common_data_from_db(sql_engine=engine)

app = FastAPI(title="LCAtricity API", description="Assess environmental impacts of electricity generation on multiple dimensions.", version=API_VERSION)


@app.get('/')
def get_main():
    """Redirect the root url to the documentation page"""
    return RedirectResponse('/docs',status_code=status.HTTP_302_FOUND)


@app.get('/docs')
def get_docs():
    """Get documentation"""
    return get_swagger_ui_html(title='API documentation for elc_lca')


@app.get('/list_regions')
async def list_regions():
    """
    List the Electricity regions available for calculation. Note that this is a mixture of coutries and electricity regions (which may be sub-national or across multiple countries).
    Please see the ENTSO-E region documentation for notes on certain regions

    :return:
    JSON
    """
    regions_df = cache.regions
    return Response(regions_df.to_json(orient='records'), media_type="application/json")


@app.get('/list_generation_types')
async def list_generation_types():
    """
    List the types of electricity generation, including the names and codes needed for running calculation queries

    :return:
    JSON
    """
    generation_types_df = cache.generation_types
    return Response(generation_types_df.to_json(orient='records'), media_type="application/json")


@app.get('/list_generation_type_mappings')
async def list_generation_type_mappings():
    """
    Lists the original names for different electricity generation types from the different data sources. This is provided for transparency and fact-checking.
    Original data sources include ENTSO-E and the UNECE report “Life Cycle Assessment of Electricity Generation Options | UNECE.” Accessed December 5, 2023. https://unece.org/sed/documents/2021/10/reports/life-cycle-assessment-electricity-generation-options.

    :return:
    JSON
    """
    generation_type_mappings_df = cache.generation_type_mappings
    return Response(generation_type_mappings_df.to_json(orient='records'), media_type="application/json")


@app.get('/generation')
async def get_electricity_generation(date_start, region_code: str, generation_type_id: int):
    """
    Get the electricity generation on a given date (e.g. 2024-02-01) for a given region (e.g. NL or FR) and an
    electricity regions type (e.g. 4 for fossil gas)

    :return:
    JSON
    """
    try:
        df = await get_electricity_generation_df(date_start, None, region_code, generation_type_id, engine=engine)
    except TypeError as e:
        return Response(status_code=400, content=str(e))
    except ValueError as e:
        return Response(status_code=422, content=str(e))
    except ServerError as e:
        return Response(status_code=500, content=str(e))
    if not isinstance(df, pd.DataFrame):
        return Response(status_code=500)
    return Response(df.to_json(orient='records'), media_type="application/json")


@app.get('/calculate', response_model=ImpactResultSchema)
async def calculate_impact(date_start, region_code: str, generation_type_id: int) -> Any:
    """
    Get environmental impacts of electricity generation on given date (e.g. 2024-02-01) for a given region (e.g. NL or FR) and an electricity regions type (e.g. 4 for fossil gas)

    :return
    ImpactResultSchema
    """

    try:
        impact_df = await calculate_impact_df(date_start, region_code, generation_type_id, engine=engine)
    except TypeError as e:
        return Response(status_code=400, content=str(e))
    except ValueError as e:
        return Response(status_code=422, content=str(e))
    except ServerError as e:
        return Response(status_code=500, content=str(e))
    if not isinstance(impact_df, pd.DataFrame):
        return Response(status_code=500)
    return Response(impact_df.to_json(orient='records'), media_type='text/json')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='api.log')
    uvicorn.run(app, port=80)
