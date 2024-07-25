import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional, List

import pandas as pd
import sqlalchemy as sqla
from dotenv import load_dotenv
from fastapi import FastAPI, Response
from fastapi.openapi.docs import get_swagger_ui_html
import uvicorn
from starlette import status
from starlette.responses import RedirectResponse

from lcatricity_api.microservice.ResponseModels import GenerationResponseModel, ImpactResultSchema, \
    DataAvailabilityResponse
from lcatricity_api.microservice.cache_queries import list_regions_in_cache, list_generation_types_in_cache, \
    list_generation_type_mappings_in_cache, list_impact_categories_df_in_cache, init_cache
from lcatricity_api.microservice.calculate import calculate_impact_df
from lcatricity_api.microservice.constants import ServerError, NoDataAvailableError
from lcatricity_api.microservice.data_availability import get_regions_with_generation_data, get_datapoints_per_day
from lcatricity_api.microservice.generation import get_electricity_generation_df

load_dotenv()
HOST = os.getenv('ELEC_LCA_DB_HOST')
DB_NAME = os.getenv('ELEC_LCA_DB_NAME')
USER = os.getenv('ELEC_LCA_DB_LOGIN')
PASSWORD = os.getenv('ELEC_LCA_DB_PWD')
DB_PORT = os.getenv('ELEC_LCA_DB_PORT')
API_PORT = os.getenv('ELEC_LCA_API_PORT')

try:
    API_PORT = int(API_PORT)
except TypeError as e:
    raise TypeError(f"API_PORT set in .env is invalid. Value should be an integer. Current value {API_PORT}")
except Exception as e:
    raise Exception("Other issue with API_PORT in .env")

API_VERSION = os.getenv('ELEC_LCA_API_VERSION')

# Connect to postgres database
engine = sqla.create_engine(sqla.engine.url.URL.create(
    drivername='postgresql',
    host=HOST,
    database=DB_NAME,
    username=USER,
    password=PASSWORD,
    port=DB_PORT
))
init_cache(engine)

app = FastAPI(title="LCAtricity API",
              description="Assess environmental impacts of electricity generation on multiple dimensions.",
              version=API_VERSION)


@app.get('/')
def get_main():
    """Redirect the root url to the documentation page"""
    return RedirectResponse('/docs', status_code=status.HTTP_302_FOUND)


@app.get('/docs')
def get_docs():
    """Get documentation"""
    return get_swagger_ui_html(title='API documentation for LCAtricity')


@app.get('/list_regions')
async def list_regions():
    """
    List the Electricity regions available for calculation. Note that this is a mixture of coutries and electricity regions (which may be sub-national or across multiple countries).
    Please see the ENTSO-E region documentation for notes on certain regions

    :return:
    JSON
    """
    regions_df = await list_regions_in_cache()
    return Response(regions_df.to_json(orient='records'), media_type="application/json")


@app.get('/list_generation_types')
async def list_generation_types():
    """
    List the types of electricity generation, including the names and codes needed for running calculation queries

    :return:
    JSON
    """
    generation_types_df = await list_generation_types_in_cache()
    return Response(generation_types_df.to_json(orient='records'), media_type="application/json")


@app.get('/list_generation_type_mappings')
async def list_generation_type_mappings():
    """
    Lists the original names for different electricity generation types from the different data sources. This is provided for transparency and fact-checking.
    Original data sources include ENTSO-E and the UNECE report “Life Cycle Assessment of Electricity Generation Options | UNECE.” Accessed December 5, 2023. https://unece.org/sed/documents/2021/10/reports/life-cycle-assessment-electricity-generation-options.

    :return:
    JSON
    """
    generation_type_mappings_df = await list_generation_type_mappings_in_cache()
    return Response(generation_type_mappings_df.to_json(orient='records'), media_type="application/json")


@app.get("/available_data_region",response_model=DataAvailabilityResponse)
async def availability_regions(date_start: Optional[str] = None, date_end: Optional[str] = None, max_rows: Optional[int] = 1000):
    """
        Get info on the available generation data per region. Returns a JSON with keys RegionId, EarliestTimeStamp, LatestTimeStamp,CountDataPoints

        Note that in several regions the electricity grid operator does not provide electricity data via the ENTSO-E
        Transparency Platform to our knowledge, or not within the region specified. In this case, the region is
        listed with null start and end datestamp, and data point count = 0

        :param date_start: Start date in the format yyyy-mm-dd. If None, then will return information on the earliest, last and count of data points for the region stored over all time in the database.
        :param date_end:  End date of the period to search in the format yyyy-mm-dd. If None and date_start is a datestamp, then will be set to date_start+1day
        :param max_rows: Maximum number of rows to return. By default = 1000. Maximum is 100000

        :return:
        """

    data_availability_df = await get_regions_with_generation_data(engine, date_start=date_start, date_end=date_end, max_rows=1000)
    return Response(data_availability_df.to_json(orient='records',date_format='iso'), media_type="application/json")


@app.get("/datapoints_count_by_day")
async def datapoints_count_by_day(region_code: Optional[str]):
    """
        Get info on the count of generation datapoints per day per region. Returns JSON with keys Datestamp (in the form YYYY-MM-DD), RegionId, CountDataPoints

        :param region_code: Optional[str]. A region code to filter on. If None, searches across all regions. Must be an short name in string value form, like `FR`

        :return:
        """

    datapoint_counts_df = await get_datapoints_per_day(engine, region_code=region_code)
    return Response(datapoint_counts_df.to_json(orient='records',date_format='iso'), media_type="application/json")


@app.get('/list_impact_categories')
async def list_impact_categories():
    """
    List the environmental impacts that can be calculated via the API

    WARNING: This operation may take some time to return

    :return:
    JSON
    """

    impact_categories_df = await list_impact_categories_df_in_cache()
    return Response(impact_categories_df.to_json(orient='records'), media_type="application/json")


@app.get('/generation', response_model=List[GenerationResponseModel])
async def get_electricity_generation(date_start: str, region_code: str, date_end: Optional[str] = None,
                                     generation_type_id: Optional[int] = None):
    """
    Get the electricity generation on a given time period (e.g. 2024-02-01 to 2024-02-02) for a given region (e.g. NL or FR) and optionally an
    electricity regions type (e.g. 4 for fossil gas)

    :return:
    JSON
    """
    try:
        df = await get_electricity_generation_df(date_start, region_code=region_code, engine=engine,
                                                 generation_type_id=generation_type_id, date_end=date_end)
    except TypeError as e:
        return Response(status_code=400, content=str(e))
    except ValueError as e:
        return Response(status_code=422, content=str(e))
    except ServerError as e:
        return Response(status_code=500, content=str(e))
    if not isinstance(df, pd.DataFrame):
        return Response(status_code=500)
    return Response(df.to_json(orient='records',date_format='iso'), media_type="application/json")


@app.get('/calculate', response_model=List[ImpactResultSchema])
async def calculate_impact(date_start: str, region_code: str, impact_category_id: int, date_end: str = None) -> Any:
    """
    Get environmental impacts of electricity generation on given date (e.g. 2024-02-01) for a given region (e.g. NL or FR) and an electricity regions type (e.g. 4 for fossil gas)

    By default date_end will be date_start + 1 day if left None

    :return
    ImpactResultSchema
    """
    assert isinstance(date_start, str)
    if date_end is None:
        start_datetime = datetime.strptime(date_start, '%Y-%m-%d')
        end_datetime = start_datetime + timedelta(days=1)
        date_end = end_datetime.strftime('%Y-%m-%d')
    try:
        impact_df = await calculate_impact_df(date_start, date_end, region_code, impact_category_id=impact_category_id,
                                              engine=engine)
    except NoDataAvailableError as exc:
        return Response(status_code=400, content=json.dumps({'response': 400, 'error_info': exc.message}), media_type='text/json')
    except TypeError as e:
        return Response(status_code=400, content=str(e))
    except ValueError as e:
        return Response(status_code=422, content=str(e))
    except ServerError as e:
        return Response(status_code=500, content=str(e))
    if not isinstance(impact_df, pd.DataFrame):
        return Response(status_code=500)
    return Response(impact_df.to_json(orient='records', date_format='iso'), media_type='text/json')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, filename='api.log')
    uvicorn.run(app, port=API_PORT)
