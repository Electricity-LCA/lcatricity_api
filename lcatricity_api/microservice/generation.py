import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from sqlalchemy import literal
from sqlalchemy.orm import sessionmaker

from lcatricity_api.microservice.constants import ServerError, ROW_LIMIT, NoDataAvailableError
from lcatricity_dataschema.base import Regions, ElectricityGeneration


async def get_electricity_generation_all_generationtypes_df(date_start, date_end, region_code: str,
                                                            engine) -> pd.DataFrame:
    if not isinstance(region_code, str):
        raise TypeError('Invalid region code. Region code must be a string')
    session_obj = sessionmaker(bind=engine)
    with session_obj() as session:
        impacts_query = session.query(Regions.Id).where(Regions.Code == region_code).limit(1)

        region_id_df = pd.read_sql(impacts_query.statement, session.bind)
        if region_id_df is None or region_id_df.shape[0] == 0:
            raise ValueError(f'Region Code `{region_code}` could not be found in database')
        elif region_id_df.shape[0] > 1:
            raise ServerError(
                f'More than one region found for region code `{region_code}`. There is probably an error in the database')

        region_id = int(region_id_df.iat[0, 0])
        logging.debug(f'REGION IS {region_id}')

        query = (session.query(ElectricityGeneration)
                 .where(ElectricityGeneration.RegionId == region_id)
                 .limit(ROW_LIMIT))

        df = pd.read_sql(query.statement, session.bind)
    return df


async def get_electricity_generation_df(date_start: str, region_code: str, engine,
                                        generation_type_id: Optional[int] = None, date_end: str = None) -> pd.DataFrame:
    """Get electricity generation on a given day"""
    if not isinstance(region_code, str):
        raise TypeError('Invalid region code. Region code must be a string')

    if not isinstance(generation_type_id, int) and generation_type_id is not None:
        raise TypeError('Invalid generation type id. Generation type id must be an integer or None')

    try:
        date_start = datetime.strptime(date_start, '%Y-%m-%d')

    except (ValueError, TypeError):
        raise ValueError(
            f'Could not handle start or end date in the period `{date_start}`-`{date_end}`. Check your input is in the form yyyy-mm-dd')

    try:
        if date_end is None:
            date_end = date_start + timedelta(days=1)
        elif isinstance(date_end, str):
            date_end = datetime.strptime(date_end, '%Y-%m-%d')
        else:
            raise ValueError('date_end was an unexpected type - it should be a string or None')
    except (ValueError, TypeError):
        raise ValueError(
            f'Could not handle start or end date in the period `{date_start}`-`{date_end}`. Check your input is in the form yyyy-mm-dd')

    session_obj = sessionmaker(bind=engine)
    with session_obj() as session:
        impacts_query = session.query(Regions.Id).where(Regions.Code == region_code).limit(1)

        # TODO: Consider replacing with a lookup in the common data cached in the main body
        region_id_df = pd.read_sql(impacts_query.statement, session.bind)
        if region_id_df is None or region_id_df.shape[0] == 0:
            raise ValueError(f'Region Code `{region_code}` could not be found in database')
        elif region_id_df.shape[0] > 1:
            raise ServerError(
                f'More than one region found for region code `{region_code}`. There is probably an error in the database')

        region_id = int(region_id_df.iat[0, 0])
        logging.debug(f'REGION IS {region_id}')

        if generation_type_id:
            query = (session.query(ElectricityGeneration)
                     .with_entities(literal(region_code).label('RegionCode'),
                                    ElectricityGeneration.DateStamp,
                                   ElectricityGeneration.GenerationTypeId, ElectricityGeneration.AggregatedGeneration)
                     .where(ElectricityGeneration.GenerationTypeId == generation_type_id)
                     .where(ElectricityGeneration.RegionId == region_id)
                     .where((ElectricityGeneration.DateStamp >= date_start)
                            & (ElectricityGeneration.DateStamp <= date_end))
                     .limit(ROW_LIMIT))
        else:
            query = (session.query(ElectricityGeneration)
                     .with_entities(literal(region_code).label('RegionCode'),
                                    ElectricityGeneration.DateStamp,
                                    ElectricityGeneration.GenerationTypeId, ElectricityGeneration.AggregatedGeneration)
                     .where(ElectricityGeneration.RegionId == region_id)
                     .where((ElectricityGeneration.DateStamp >= date_start)
                            & (ElectricityGeneration.DateStamp <= date_end))
                     .limit(ROW_LIMIT))
        df = pd.read_sql(query.statement, session.bind)
    return df
