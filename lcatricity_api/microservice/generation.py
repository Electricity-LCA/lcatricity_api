import logging

import pandas as pd
from sqlalchemy.orm import sessionmaker

from lcatricity_api.microservice.constants import ServerError, ROW_LIMIT
from lcatricity_dataschema.base import Regions, ElectricityGeneration


async def get_electricity_generation_df(date_start, date_end, region_code: str, generation_type_id: int, engine) -> pd.DataFrame:
    if not isinstance(region_code, str):
        raise TypeError('Invalid region code. Region code must be a string')

    if not isinstance(generation_type_id, int):
        raise TypeError('Invalid generation type id. Generation type id must be an integer')


    session_obj = sessionmaker(bind=engine)
    with session_obj() as session:
        impacts_query = session.query(Regions.Id).where(Regions.Code == region_code).limit(1)

        # TODO: Consider replacing with a lookup in the common data cached in the main body
        region_id_df = pd.read_sql(impacts_query.statement, session.bind)
        if region_id_df is None or region_id_df.shape[0] == 0:
            raise ValueError(f'Region Code `{region_code}` could not be found in database')
        elif region_id_df.shape[0] > 1:
            raise ServerError(f'More than one region found for region code `{region_code}`. There is probably an error in the database')

        region_id = int(region_id_df.iat[0, 0])
        logging.debug(f'REGION IS {region_id}')

        query = (session.query(ElectricityGeneration)
                 .where(ElectricityGeneration.GenerationTypeId == generation_type_id)
                 .where(ElectricityGeneration.RegionId == region_id)
                 .limit(ROW_LIMIT))

        df = pd.read_sql(query.statement, session.bind)
    return df
