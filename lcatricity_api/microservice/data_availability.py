from datetime import datetime
from typing import Optional

import pandas as pd
import sqlalchemy
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from lcatricity_dataschema.base import ElectricityGeneration


async def get_regions_with_generation_data(engine, datestamp: Optional[str] = None) -> pd.DataFrame:
    """
    Get info on the available generation data per region. Returns a pandas dataframe with columns RegionId, EarliestTimeStamp, LatestTimeStamp,CountDataPoints

    I
    :param datestamp: Datestamp in the format yyyy-mm-dd. If None, then will return information on the earliest, last and count of data points for the region stored over all time in the database.
    :param engine:
    :return:
    """
    session_obj = sessionmaker(bind=engine)
    with session_obj() as session:
        if datestamp is None:
            regions_with_data_statement = session.query(ElectricityGeneration.RegionId,
                                                        func.min(ElectricityGeneration.DateStamp).label(
                                                            'EarliestTimeStamp'),
                                                        func.max(ElectricityGeneration.DateStamp).label(
                                                            'LatestTimeStamp'),
                                                        func.count(ElectricityGeneration.DateStamp).label(
                                                            'CountDataPoints')
                                                        ).group_by(ElectricityGeneration.RegionId)
        elif isinstance(datestamp, str):
            datestamp_interpreted = datetime.strptime(datestamp, '%Y-%m-%d')
            regions_with_data_statement = session.query(ElectricityGeneration.RegionId,
                                                        func.min(ElectricityGeneration.DateStamp).label(
                                                            'EarliestTimeStamp'),
                                                        func.max(ElectricityGeneration.DateStamp).label(
                                                            'LatestTimeStamp'),
                                                        func.count(ElectricityGeneration.DateStamp).label(
                                                            'CountDataPoints')
                                                        ).where(
                ElectricityGeneration.DateStamp.__eq__(datestamp_interpreted)
                ).group_by(ElectricityGeneration.RegionId)
        region_ids_df = pd.read_sql(regions_with_data_statement.statement, session.bind)
    return region_ids_df


async def get_datapoints_per_day(engine, region_code: Optional[str]) -> pd.DataFrame:
    """
    Get info on the count of generation datapoints per day per region. Returns a pandas dataframe with columns Datestamp (in the form YYYY-MM-DD), RegionId, CountDataPoints

    :param region_code: Optional[str]. A region code to filter on. If None, searches across all regions. Must be an short name in string value form, like `FR`
    :param engine:
    :return:
    """
    session_obj = sessionmaker(bind=engine)
    with session_obj() as session:
        if isinstance(region_code, str):
            assert len(region_code) < 10  # Force the region_code is less than 10 characters to minimzie risk of XSS/SQL injection
            region_df = pd.read_sql(sqlalchemy.text(f'SELECT "Id" FROM public."Regions" where "Code"=:code').bindparams(code=region_code), engine)
            if region_df.shape[0] == 0:
                raise ValueError(f'No regions found for region code {region_code}')
            elif region_df.shape[0] > 1:
                raise ValueError(f'More than one region found for region code {region_code}')
            region_id = region_df.iloc[0,0]
            region_id = int(region_id)
            print('Region id is :', region_id)

            data_per_day_query = session.query(
                func.to_char(ElectricityGeneration.DateStamp, "YYYY-MM-DD").label('Datestamp'),
                ElectricityGeneration.RegionId,
                func.count(ElectricityGeneration.DateStamp).label('CountDataPoints')
            ).where(ElectricityGeneration.RegionId == region_id).group_by(
                func.to_char(ElectricityGeneration.DateStamp, "YYYY-MM-DD"), ElectricityGeneration.RegionId)

        elif region_code is None:
            data_per_day_query = session.query(
                func.to_char(ElectricityGeneration.DateStamp, "YYYY-MM-DD").label('Datestamp'),
                ElectricityGeneration.RegionId,
                func.count(ElectricityGeneration.DateStamp).label('CountDataPoints')
                ).group_by(func.to_char(ElectricityGeneration.DateStamp, "YYYY-MM-DD"), ElectricityGeneration.RegionId)
        else:
            print('Error, the region code is not a string or None')
            return None
        data_per_day_df = pd.read_sql(data_per_day_query.statement, session.bind)
    return data_per_day_df
