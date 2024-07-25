from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import sqlalchemy
from sqlalchemy import func, desc
from sqlalchemy.orm import sessionmaker

from lcatricity_dataschema.base import ElectricityGeneration, Regions


async def get_regions_with_generation_data(engine, date_start: Optional[str] = None, date_end: Optional[str] = None,
                                           max_rows: Optional[int] = 100) -> pd.DataFrame:
    """
    Get info on the available generation data per region. Returns a pandas dataframe with columns RegionId, EarliestTimeStamp, LatestTimeStamp,CountDataPoints

    I
    :param engine: SQLalchemy Engine to use
    :param date_start: Start date in the format yyyy-mm-dd. If None, then will return information on the earliest, last and count of data points for the region stored over all time in the database.
    :param date_end:  End date of the period to search in the format yyyy-mm-dd. If None and date_start is a datestamp, then will be set to date_start+1day
    :param max_rows: Maximum number of rows to return. By default = 100. Maximum is 200 (which is more than the number of regions). If negative or <1 then will set to the default value of 1000

    :return:pd.DataFrame
    """

    assert isinstance(max_rows, int)
    if max_rows < 1:
        max_rows = 100
    if max_rows > 200:
        max_rows = 100

    if date_start is not None:
        try:
            date_start = datetime.strptime(date_start, '%Y-%m-%d')

        except (ValueError, TypeError):
            raise ValueError(
                f'Could not handle start or end date in the period `{date_start}`-`{date_end}`. Check your input is in the form yyyy-mm-dd')

        try:
            if date_end is None and isinstance(date_start, datetime):
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
        if date_start is None:
            regions_with_data_statement = session.query(
                Regions.Code.label('RegionCode'),
                func.min(ElectricityGeneration.DateStamp).label(
                    'EarliestTimeStamp'),
                func.max(ElectricityGeneration.DateStamp).label(
                    'LatestTimeStamp'),
                func.count(ElectricityGeneration.DateStamp).label(
                    'CountDataPoints')
            ).select_from(Regions).join(ElectricityGeneration, isouter=True
                                        ).group_by(Regions.Code
                                                   ).order_by(desc('CountDataPoints')).limit(max_rows)
        elif isinstance(date_start, datetime):
            regions_with_data_statement = session.query(
                Regions.Code.label("RegionCode"),

            ).select_from(Regions).join(ElectricityGeneration, isouter=True
                                        ).add_columns(func.min(ElectricityGeneration.DateStamp).label(
                'EarliestTimeStamp'),
                func.max(ElectricityGeneration.DateStamp).label(
                    'LatestTimeStamp'),
                func.count(ElectricityGeneration.DateStamp).label(
                    'CountDataPoints')
            ).where((
                            (ElectricityGeneration.DateStamp >= date_start) & (
                            ElectricityGeneration.DateStamp <= date_end)) | (ElectricityGeneration.DateStamp == None)
                    ).group_by(Regions.Code
                               ).order_by(desc('CountDataPoints')).limit(max_rows)
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
            assert len(
                region_code) < 10  # Force the region_code is less than 10 characters to minimzie risk of XSS/SQL injection
            region_df = pd.read_sql(
                sqlalchemy.text(f'SELECT "Id" FROM public."Regions" where "Code"=:code').bindparams(code=region_code),
                engine)
            if region_df.shape[0] == 0:
                raise ValueError(f'No regions found for region code {region_code}')
            elif region_df.shape[0] > 1:
                raise ValueError(f'More than one region found for region code {region_code}')
            region_id = region_df.iloc[0, 0]
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
