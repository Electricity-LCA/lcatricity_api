from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from lcatricity_api.orm.base import ElectricityGeneration


async def get_regions_with_generation_data(engine, datestamp:Optional[str]=None) -> pd.DataFrame:
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
                                                        func.min(ElectricityGeneration.DateStamp).label('EarliestTimeStamp'),
                                                        func.max(ElectricityGeneration.DateStamp).label('LatestTimeStamp'),
                                                        func.count(ElectricityGeneration.DateStamp).label('CountDataPoints')
                                                        ).group_by(ElectricityGeneration.RegionId)
        elif isinstance(datestamp, str):
            datestamp_interpreted = datetime.strptime(datestamp,'%Y-%m-%d')
            regions_with_data_statement = session.query(ElectricityGeneration.RegionId,
                                                        func.min(ElectricityGeneration.DateStamp).label('EarliestTimeStamp'),
                                                        func.max(ElectricityGeneration.DateStamp).label('LatestTimeStamp'),
                                                        func.count(ElectricityGeneration.DateStamp).label('CountDataPoints')
                                                        ).where(ElectricityGeneration.DateStamp.__eq__(datestamp_interpreted)
                                                                ).group_by(ElectricityGeneration.RegionId)
        region_ids_df = pd.read_sql(regions_with_data_statement.statement, session.bind)
    return region_ids_df