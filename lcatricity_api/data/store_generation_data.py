import logging
import time

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from lcatricity_api.orm.base import sql_alchemy_base


def store_generation_data_to_db(
        generation_mw: pd.Series,
        generation_type_id: int,
        region_id: int,
        sql_engine: sqlalchemy.Engine) -> bool:
    """
    Store electricity time series power generation data to elec_lca database. Uses an upsert approach, removing any
        existing generation data for the same interval and region id (as it is likely outdated or null, as new data is generated continuously).

    @param generation_mw: Pandas Series with the generation data (MW per time interval) to insert. The index should be the timestamps of the beginning of each interval
    @param generation_type_id: Internal generation type id (int)
    @param region_id: Internal region id (int)
    @param sql_engine: SQL engine to the elec_lca database
    @return:
    """
    s_1 = time.time()

    # Validate the Series provided:
    try:
        assert isinstance(generation_mw.index[0], pd.Timestamp)
        assert isinstance(generation_mw.values[0,0], float)
    except AssertionError as e:
        raise e
    start = generation_mw.index.min()
    end = generation_mw.index.max()

    # Create dataframe of values to insert
    values_to_insert = pd.DataFrame({'RegionId': region_id,
                                     'DateStamp': generation_mw.index.to_list(),
                                     'GenerationTypeId': generation_type_id,
                                     'AggregatedGeneration': generation_mw['Actual Aggregated'].values}
                                    )
    logging.info(values_to_insert)
    session_obj = sessionmaker(bind=sql_engine)
    with session_obj() as session:
        elec_gen_table = sqlalchemy.Table('ElectricityGeneration', sql_alchemy_base.metadata, autoload=True)
        delete_query = elec_gen_table.delete().where(
            (elec_gen_table.c.DateStamp >= start) &
            (elec_gen_table.c.DateStamp <= end) &
            (elec_gen_table.c.RegionId == int(region_id)) &
            (elec_gen_table.c.GenerationTypeId == int(generation_type_id))
        )
        deletion_result = session.execute(delete_query)
        session.commit()
        logging.info(f'{deletion_result.rowcount} rows deleted, that already existed for region={region_id}, date range=`{start}`-`{end}` and generation type={generation_type_id}')

    time.sleep(1)
    count_rows = values_to_insert.to_sql('ElectricityGeneration', sql_engine, if_exists='append', index=False)
    logging.info(f'Inserted {count_rows} values for region with id = `{region_id}` to database')
    e = time.time()
    logging.info(f'{e - s_1:.2f} s to write to database')
    return True
