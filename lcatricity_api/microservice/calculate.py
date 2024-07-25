import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy.orm import sessionmaker

from lcatricity_api.microservice.constants import conversion_factors, NoDataAvailableError
from lcatricity_api.microservice.generation import get_electricity_generation_df
from lcatricity_dataschema.base import EnvironmentalImpacts


async def calculate_impact_df(date_start: str, date_end: str, region_code: str, impact_category_id: int, engine, max_rows: Optional[int] = 1000):
    logging.debug(
        f'Getting electricity generation data for date {date_start}, region code {region_code}, impact category id {impact_category_id}')
    try:
        datetime_start = datetime.strptime(date_start, '%Y-%m-%d')
        datetime_end = datetime.strptime(date_end, '%Y-%m-%d')
    except (ValueError, TypeError):
        raise ValueError(
            f'Could not handle start or end date in the period `{date_start}`-`{date_end}`. Check your input is in the form yyyy-mm-dd')

    generation_df = await get_electricity_generation_df(date_start, region_code, engine=engine, generation_type_id=None,
                                                        date_end=date_end)
    if generation_df.empty:
        raise NoDataAvailableError(f"No data available for region '{region_code}' in the period '{datetime_start}' - '{datetime_end}'")
    logging.debug('Retrieved generation data')
    environmental_impacts_df = await get_calculation_data(engine=engine)


    # Annotate generation data with units # TODO: Move to DB
    if 'GenerationUnit' not in generation_df.columns.to_list():
        generation_df['GenerationUnit'] = 'MJ'

    # Remove unnecessary columns for calculation
    environmental_impacts_df.drop(['ReferenceYear'], axis=1, inplace=True)
    environmental_impacts_df.drop(['Id'], axis=1, inplace=True)

    # Join dataframes
    calculation_df = generation_df.merge(environmental_impacts_df,
                       left_on='GenerationTypeId',
                       right_on='ElectricityGenerationTypeId')

    calculation_df.drop(["GenerationTypeId"],axis=1,inplace=True)
    # Convert units
    calculation_df['ConversionFactor'] = calculation_df[['GenerationUnit','PerUnit']].apply(lambda x: conversion_factors[(x['GenerationUnit'],x['PerUnit'])],axis=1)
    calculation_df['AggregatedGenerationConverted'] = calculation_df['AggregatedGeneration'] * calculation_df['ConversionFactor']
    calculation_df['EnvironmentalImpact'] = calculation_df['AggregatedGenerationConverted']*calculation_df['ImpactValue']

    # TODO: Move to database
    return calculation_df




async def get_calculation_data(engine) -> pd.DataFrame:
    session_obj = sessionmaker(bind=engine)
    with session_obj() as session:
        impacts_query = session.query(EnvironmentalImpacts)
        impacts_df = pd.read_sql(impacts_query.statement, session.bind)
        # if isinstance(EnvironmentalImpactsSchema.validate(impacts_df), (SchemaError, SchemaErrors)):
        #     logging.error('Schema error in environmental impacts data returned from database')
        #     raise ServerError('Schema error in environmental impacts data returned from database')
    return impacts_df
