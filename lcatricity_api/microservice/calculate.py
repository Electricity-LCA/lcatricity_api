import logging
import pandas as pd
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker

from lcatricity_api.microservice.constants import conversion_factors
from lcatricity_api.microservice.generation import get_electricity_generation_df
from lcatricity_api.orm.base import EnvironmentalImpacts


class ImpactResultSchema(BaseModel):
    GenerationUnit: str
    PerUnit: str
    ConversionFactor: float
    AggregatedGenerationConverted: float
    EnvironmentalImpact: float
    model_config = {
        "json_schema_extra": {
            "examples": [{
                "GenerationUnit": "MJ",
                "PerUnit": "kWh",
                "ConversionFactor":3.6,
                "AggregatedGenerationConverted":2,
                "EnvironmentalImpact":3
            }
            ]
        }
    }


async def calculate_impact_df(date_start, region_code: str, generation_type_id: int, engine):
    logging.debug(
        f'Getting electricity generation data for date {date_start}, region code {region_code}, generation type id {generation_type_id}')
    generation_df = await get_electricity_generation_df(date_start, None, region_code, generation_type_id,
                                                        engine=engine)
    logging.debug('Retrieved generation data')
    environmental_impacts_df = await get_calculation_data(engine=engine)

    # Annotate generation data with units # TODO: Move to DB
    if 'GenerationUnit' not in generation_df.columns.to_list():
        generation_df['GenerationUnit'] = 'MJ'
    generation_df.drop(['Id'], axis=1, inplace=True)

    # Remove unnecessary columns for calculation
    environmental_impacts_df.drop(['ReferenceYear'], axis=1, inplace=True)
    environmental_impacts_df.drop(['Id'], axis=1, inplace=True)

    # Join dataframes
    calculation_df = generation_df.merge(environmental_impacts_df,
                       left_on='GenerationTypeId',
                       right_on='ElectricityGenerationTypeId')

    # TODO: Move to database
    # Convert units
    calculation_df['ConversionFactor'] = calculation_df[['GenerationUnit','PerUnit']].apply(lambda x: conversion_factors[(x['GenerationUnit'],x['PerUnit'])],axis=1)
    calculation_df['AggregatedGenerationConverted'] = calculation_df['AggregatedGeneration'] * calculation_df['ConversionFactor']
    calculation_df['EnvironmentalImpact'] = calculation_df['AggregatedGenerationConverted']*calculation_df['ImpactValue']

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
