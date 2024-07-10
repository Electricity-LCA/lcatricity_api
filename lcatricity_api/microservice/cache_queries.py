import pandas as pd

from lcatricity_api.data.get_common_data import load_common_data_from_db


cache = None

def init_cache(engine):
    global cache
    cache = load_common_data_from_db(sql_engine=engine)

async def list_regions_in_cache() -> pd.DataFrame:
    """
    List the Electricity regions available for calculation. Note that this is a mixture of coutries and electricity regions (which may be sub-national or across multiple countries).
    Please see the ENTSO-E region documentation for notes on certain regions

    :return:
    DF
    """
    return cache.regions


async def list_generation_types_in_cache() -> pd.DataFrame:
    """
    List the types of electricity generation, including the names and codes needed for running calculation queries

    :return:
    JSON
    """
    return cache.generation_types


async def list_generation_type_mappings_in_cache() -> pd.DataFrame:
    """
    Lists the original names for different electricity generation types from the different data sources. This is provided for transparency and fact-checking.
    Original data sources include ENTSO-E and the UNECE report “Life Cycle Assessment of Electricity Generation Options | UNECE.” Accessed December 5, 2023. https://unece.org/sed/documents/2021/10/reports/life-cycle-assessment-electricity-generation-options.

    :return:
    JSON
    """
    return cache.generation_type_mappings


async def list_impact_categories_df_in_cache() -> pd.DataFrame:
    """
    List the environmental impacts that can be calculated via the API

    :return:
    JSON
    """

    return cache.impact_categories
