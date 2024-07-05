# Test that can retrieve definitions of the entities in the database
# Namely, electricity generation types, regions and generation_type_mappings
import os

import httpx
from dotenv import load_dotenv


def test_get_regions():
    EXPECTED_NUMBER_REGIONS = 99 # THis is not expected to change frequently
    load_dotenv()
    API_BASE_URL = os.getenv('ELEC_LCA_API_URL')
    ELEC_LCA_API_PORT = os.getenv('ELEC_LCA_API_PORT')

    response = httpx.get(url=f'{API_BASE_URL}:{ELEC_LCA_API_PORT}/list_regions')
    assert 200 <= response.status_code < 300
    try:
        response_json = response.json()
        assert len(response_json)
    except Exception as e:
        raise e

    assert len(response_json) == EXPECTED_NUMBER_REGIONS
