from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class GenerationResponseModel(BaseModel):
    RegionCode: str = Field(examples=["FR"])
    DateStamp: datetime = Field(examples=["2023-04-15T14:30:00Z"])
    GenerationTypeId: int = Field(examples=[6])
    AggregatedGeneration: int = Field(examples=[3010])


class ImpactResultSchema(BaseModel):
    RegionCode: str
    DateStamp: datetime
    AggregatedGeneration: int
    GenerationUnit: str
    ElectricityGenerationTypeId: int
    ImpactCategoryId: int
    ImpactValue: int
    ImpactCategoryUnit: str
    PerUnit: str
    ConversionFactor: float
    AggregatedGenerationConverted: float
    EnvironmentalImpact: float
    model_config = {
        "json_schema_extra": {
            "examples": [{
                "RegionCode": "FR",
                "DateStamp": "2023-12-02T00:00:00.000",
                "AggregatedGeneration": 3010,
                "GenerationUnit": "MJ",
                "ElectricityGenerationTypeId": 6,
                "ImpactCategoryId": 1,
                "ImpactValue": 280,
                "ImpactCategoryUnit": "g CO2 eq.",
                "PerUnit": "kWh",
                "ConversionFactor": 3.6,
                "AggregatedGenerationConverted": 10836,
                "EnvironmentalImpact": 3034080
            }]
        }
    }


class DataAvailabilityResponse(BaseModel):
    RegionId: int
    EarliestTimeStamp: Optional[datetime]
    LatestTimeStamp: Optional[datetime]
    CountDataPoints: int
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "RegionCode": "GB_IFA",
                    "EarliestTimeStamp": None,
                    "LatestTimeStamp": None,
                    "CountDataPoints": 0
                },
                {
                    "RegionCode": "FR",
                    "EarliestTimeStamp": "2023-12-02T00:00:00.000",
                    "LatestTimeStamp": "2023-12-02T23:00:00.000",
                    "CountDataPoints": 216
                },]
        }
    }