from datetime import datetime

from pydantic import BaseModel, Field


class GenerationResponseModel(BaseModel):
    RegionCode: str = Field(examples=["FR"])
    DateStamp: datetime = Field(examples=["2023-04-15T14:30:00Z"])
    GenerationTypeId: int = Field(examples=[6])
    AggregatedGeneration: int = Field(examples=[3010])


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
