from sqlalchemy.orm import declarative_base
import sqlalchemy as sqla

sql_alchemy_base = declarative_base()


class ImpactCategories(sql_alchemy_base):
    __tablename__ = 'ImpactCategories'
    Id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=True)
    Name = sqla.Column(sqla.String(250), nullable=False)
    Unit = sqla.Column(sqla.String(50), nullable=False)


class ElectricityGenerationTypes(sql_alchemy_base):
    __tablename__ = 'ElectricityGenerationTypes'
    Id = sqla.Column(sqla.Integer,primary_key=True, autoincrement=True, nullable=True)
    Name = sqla.Column(sqla.String(50))

class ElectricityGenerationTypesMapping(sql_alchemy_base):
    __tablename__ = 'ElectricityGenerationTypesMapping'
    Id = sqla.Column(sqla.Integer,primary_key=True, autoincrement=True, nullable=True)
    DataSourceName = sqla.Column(sqla.String(50))
    ElectricityGenerationTypeId = sqla.Column(sqla.Integer)
    ExternalName = sqla.Column(sqla.String(100))
    Comment = sqla.Column(sqla.String(500))
    __table_args = (
        sqla.ForeignKeyConstraint(['ElectricityGenerationTypeId'], ['ElectricityGenerationTypes.Id']),
        sqla.UniqueConstraint('DataSourceName', 'ExternalName', name='UX_ElectricityGenerationTypesMapping_Import'),
    )


class EnvironmentalImpacts(sql_alchemy_base):
    __tablename__ = 'EnvironmentalImpacts'
    Id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=True)
    ElectricityGenerationTypeId = sqla.Column(sqla.Integer)
    ImpactCategoryId = sqla.Column(sqla.Integer)
    ImpactValue = sqla.Column(sqla.Float)
    ImpactCategoryUnit = sqla.Column(sqla.String(20))
    PerUnit = sqla.Column(sqla.String(10))
    ReferenceYear = sqla.Column(sqla.DateTime(), default=2021, nullable=False)
    __table_args__ = (
        sqla.ForeignKeyConstraint(['ElectricityGenerationTypeId'], ['ElectricityGenerationTypes.Id']),
        sqla.ForeignKeyConstraint(['ImpactCategoryId'], ['ImpactCategories.Id']),
        sqla.UniqueConstraint('ElectricityGenerationTypeId', 'ImpactCategoryId', name='UX_EnvironmentalImpacts'),
    )


class Regions(sql_alchemy_base):
    __tablename__ = 'Regions'
    Id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=True)
    Code = sqla.Column(sqla.VARCHAR(36))
    Type = sqla.Column(sqla.VARCHAR(36))  # Currently `Country`
    Description = sqla.Column(sqla.VARCHAR(150))
    __table_args__ = (
        sqla.UniqueConstraint('Code', name='UX_Regions'),
    )


class ElectricityGeneration(sql_alchemy_base):
    __tablename__ = 'ElectricityGeneration'
    Id = sqla.Column(sqla.Integer,primary_key=True, autoincrement=True, nullable=True)
    RegionId = sqla.Column(sqla.Integer)
    DateStamp = sqla.Column(sqla.DateTime)
    GenerationTypeId = sqla.Column(sqla.Integer)
    AggregatedGeneration = sqla.Column(sqla.Float)
    __table_args__ = (
        sqla.ForeignKeyConstraint(['RegionId'], ['Regions.Id']),
        sqla.ForeignKeyConstraint(['GenerationTypeId'], ['ElectricityGenerationTypes.Id']),
        sqla.UniqueConstraint('RegionId', 'GenerationTypeId', 'DateStamp', name='UX_ElectricityGeneration'),
    )

