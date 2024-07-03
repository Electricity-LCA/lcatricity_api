class ServerError(Exception):
    pass


conversion_factors = {('MJ', 'kWh'): 3.6}  # {(FromUnit,ToUnit): ConversionFactor, ...}
ROW_LIMIT = 500
