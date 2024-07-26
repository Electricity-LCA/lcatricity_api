class ServerError(Exception):
    pass


conversion_factors = {('MJ', 'kWh'): 3.6}  # {(FromUnit,ToUnit): ConversionFactor, ...}


class NoDataAvailableError(Exception):
    def __init__(self, message: str, *args):
        super().__init__(args)
        self.message = message

    pass
