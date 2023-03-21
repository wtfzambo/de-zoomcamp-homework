from typing import Optional

import faust


class Ride(faust.Record, validation=True):
    PULocationID: Optional[str]


class RideCount(faust.Record, serializer='json'):
    # NOTE: the "__faust" key added by faust is called the "blessed key"
    count: int
