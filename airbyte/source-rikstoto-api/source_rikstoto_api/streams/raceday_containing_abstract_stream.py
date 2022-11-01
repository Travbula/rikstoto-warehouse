from .rikstoto_abstract_stream import RikstotoAbstractStream
from .rikstoto_index import RikstotoIndex

class RacedayContainingAbstractStream(RikstotoAbstractStream):
    def __init__(self, raceday_stream: RikstotoIndex, **kwargs):
        super().__init__(**kwargs)
        self.raceday_stream: RikstotoIndex = raceday_stream
