from typing import List, Mapping, Any, Optional, Iterable

from .rikstoto_abstract_stream import RikstotoAbstractStream

class TotalInvestment(RikstotoAbstractStream):
    def __init__(self, racedays, **kwargs):
        super().__init__(**kwargs)
        self.racedays = racedays

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        key = stream_slice["key"].split("/")[0]
        race = stream_slice["key"].split("/")[1]

        return f"results/raceDays/{key}-{race}/totalInvestment"

    def read_records(
        self,
        stream_slice: Optional[Mapping[str, Any]] = None,
        **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        for raceday in self.racedays:
            for key in raceday.first_racekeys():
                self.raceday_key = key
                yield from super().read_records(stream_slice={"key": key}, **kwargs)
