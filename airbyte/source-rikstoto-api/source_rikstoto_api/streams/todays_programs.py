from typing import Mapping, Any, Optional, Iterable

from .raceday_containing_abstract_stream import RacedayContainingAbstractStream

class TodaysPrograms(RacedayContainingAbstractStream):
    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        key = stream_slice["key"]

        return f"game/program/{key}/VP/trot"

    # TODO: hent resultater for historiske løp i stedet for fra racedays oversikten
    def read_records(
        self,
        stream_slice: Optional[Mapping[str, Any]] = None,
        **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        for key in self.raceday_stream.racedaykeys_today():
            yield from super().read_records(stream_slice={"key": key}, **kwargs)