from typing import Mapping, Any
from datetime import datetime

from .rikstoto_abstract_stream import RikstotoAbstractStream
from airbyte_cdk.models import SyncMode

class RikstotoIndexList(RikstotoAbstractStream):
    def __init__(self, start_date, end_date, **kwargs):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(**kwargs)

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None) -> str:
        """results/racedays/2022-05-09/2022-05-14/list"""
        #start_date = stream_slice["start_date"]
        #end_date = stream_slice["end_date"]

        return f"results/racedays/{self.start_date}/{self.end_date}/list"

    def racekeys_all(self):
        for raceday in self.current_racedays():
            raceday_key = raceday["raceDay"]
            for race in raceday["races"]:
                yield self.parse_race_key(raceday_key, race)

    def current_racedays(self):
        for racedays_list in self.read_records(sync_mode=SyncMode.full_refresh):
            for racedays_object in racedays_list["result"]:
                # yield from raceday["raceDays"] ?
                for raceday in racedays_object["raceDays"]:
                    if raceday["isDomestic"]: # or raceday["isATG"]:
                        yield raceday

    def parse_race_key(self, raceday_key, race):
        return f"{raceday_key}/{race['raceNumber']}"