from typing import Mapping, Any
from datetime import datetime

from .rikstoto_abstract_stream import RikstotoAbstractStream
from airbyte_cdk.models import SyncMode

class RikstotoIndex(RikstotoAbstractStream):
    def racekeys_all(self):
        for raceday in self.current_racedays():
            for race in raceday["races"]:
                yield self.parse_race_key(race)

    def racekeys_today(self):
        for raceday in self.todays_racedays():
            for race in raceday["races"]:
                yield self.parse_race_key(race)

    def racedaykeys_today(self):
        for raceday in self.todays_racedays():
                yield raceday["raceDayKey"]

    def current_racedays(self):
        for racedays in self.read_records(sync_mode=SyncMode.full_refresh):
            for raceday in racedays["result"]:
                yield raceday

    def todays_racedays(self):
        for racedays in self.read_records(sync_mode=SyncMode.full_refresh):
            for raceday in racedays["result"]:
                race_date = datetime.strptime(raceday["startTime"], "%Y-%m-%dT%H:%M:%S").date()

                if race_date == datetime.today().date():
                    yield raceday

    def parse_race_key(self, race):
        return race["raceKey"].replace("#", "/")

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "raceDays"