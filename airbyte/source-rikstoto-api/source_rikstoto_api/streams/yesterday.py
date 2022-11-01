from typing import Mapping, Any
import datetime

from .rikstoto_abstract_stream import RikstotoAbstractStream
from airbyte_cdk.models import SyncMode

class Yesterday(RikstotoAbstractStream):
    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=1)
        end_date = start_date

        return f"results/racedays/{start_date}/{end_date}/list"

    def racekeys_all(self):
        for raceday in self.current_racedays():
            raceday_key = raceday["raceDay"]

            for race in raceday["races"]:
                key = f"{raceday_key}/{race['raceNumber']}"
                yield key

    def current_racedays(self):
        for raceday_results in self.read_records(sync_mode=SyncMode.full_refresh):
            for day in raceday_results["result"]:
                for raceday in day["raceDays"]:
                    yield raceday
