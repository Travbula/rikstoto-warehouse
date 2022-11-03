#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from imaplib import _Authenticator
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import NoAuth

from .streams import (
    RikstotoIndex,
    RikstotoIndexList,
    CompleteResults,
    TodaysPrograms,
    Yesterday,
    WinOddsRace,
    PlaceOddsRace,
)

import datetime


# Source
class SourceRikstotoApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        #accepted_racetracks = {"BT_NR", "FO_NR", "JA_NR"}
        #input_racetrack = config["racetrack"]

        #if input_racetrack not in accepted_racetracks:
        #    return False, f"Input racetrack {input_racetrack} is invalid. Please input one of the following racetracks: {accepted_racetracks}"
        #else:

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = NoAuth()

        # TODO: create HistoricSourceRikstotoApi that takes start_date and end_date?
        # or does this entail a lot of code duplication?
        start_date = config["start_date"]
        end_date = config["end_date"]

        today = RikstotoIndex(authenticator=auth)
        yesterday = Yesterday(authenticator=auth)
        #history = RikstotoIndexList(authenticator=auth, start_date="2020-01-29", end_date="2020-01-31")
        #history = self.generate_raceday_lists(auth, "2019-01-01", "2021-12-31")
        history = self.generate_raceday_lists(auth, start_date=start_date, end_date=end_date)

        return [
            CompleteResults(authenticator=auth, racedays=history),
            #TodaysPrograms(authenticator=auth, raceday_stream=today),
            PlaceOddsRace(authenticator=auth, racedays=history),
            WinOddsRace(authenticator=auth, racedays=history),
        ]

    # TODO: prøve å bruke incremental sync i stedet?
    def generate_raceday_lists(self, auth, start_date, end_date, increment=30):
        raceday_list = []

        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        while start_date < end_date:
            raceday_list.append(
                RikstotoIndexList(
                    authenticator=auth,
                    start_date=start_date.date(),
                    end_date=(start_date + datetime.timedelta(days=increment)).date()
                )
            )

            start_date = start_date + datetime.timedelta(days=increment)

        return raceday_list