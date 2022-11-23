import settings

from sources.metabase.helpers import csv_to_list
from sources.metabase.clients import (
    TherapistJoiningNicedayAPI,
    TherapistInteractionAPI
)


class TherapistJoiningNicedayOperation:

    def __init__(self) -> None:
        self.api = TherapistJoiningNicedayAPI()

        self._load_data()

    def get_batch_therapists_organizations(self):
        # TODO:
        pass

    def _load_data(self):

        if not settings.DEV_MODE:
            self.api.download_data()

        data = csv_to_list(self.api._THERS_JOINING_ND_FILE)
        self.data = self._validate(data)

    def _validate(self, data):

        if len(data) < 1:
            raise ValueError(
                {self.api._THERS_JOINING_ND_FILE: 'The CSV File is empty or invalid.'}
            )

        headers = data.pop(0)
        expected_headers = ['date_joined', 'therapist_id', 'organization_id']

        if set(headers) != set(expected_headers):
            raise ValueError(
                {self.api._THERS_JOINING_ND_FILE: 'The headers of CSV File has changed!'}
            )

        return data


class TherapistInteractionOperation:

    def __init__(self) -> None:
        self.api = TherapistInteractionAPI()

        self._load_data()

    def get_batch_therapists_interactions(self):
        # TODO:
        pass

    def _load_data(self):

        if not settings.DEV_MODE:
            self.api.download_data()

        data = csv_to_list(self.api._THER_INTERACTIONS_FILE)
        self.data = self._validate(data)

    def _validate(self, data):

        if len(data) < 1:
            raise ValueError(
                {self.api._THER_INTERACTIONS_FILE: 'The CSV File is empty or invalid.'}
            )

        headers = data.pop(0)
        expected_headers = ['therapist_id', 'interaction_date', 'therapist_chat_count', 'call_count']

        if set(headers) != set(expected_headers):
            raise ValueError(
                {self.api._THER_INTERACTIONS_FILE: 'The headers of CSV File has changed!'}
            )

        return data
