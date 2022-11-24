import logging
import settings

from typing import List, Dict

from sources.clients import (
    TherapistJoiningNicedayAPI,
    TherapistInteractionAPI
)
from sources.helpers import csv_to_list
from sources.mappers import TherapistOrganizationMapper, TherapistInteractionsMapper


logger = logging.getLogger(__name__)


class TherapistJoiningNicedayMetabaseOperation:
    mapper = TherapistOrganizationMapper()

    def __init__(self) -> None:
        self.api = TherapistJoiningNicedayAPI()

    def collect_data(self) -> None:
        """
        Download and validates data from Metabase if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting data from Metabase or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        data = csv_to_list(self.api._THERS_JOINING_ND_FILE)
        self.data = self._validate(data)

    def get_organizations(self) -> List[Dict]:
        """
        Returns list of the organization dictionaries.
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        return self.mapper.to_organization_dictionaries(self.data)

    def get_therapists_organization_map(self, start=0, end=0) -> Dict:
        """
        Returns chunked of therapists organization data map from the CSV file
        based on the given `start` and `end` indexes.
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        data_size = self.get_data_size()

        if data_size == 0:
            return {}

        start = start if start >= 0 else 0
        end = end if end < data_size else data_size

        sliced_data = self.data[start:end]

        return self.mapper.to_therapist_organization_map(sliced_data)

    def get_data_size(self) -> int:
        """
        Returns the size of data
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        return len(self.data)

    def _validate(self, data: List[str]) -> List[str]:
        """
        Validates downloaded CSV `data` and removes the header's row from it.

        Returns the validated CSV data.
        """

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


class TherapistInteractionMetabaseOperation:
    mapper = TherapistInteractionsMapper()

    def __init__(self) -> None:
        self.api = TherapistInteractionAPI()

    def collect_data(self) -> None:
        """
        Download and validates data from Metabase if the Developer Mode is off.

        Otherwise, we load the temporary CSV file from disk.
        """

        logger.info("Collecting data from Metabase or importing from disk...")

        if not settings.DEV_MODE:
            self.api.download_data(format='csv')

        data = csv_to_list(self.api._THER_INTERACTIONS_FILE)
        self.data = self._validate(data)

    def get_interactions_therapist_map(self, start=0, end=0) -> Dict:
        """
        Returns chunked of interactions of the therapist data map from the CSV file
        based on the given `start` and `end` indexes.
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        data_size = self.get_data_size()

        if data_size == 0:
            return {}

        start = start if start >= 0 else 0
        end = end if end < data_size else data_size

        sliced_data = self.data[start:end]

        return self.mapper.to_therapist_interaction_map(sliced_data)

    def get_data_size(self) -> int:
        """
        Returns the size of data
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        return len(self.data)

    def _validate(self, data: List[str]) -> List[str]:
        """
        Validates downloaded CSV `data` and removes the header's row from it.

        Returns the validated CSV data.
        """

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
