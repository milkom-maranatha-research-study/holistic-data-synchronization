import logging
import settings

from datetime import datetime
from dateutil.parser import parse
from typing import List, Dict, Tuple

from sources.clients import (
    TherapistJoiningNicedayAPI,
    TherapistInteractionAPI
)
from sources.helpers import csv_to_list
from sources.mappers import (
    TherapistOrganizationMapper,
    TherapistInteractionsMapper
)
from sources.dateutils import DateUtil


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

    def get_therapists_organization_map(self, start: int, end: int) -> Dict:
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

    def _validate(self, data: List[List]) -> List[List]:
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


class TherapistInteractionsMetabaseOperation:
    mapper = TherapistInteractionsMapper()
    dateutil = DateUtil()

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

        self.top_bottom_dates = self._get_bottom_top_interaction_dates()

    def get_interactions_therapist_map(self, start: datetime, end: datetime) -> Dict:
        """
        Returns chunked of therapist interactions data map from the CSV file
        based on the given `start` and `end` dates.
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        sliced_data = self._filter_data_from(start, end)

        return self.mapper.to_therapist_interaction_map(sliced_data)

    def get_interaction_date_periods(self, period_type: str) -> List[Tuple]:
        """
        Extracts and return a list of interaction date periods from the `self.data`
        with that specific `period_type`.

        Acceptable period type is either `'weekly'` or `'monthly'`.
        """

        assert hasattr(self, 'data'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        bottom, top = self._get_bottom_top_interaction_dates()

        return self.dateutil.get_periods_from(bottom, top, period_type)

    def _validate(self, data: List[List]) -> List[List]:
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

    def _get_bottom_top_interaction_dates(self) -> Tuple:
        """
        Returns tuple of the bottom-top interaction dates.
        """
        bottom_date = None
        top_date = None

        # Assumed the order of the list items is correct
        # (therapist_id, interaction_date, chat_count, call_count)
        for list_item in self.data:
            date = parse(list_item[1], yearfirst=True)

            # Bottom-top interaction dates initialization
            if bottom_date is None and top_date is None:

                bottom_date = date
                top_date = date

                continue

            # If incoming `date` is less than bottom date,
            # replace current bottom date with it.
            if date < bottom_date:
                bottom_date = date

            # If incoming `date` is greater than top date,
            # replace current top date with it.
            if date > top_date:
                top_date = date

        return (bottom_date, top_date)

    def _filter_data_from(self, start: datetime, end: datetime) -> List[List]:
        """
        Filter `self.data` based on the interaction date between `start` and `end` dates.

        TODO: REQUIRE OPTIMIZATION!! SWARM FILTERING IN COMBINATION WITH ASYNC.IO MIGHT BE A GOOD ONE!
        """

        filtered_items = []

        # Assumed the order of the list items is correct
        # (therapist_id, interaction_date, chat_count, call_count)
        for list_item in self.data:

            date = parse(list_item[1], yearfirst=True)

            if date >= start and date <= end:
                filtered_items.append(list_item)

        return filtered_items
