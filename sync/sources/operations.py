import logging
import settings

from dask import dataframe as dask_dataframe
from datetime import datetime
from typing import List, Dict, Tuple

from sources.clients import (
    TherapistJoiningNicedayAPI,
    TherapistInteractionAPI
)
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

        self._ddf = dask_dataframe.read_csv(
            self.api._THERS_JOINING_ND_FILE,
            dtype={
                'therapist_id': str,
                'organization_id': 'Int64',
                'date_joined': str
            },
            parse_dates=['date_joined']
        )

    def get_organizations(self) -> List[Dict]:
        """
        Returns list of the organization dictionaries.
        """

        assert hasattr(self, '_ddf'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        # Selects only the `organization_id` and distinct it.
        org_dataframe = self._ddf[['organization_id']].drop_duplicates().compute()

        return self.mapper.to_organization_dictionaries(org_dataframe)

    def get_therapists_organization_map(self) -> Dict:
        """
        Returns the therapists organization data map.

        We convert the whole dataframe into a dictionary object
        because the rows size is still relatively small (it's around ~2K rows).
        """

        assert hasattr(self, '_ddf'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        return self.mapper.to_therapists_organization_map(self._ddf)


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

        self._ddf = dask_dataframe.read_csv(
            self.api._THER_INTERACTIONS_FILE,
            dtype={
                'therapist_id': str,
                'interaction_date': str,
                'therapist_chat_count': 'Int64',
                'call_count': 'Int64'
            },
            parse_dates=['interaction_date']
        )

    def get_interactions_therapist_map(self, start: datetime, end: datetime) -> Dict:
        """
        Returns chunked of therapist interactions data map from the CSV file
        based on the given `start` and `end` dates.
        """

        assert hasattr(self, '_ddf'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        # Filters the dataframe where interaction date is between the given `start` and `end` dates.
        sliced_dataframe = self._ddf[
            (self._ddf['interaction_date'] >= start) & (self._ddf['interaction_date'] <= end)
        ]

        return self.mapper.to_therapist_interaction_map(sliced_dataframe)

    def get_interaction_date_periods(self, period_type: str) -> List[Tuple]:
        """
        Extracts and return a list of interaction date periods from the `self._ddf`
        with that specific `period_type`.

        Acceptable period type is either `'weekly'` or `'monthly'`.
        """

        assert hasattr(self, '_ddf'), (
            'Unable to perform this action!\n'
            'You must call `.collect_data()` first.'
        )

        df_min_obj, df_max_obj = dask_dataframe.compute(
            self._ddf[['interaction_date']].min(),
            self._ddf[['interaction_date']].max()
        )

        min_date = df_min_obj['interaction_date'].to_pydatetime()
        max_date = df_max_obj['interaction_date'].to_pydatetime()

        return self.dateutil.get_periods_from(min_date, max_date, period_type)
