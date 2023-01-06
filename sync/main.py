import logging

from datetime import datetime

from helpers import print_time_duration
from settings import configure_logging
from sources.operations import (
    TherapistMetabaseOperation,
    InteractionMetabaseOperation
)
from targets.operations import (
    OrganizationBackendOperation,
    TherapistBackendOperation,
    InteractionBackendOperation
)


logger = logging.getLogger(__name__)


class TherapistSynchronizer:

    def __init__(self) -> None:
        self.backend_org_operation = OrganizationBackendOperation()
        self.backend_ther_operation = TherapistBackendOperation()
        self.metabase_ther_operation = TherapistMetabaseOperation()

        self._start_sync()

    def _start_sync(self):
        """
        Start data synchronization from Metabase to the Backend database
        """

        start_time = datetime.now()

        # Step 1 - Collect data from Metabase
        self.metabase_ther_operation.collect_data()

        # Step 2 - Sync Organization data from Metabase to the Backend database
        self._sync_organizations()

        # Step 3 - Sync Therapist data from Metabase to the Backend database
        self._sync_therapists()

        end_time = datetime.now()

        print_time_duration('Sync Therapist and its Organization', start_time, end_time)

    def _sync_organizations(self):
        """
        Synchronize the Organization objects.
        """

        logger.info("Retrieving Organization objects from Metabase data...")
        organizations = self.metabase_ther_operation.get_organizations()

        logger.info("Upserting Organization objects in the Backend...")
        self.backend_org_operation.upsert(organizations)

    def _sync_therapists(self):
        """
        Synchronize every Therapists in the Organization.
        """

        logger.info("Retrieving Therapists objects from Metabase data...")
        org_therapists_map = self.metabase_ther_operation.get_organization_therapists_map()

        logger.info("Upserting every Therapists object in the Backend...")
        for org_id, therapists in org_therapists_map.items():
            self.backend_ther_operation.upsert(org_id, therapists)


class InteractionSynchronizer:

    def __init__(self) -> None:
        self.backend_interaction_operation = InteractionBackendOperation()
        self.metabase_operation = InteractionMetabaseOperation()

        self._start_sync()

    def _start_sync(self):
        """
        Start data synchronization from Metabase to the Backend database
        """

        start_time = datetime.now()

        # Step 1 - Collect data from Metabase
        self.metabase_operation.collect_data()

        # Step 2 - Sync Therapist Interactions from Metabase to the Backend database
        self._sync_interactions()

        end_time = datetime.now()

        print_time_duration('Sync Therapist Interactions', start_time, end_time)

    def _sync_interactions(self):
        """
        Synchronize every Therapist Interactions in monthly requests.
        """

        logger.info("Calculating time periods for sync...")

        periods = self.metabase_operation.get_interaction_date_periods('monthly')

        for start, end in periods:

            tag = f"Batch: from {start} to {end}"

            logger.info(f"{tag} - Retrieving Therapist Interactions objects from Metabase data...")
            ther_interactions_map = self.metabase_operation.get_therapist_interactions_map(start, end)

            logger.info(f"{tag} - Upserting every Therapist Interactions object in the Backend...")
            for ther_id, interactions in ther_interactions_map.items():
                self.backend_interaction_operation.upsert(ther_id, interactions)

            start = end


if __name__ == '__main__':
    configure_logging()

    TherapistSynchronizer()
    InteractionSynchronizer()
