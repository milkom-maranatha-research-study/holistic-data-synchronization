import logging

from settings import configure_logging
from sources.operations import (
    TherapistJoiningNicedayMetabaseOperation,
    TherapistInteractionsMetabaseOperation
)
from targets.operations import (
    OrganizationBackendOperation,
    TherapistsOrganizationBackendOperation,
    TherapistInteractionsBackendOperation
)


logger = logging.getLogger(__name__)


class TherapistsOrganizationSync:

    def __init__(self) -> None:
        self.backend_org_operation = OrganizationBackendOperation()
        self.backend_thers_org_operation = TherapistsOrganizationBackendOperation()
        self.metabase_operation = TherapistJoiningNicedayMetabaseOperation()

        self._start_sync()

    def _start_sync(self):
        """
        Start data synchronization from Metabase to the Backend database
        """

        # Step 1 - Collect data from Metabase
        self.metabase_operation.collect_data()

        # Step 2 - Sync Organization data from Metabase to the Backend database
        self._sync_organizations()

        # Step 3 - Sync Therapist Organization data from Metabase to the Backend database
        # self._sync_therapists_organization_in_batch()

    def _sync_organizations(self):
        """
        Runs full synchronization of the Organization objects.
        """

        logger.info("Retrieving Organization objects from Metabase data...")
        organizations = self.metabase_operation.get_organizations()

        logger.info("Upserting Organization objects in the Backend...")
        self.backend_org_operation.upsert(organizations)

    def _sync_therapists_organization_in_batch(self):
        """
        Runs batch synchronization for every Therapists in the Organization.
        """

        logger.info("Calculating batch sizing for sync...")

        size = self.metabase_operation.get_data_size()
        num_items_per_batch = 1000
        batch_size = size // num_items_per_batch + 1  # Ignores remainder

        start = 0

        for iteration in range(0, batch_size):
            tag = f"Batch {iteration + 1}"

            end = (iteration + 1) * num_items_per_batch

            logger.info(f"{tag} - Retrieving Therapists Organization objects from Metabase data...")
            therapists_org_map = self.metabase_operation.get_therapists_organization_map(start, end)

            logger.info(f"{tag} - Upserting every Therapists Organization object in the Backend...")
            for org_id, therapists in therapists_org_map.items():
                self.backend_thers_org_operation.upsert(org_id, therapists)

            start = end


class TherapistInteractionsSync:

    def __init__(self) -> None:
        self.backend_therapist_interactions = TherapistInteractionsBackendOperation()
        self.metabase_operation = TherapistInteractionsMetabaseOperation()

        self._start_sync()
    
    def _start_sync(self):
        """
        Start data synchronization from Metabase to the Backend database
        """

        # Step 1 - Collect data from Metabase
        self.metabase_operation.collect_data()

        # Step 3 - Sync Therapist Interaction data from Metabase to the Backend database
        self._sync_interactions_of_therapist_in_batch()


    def _sync_interactions_of_therapist_in_batch(self):
        """
        Runs batch synchronization for every Therapist Interactions per month.
        """

        logger.info("Calculating time periods for sync...")

        periods = self.metabase_operation.get_interaction_date_periods('monthly')

        for start, end in periods:

            tag = f"Batch: from {start} to {end}"

            logger.info(f"{tag} - Retrieving Therapist Interactions objects from Metabase data...")
            interactions_ther_map = self.metabase_operation.get_interactions_therapist_map(start, end)

            logger.info(f"{tag} - Upserting every Therapist Interactions object in the Backend...")
            for ther_id, interactions in interactions_ther_map.items():
                self.backend_therapist_interactions.upsert(ther_id, interactions)

            start = end


if __name__ == '__main__':
    configure_logging()

    TherapistsOrganizationSync()
    # TherapistInteractionsSync()
