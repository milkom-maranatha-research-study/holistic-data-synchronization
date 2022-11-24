import logging

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
        self.metabase_operation = TherapistJoiningNicedayMetabaseOperation()

        self.backend_org_operation = OrganizationBackendOperation()
        self.backend_thers_org_operation = TherapistsOrganizationBackendOperation()

        self.metabase_operation.collect_data()

        self._sync_organizations()
        self._sync_therapists_organization_in_batch()

    def _sync_organizations(self):
        """
        Runs full synchronization of the Organization objects.
        """

        logger.info("Upserting Organization objects in the Backend...")

        organizations = self.metabase_operation.get_organizations()
        self.backend_org_operation.upsert(organizations)

    def _sync_therapists_organization_in_batch(self):
        """
        Runs batch synchronization for every Therapists Organization.
        """

        size = self.metabase_operation.get_data_size()
        num_items_per_batch = 1000
        batch_size = size // num_items_per_batch + 1  # Ignores remainder

        start = 0

        for iteration in range(0, batch_size):
            tag = f"Batch {iteration + 1}"

            logger.info(f"{tag} - Upserting every Therapists Organization object in the Backend...")

            end = (iteration + 1) * num_items_per_batch

            therapists_org_map = self.metabase_operation.get_therapists_organization_map(start, end)

            for org_id, therapists in therapists_org_map.items():
                self.backend_thers_org_operation.upsert(org_id, therapists)

            start = end


class TherapistInteractionsSync:

    def __init__(self) -> None:
        self.metabase_operation = TherapistInteractionsMetabaseOperation()

        self.backend_therapist_interactions = TherapistInteractionsBackendOperation()

        self.metabase_operation.collect_data()

        self._sync_interactions_of_therapist_in_batch()

    def _sync_interactions_of_therapist_in_batch(self):
        # TODO:
        pass


if __name__ == '__main__':
    TherapistsOrganizationSync()
    TherapistInteractionsSync()
