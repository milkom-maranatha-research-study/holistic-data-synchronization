from sources.operations import (
    TherapistJoiningNicedayOperation,
    TherapistInteractionOperation
)
from targets.operations import (
    OrganizationBackendOperation,
    TherapistsOrganizationBackendOperation,
    TherapistsInteractionBackendOperation
)


class TherapistOrganizationSync:

    def __init__(self) -> None:

        self.mb_thers_join_niceday_operation = TherapistJoiningNicedayOperation()
        self.be_org_operation = OrganizationBackendOperation()
        self.be_thers_org = TherapistsOrganizationBackendOperation()

    def start(self):
        self._sync_organizations()
        self._sync_therapists_organizations()

    def _sync_organizations(self):
        # TODO:
        pass

    def _sync_therapists_organizations(self):
        # TODO:
        pass


class TherapistInteractionSync:

    def __init__(self) -> None:
        self.mb_ther_interactions_operation = TherapistInteractionOperation()
        self.be_ther_interactions_operation = TherapistsInteractionBackendOperation()

    def start(self):
        self._sync_interactions()

    def _sync_interactions(self):
        # TODO:
        pass


if __name__ == '__main__':
    TherapistOrganizationSync().start()
    TherapistInteractionSync().start()
