from sources.metabase.operations import (
    TherapistJoiningNicedayOperation,
    TherapistInteractionOperation
)


class TherapistOrganizationSync:

    def __init__(self) -> None:
        self.metabase_operation = TherapistJoiningNicedayOperation()

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
        self.metabase_operation = TherapistInteractionOperation()

    def start(self):
        self._sync_interactions()

    def _sync_interactions(self):
        # TODO:
        pass


if __name__ == '__main__':
    TherapistOrganizationSync().start()
    TherapistInteractionSync().start()
