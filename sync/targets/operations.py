from targets.clients import (
    OrganizationAPI,
    TherapistOrganizationAPI,
    TherapistInteractionAPI
)


class OrganizationBackendOperation:

    def __init__(self) -> None:
        self.api = OrganizationAPI()

    def get_batch_therapists_organizations(self):
        # TODO:
        pass


class TherapistsOrganizationBackendOperation:

    def __init__(self) -> None:
        self.api = TherapistOrganizationAPI()

    def get_batch_therapists_interactions(self):
        # TODO:
        pass


class TherapistsInteractionBackendOperation:

    def __init__(self) -> None:
        self.api = TherapistInteractionAPI()

    def get_batch_therapists_interactions(self):
        # TODO:
        pass
