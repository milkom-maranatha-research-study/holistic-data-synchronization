from sources.metabase.clients import (
    TherapistJoiningNicedayAPI,
    TherapistInteractionAPI
)


class TherapistJoiningNicedayOperation:

    def __init__(self) -> None:
        self.api = TherapistJoiningNicedayAPI()

    def get_batch_organizations(self):
        # TODO:
        pass

    def get_batch_therapists_organizations(self):
        # TODO:
        pass


class TherapistInteractionOperation:

    def __init__(self) -> None:
        self.api = TherapistInteractionAPI()

    def get_batch_therapists_interactions(self):
        # TODO:
        pass
