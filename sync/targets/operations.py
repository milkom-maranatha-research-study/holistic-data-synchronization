from typing import List, Dict

from targets.clients import (
    OrganizationAPI,
    TherapistAPI,
    InteractionAPI
)


class OrganizationBackendOperation:

    def __init__(self) -> None:
        self.api = OrganizationAPI()

    def upsert(self, organizations: List[Dict]) -> Dict:
        """
        Create or update organization object in the Backend.
        """

        return self.api.upsert(organizations)


class TherapistBackendOperation:

    def __init__(self) -> None:
        self.api = TherapistAPI()

    def upsert(self, org_id: int, therapists: List[Dict]) -> Dict:
        """
        Create or update therapist objects per organization in the Backend.
        """

        return self.api.upsert(org_id, therapists)


class InteractionBackendOperation:

    def __init__(self) -> None:
        self.api = InteractionAPI()

    def upsert(self, ther_id: str, interactions: List[Dict]) -> Dict:
        """
        Create or update interaction objects per therapist in the Backend.
        """

        return self.api.upsert(ther_id, interactions)
