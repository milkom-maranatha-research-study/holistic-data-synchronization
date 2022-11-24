from typing import List, Dict


class TherapistOrganizationMapper:

    def to_organization_dictionaries(self, therapist_organization_lists: List[List]) -> List[Dict]:
        """
        Returns list of dictionary that contains unique organization ids
        from that given `therapist_organization_lists` list.
        """

        org_ids = self._get_organization_ids(therapist_organization_lists)

        return [{'organization_id': org_id} for org_id in org_ids]

    def to_therapist_organization_map(self, therapist_organization_lists: List[List]) -> Dict:
        """
        Returns a map of therapists organization from that given `therapist_organization_lists`.
        """

        ther_org_map = {}

        for list_item in therapist_organization_lists:
            ther_org = self._get_ther_org_dict(list_item)

            org_id = ther_org.pop('organization_id')

            therapists = ther_org_map.get(org_id, [])
            therapists.append(ther_org)

            ther_org_map[org_id] = therapists

        return ther_org_map

    def _get_organization_ids(self, therapist_organization_lists: List[List]) -> set:
        """
        Returns unique organization ids from that given `therapist_organization_lists`.
        """

        org_ids = set()

        for list_item in therapist_organization_lists:
            _, _, org_id = list_item
            org_ids.add(org_id)

        return org_ids

    def _get_ther_org_dict(self, list_item: str) -> Dict:
        """
        Converts the given `list_item` into a dictionary and then return it.
        """

        date_joined, therapist_id, org_id = list_item
        return {'date_joined': date_joined, 'therapist_id': therapist_id, 'organization_id': org_id}


class TherapistInteractionsMapper:

    def to_therapist_interaction_map(self, therapist_interaction_lists: List[List]) -> Dict:
        """
        Returns a map of interactions therapist from that given `therapist_interaction_lists`.
        """

        # TODO:
        return {}
