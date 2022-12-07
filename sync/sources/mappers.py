
from dask import dataframe as dask_dataframe
from typing import List, Dict


class TherapistOrganizationMapper:

    def to_organization_dictionaries(self, org_dataframe: dask_dataframe) -> List[Dict]:
        """
        Returns list of the Organization ID dictionary from the given `org_dataframe`.
        """

        return [
            {'organization_id': int(getattr(row, 'organization_id'))}
            for row in org_dataframe.itertuples()
        ]

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

        interaction_map = self._group_by_therapist_and_interaction_date(therapist_interaction_lists)
        return self._group_by_therapist(interaction_map)

    def _group_by_therapist_and_interaction_date(self, therapist_interaction_lists: List[List]) -> Dict:
        """
        Returns a map of interactions therapist grouped by `therapist_id` and `interaction_date`
        from that given `therapist_interaction_lists`.
        """

        interaction_map = {}

        for list_item in therapist_interaction_lists:
            interaction = self._get_interaction_dict(list_item)

            key = f"{interaction.pop('therapist_id')}#{interaction['interaction_date']}"

            interactions = interaction_map.get(key, [])

            # Every therapist can have multiple interactions on the same date.
            # It means they are treating multiple clients on the same day.
            # However, since we don't have any information about the client,
            # we put an auto-increment id to indicate that every interaction on the same day is unique.
            interaction['interaction_id'] = len(interactions) + 1

            interactions.append(interaction)

            interaction_map[key] = interactions

        return interaction_map

    def _group_by_therapist(self, therapist_interaction_date_map: Dict) -> Dict:
        """
        Returns a map of interactions therapist grouped by `therapist_id`
        from that given `therapist_interaction_date_map`.
        """

        interaction_map = {}

        for key, values in therapist_interaction_date_map.items():
            ther_id, _ = key.split('#')

            interactions = interaction_map.get(ther_id, [])
            interactions.extend(values)

            interaction_map[ther_id] = interactions

        return interaction_map

    def _get_interaction_dict(self, list_item: str) -> Dict:
        """
        Converts the given `list_item` into a dictionary and then return it.
        """

        therapist_id, interaction_date, chat_count, call_count = list_item

        return {
            'therapist_id': therapist_id,
            'interaction_date': interaction_date,
            'chat_count': chat_count,
            'call_count': call_count
        }
