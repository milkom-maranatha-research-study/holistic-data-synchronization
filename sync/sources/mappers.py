
from dask import dataframe as dask_dataframe
from dask.dataframe import Series
from typing import Dict, List, Tuple


class TherapistOrganizationMapper:

    def to_organization_dictionaries(self, dataframe: dask_dataframe) -> List[Dict]:
        """
        Returns list of the Organization ID dictionary from the given `dataframe`.
        """

        return [
            {'organization_id': int(getattr(row, 'organization_id'))}
            for row in dataframe.itertuples()
        ]

    def to_therapists_organization_map(self, dataframe: dask_dataframe) -> Dict:
        """
        Returns a map of the therapists organization from that given `dataframe`.
        """

        thers_org_map = {}

        for row in dataframe.itertuples():
            # Convert row into a dictionary
            ther_dict = self._get_therapist_dict(row)

            # Construct a dictionary of {[org_id]: [therapists]}
            # * Get and removes the Organization ID from the dictionary
            org_id = ther_dict.pop('organization_id')

            # * Get the list of therapists belonging to that Organization ID
            # * If no therapists are associated with it, use empty list.
            therapists = thers_org_map.get(org_id, [])

            # * Append the therapist dictionary into the list
            therapists.append(ther_dict)

            # * Update Organization ID therapists
            thers_org_map[org_id] = therapists

        return thers_org_map

    def _get_therapist_dict(self, row: Tuple[str, Series]) -> Dict:
        """
        Converts the given `row` into a dictionary and then return it.
        """
        return {
            'date_joined': getattr(row, 'date_joined').strftime("%Y-%m-%d"),
            'therapist_id': getattr(row, 'therapist_id'),
            'organization_id': int(getattr(row, 'organization_id'))
        }


class TherapistInteractionsMapper:

    def to_therapist_interaction_map(self, dataframe: dask_dataframe) -> Dict:
        """
        Returns a map of interactions therapist from that given `dataframe`.
        """

        interaction_map = self._group_by_therapist_and_interaction_date(dataframe)
        return self._group_by_therapist(interaction_map)

    def _group_by_therapist_and_interaction_date(self, dataframe: dask_dataframe) -> Dict:
        """
        Returns a map of interactions therapist grouped by `therapist_id` and `interaction_date`
        from that given `dataframe`.
        """

        interaction_map = {}

        for row in dataframe.itertuples():
            # Convert row into a dictionary
            interaction = self._get_interaction_dict(row)

            # Construct a dictionary of {[therapist_id#interaction_date]: [interactions]}
            # * Create a dictionary's key
            key = f"{interaction.pop('therapist_id')}#{interaction['interaction_date']}"

            # * Get the list of interactions belonging to that key
            # * If no interactions are associated with it, use empty list.
            interactions = interaction_map.get(key, [])

            # * Every therapist can have multiple interactions on the same date.
            # * It means they are treating multiple clients on the same day.
            # * However, since we don't have any information about the client,
            # * we put an auto-increment id to indicate that every interaction on the same day is unique.
            interaction['interaction_id'] = len(interactions) + 1

            # * Append the updated dictionary of interaction into the interaction list
            interactions.append(interaction)

            # * Update interaction map with interactions
            interaction_map[key] = interactions

        return interaction_map

    def _group_by_therapist(self, therapist_interaction_date_map: Dict) -> Dict:
        """
        Returns a map of interactions therapist grouped by `therapist_id`
        from that given `therapist_interaction_date_map`.
        """

        interaction_map = {}

        for key, values in therapist_interaction_date_map.items():
            # Get Therapist ID
            ther_id, _ = key.split('#')

            # Construct a dictionary of {[therapist_id]: [interactions]} for accumulating the interaction list
            # * Get the list of interactions belonging to that key
            # * If no interactions are associated with it, use empty list.
            interactions = interaction_map.get(ther_id, [])

            # * Extends the interaction list with the interaction values
            interactions.extend(values)

            # * Update interaction map with extended interactions
            interaction_map[ther_id] = interactions

        return interaction_map

    def _get_interaction_dict(self, row: Tuple[str, Series]) -> Dict:
        """
        Converts the given `row` into a dictionary and then return it.
        """

        return {
            'therapist_id': getattr(row, 'therapist_id'),
            'interaction_date': getattr(row, 'interaction_date').strftime("%Y-%m-%d"),
            'chat_count': int(getattr(row, 'therapist_chat_count')),
            'call_count': int(getattr(row, 'call_count'))
        }
