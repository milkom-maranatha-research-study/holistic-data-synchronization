import pandas as pd

from dask import dataframe as dask_dataframe
from dateutil.parser import parse
from unittest import TestCase

from sync.sources.mappers import (
    TherapistOrganizationMapper,
    TherapistInteractionsMapper
)


class TestTherapistOrganizationMapper(TestCase):

    def setUp(self):
        self.mapper = TherapistOrganizationMapper()

    def test_to_organization_dictionaries_success(self):
        """
        Test the `to_organization_dictionaries` method to ensure it returns correct result.
        """

        pd_dataframe = pd.DataFrame({
            "organization_id": [1, 2, 3, 4, 5]
        })

        dataframe = dask_dataframe.from_pandas(pd_dataframe, npartitions=2)

        expected = [{'organization_id': 1}, {'organization_id': 2}, {'organization_id': 3}, {'organization_id': 4}, {'organization_id': 5}]
        actual = self.mapper.to_organization_dictionaries(dataframe)

        self.assertCountEqual(expected, actual)

    def test_to_therapists_organization_map_success(self):
        """
        Test the `to_therapists_organization_map` method to ensure it returns correct result.
        """

        pd_dataframe = pd.DataFrame({
            "organization_id": [1, 2, 1, 3, 4],
            "therapist_id": ['t1', 't2', 't3', 't4', 't5'],
            "date_joined": [
                parse('2021-01-28', yearfirst=True), parse('2021-02-03', yearfirst=True),
                parse('2021-03-14', yearfirst=True), parse('2021-04-05', yearfirst=True),
                parse('2021-05-01', yearfirst=True)
            ]
        })

        dataframe = dask_dataframe.from_pandas(pd_dataframe, npartitions=2)

        expected = {
            1: [{'date_joined': '2021-01-28', 'therapist_id': 't1'}, {'date_joined': '2021-03-14', 'therapist_id': 't3'}],
            2: [{'date_joined': '2021-02-03', 'therapist_id': 't2'}],
            3: [{'date_joined': '2021-04-05', 'therapist_id': 't4'}],
            4: [{'date_joined': '2021-05-01', 'therapist_id': 't5'}]
        }
        actual = self.mapper.to_therapists_organization_map(dataframe)

        self.assertDictEqual(expected, actual)

    def test_get_therapist_dict_success(self):
        """
        Test the `_get_therapist_dict` method to ensure it returns correct result.
        """

        pd_dataframe = pd.DataFrame({
            "organization_id": [1, 2, 1, 3, 4],
            "therapist_id": ['t1', 't2', 't3', 't4', 't5'],
            "date_joined": [
                parse('2021-01-28', yearfirst=True), parse('2021-02-03', yearfirst=True),
                parse('2021-03-14', yearfirst=True), parse('2021-04-05', yearfirst=True),
                parse('2021-05-01', yearfirst=True)
            ]
        })

        dataframe = dask_dataframe.from_pandas(pd_dataframe, npartitions=2)

        expected = [
            {'date_joined': '2021-01-28', 'therapist_id': 't1', 'organization_id': 1},
            {'date_joined': '2021-02-03', 'therapist_id': 't2', 'organization_id': 2},
            {'date_joined': '2021-03-14', 'therapist_id': 't3', 'organization_id': 1},
            {'date_joined': '2021-04-05', 'therapist_id': 't4', 'organization_id': 3},
            {'date_joined': '2021-05-01', 'therapist_id': 't5', 'organization_id': 4}
        ]
        actual = [self.mapper._get_therapist_dict(row) for row in dataframe.itertuples()]

        self.assertCountEqual(expected, actual)


class TestTherapistInteractionsMapper(TestCase):

    def setUp(self):
        self.mapper = TherapistInteractionsMapper()

    def test_to_therapist_interaction_map_success(self):
        """
        Test the `to_therapist_interaction_map` method to ensure it returns correct result.
        """

        pd_dataframe = pd.DataFrame({
            "therapist_id": ['t1', 't1', 't2', 't3', 't3'],
            "interaction_date": [
                parse('2021-01-28', yearfirst=True), parse('2021-02-03', yearfirst=True),
                parse('2021-03-14', yearfirst=True), parse('2021-04-05', yearfirst=True),
                parse('2021-04-05', yearfirst=True)
            ],
            "therapist_chat_count": [1, 1, 0, 1, 2],
            "call_count": [1, 2, 1, 1, 1]
        })

        dataframe = dask_dataframe.from_pandas(pd_dataframe, npartitions=2)

        expected = {
            't1': [
                {'interaction_date': '2021-01-28', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1},
                {'interaction_date': '2021-02-03', 'chat_count': 1, 'call_count': 2, 'interaction_id': 1}
            ],
            't2': [
                {'interaction_date': '2021-03-14', 'chat_count': 0, 'call_count': 1, 'interaction_id': 1},
            ],
            't3': [
                {'interaction_date': '2021-04-05', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1},
                {'interaction_date': '2021-04-05', 'chat_count': 2, 'call_count': 1, 'interaction_id': 2}
            ]
        }
        actual = self.mapper.to_therapist_interaction_map(dataframe)

        self.assertDictEqual(expected, actual)

    def test_group_by_therapist_and_interaction_date_success(self):
        """
        Test the `_group_by_therapist_and_interaction_date` method to ensure it returns correct result.
        """

        pd_dataframe = pd.DataFrame({
            "therapist_id": ['t1', 't1', 't2', 't3', 't3'],
            "interaction_date": [
                parse('2021-01-28', yearfirst=True), parse('2021-02-03', yearfirst=True),
                parse('2021-03-14', yearfirst=True), parse('2021-04-05', yearfirst=True),
                parse('2021-04-05', yearfirst=True)
            ],
            "therapist_chat_count": [1, 1, 0, 1, 2],
            "call_count": [1, 2, 1, 1, 1]
        })

        dataframe = dask_dataframe.from_pandas(pd_dataframe, npartitions=2)

        expected = {
            't1#2021-01-28': [
                {'interaction_date': '2021-01-28', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1}
            ],
            't1#2021-02-03': [
                {'interaction_date': '2021-02-03', 'chat_count': 1, 'call_count': 2, 'interaction_id': 1}
            ],
            't2#2021-03-14': [
                {'interaction_date': '2021-03-14', 'chat_count': 0, 'call_count': 1, 'interaction_id': 1},
            ],
            't3#2021-04-05': [
                {'interaction_date': '2021-04-05', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1},
                {'interaction_date': '2021-04-05', 'chat_count': 2, 'call_count': 1, 'interaction_id': 2}
            ]
        }
        actual = self.mapper._group_by_therapist_and_interaction_date(dataframe)

        self.assertDictEqual(expected, actual)

    def test_group_by_therapist_success(self):
        """
        Test the `_group_by_therapist` method to ensure it returns correct result.
        """

        ther_interaction_date_map = {
            't1#2021-01-28': [
                {'interaction_date': '2021-01-28', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1}
            ],
            't1#2021-02-03': [
                {'interaction_date': '2021-02-03', 'chat_count': 1, 'call_count': 2, 'interaction_id': 1}
            ],
            't2#2021-03-14': [
                {'interaction_date': '2021-03-14', 'chat_count': 0, 'call_count': 1, 'interaction_id': 1},
            ],
            't3#2021-04-05': [
                {'interaction_date': '2021-04-05', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1},
                {'interaction_date': '2021-04-05', 'chat_count': 2, 'call_count': 1, 'interaction_id': 2}
            ]
        }

        expected = {
            't1': [
                {'interaction_date': '2021-01-28', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1},
                {'interaction_date': '2021-02-03', 'chat_count': 1, 'call_count': 2, 'interaction_id': 1}
            ],
            't2': [
                {'interaction_date': '2021-03-14', 'chat_count': 0, 'call_count': 1, 'interaction_id': 1},
            ],
            't3': [
                {'interaction_date': '2021-04-05', 'chat_count': 1, 'call_count': 1, 'interaction_id': 1},
                {'interaction_date': '2021-04-05', 'chat_count': 2, 'call_count': 1, 'interaction_id': 2}
            ]
        }
        actual = self.mapper._group_by_therapist(ther_interaction_date_map)

        self.assertDictEqual(expected, actual)

    def test_get_interaction_dict_success(self):
        """
        Test the `_get_interaction_dict` method to ensure it returns correct result.
        """

        pd_dataframe = pd.DataFrame({
            "therapist_id": ['t1', 't1', 't2', 't3', 't3'],
            "interaction_date": [
                parse('2021-01-28', yearfirst=True), parse('2021-02-03', yearfirst=True),
                parse('2021-03-14', yearfirst=True), parse('2021-04-05', yearfirst=True),
                parse('2021-04-05', yearfirst=True)
            ],
            "therapist_chat_count": [1, 1, 0, 1, 2],
            "call_count": [1, 2, 1, 1, 1]
        })

        dataframe = dask_dataframe.from_pandas(pd_dataframe, npartitions=2)

        expected = [
            {'therapist_id': 't1', 'interaction_date': '2021-01-28', 'chat_count': 1, 'call_count': 1},
            {'therapist_id': 't1', 'interaction_date': '2021-02-03', 'chat_count': 1, 'call_count': 2},
            {'therapist_id': 't2', 'interaction_date': '2021-03-14', 'chat_count': 0, 'call_count': 1},
            {'therapist_id': 't3', 'interaction_date': '2021-04-05', 'chat_count': 1, 'call_count': 1},
            {'therapist_id': 't3', 'interaction_date': '2021-04-05', 'chat_count': 2, 'call_count': 1}
        ]
        actual = [self.mapper._get_interaction_dict(row) for row in dataframe.itertuples()]

        self.assertCountEqual(expected, actual)
