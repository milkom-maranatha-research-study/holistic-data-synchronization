from dateutil.parser import parse
from unittest import TestCase

from sync.sources.dateutils import DateUtil


class TestDateUtil(TestCase):

    def setUp(self):
        self.util = DateUtil()

    def test_get_periods_from_1(self):
        """
        Test the `get_periods_from` method to return a single item of weekly period.
        """

        start = parse('24/11/2022')
        end = parse('25/11/2022')

        actual = self.util.get_periods_from(start, end, 'weekly')
        expected = [
            (parse('21/11/2022'), parse('27/11/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_periods_from_2(self):
        """
        Test the `get_periods_from` method to return multiple items of weekly periods.
        """

        start = parse('24/11/2022')
        end = parse('01/12/2022', dayfirst=True)

        actual = self.util.get_periods_from(start, end, 'weekly')
        expected = [
            (parse('21/11/2022'), parse('27/11/2022')),
            (parse('28/11/2022'), parse('04/12/2022', dayfirst=True))
        ]

        self.assertListEqual(actual, expected)

    def test_get_periods_from_3(self):
        """
        Test the `get_periods_from` method to return a single item of weekly period
        when the `start` and `end` dates are equal.
        """

        start = parse('24/11/2022')
        end = start

        actual = self.util.get_periods_from(start, end, 'weekly')
        expected = [
            (parse('21/11/2022'), parse('27/11/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_periods_from_4(self):
        """
        Test the `get_periods_from` method to return a single item of monthly period.
        """

        start = parse('02/01/2022', dayfirst=True)
        end = parse('20/01/2022')

        actual = self.util.get_periods_from(start, end, 'monthly')
        expected = [
            (parse('01/01/2022', dayfirst=True), parse('31/01/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_periods_from_5(self):
        """
        Test the `get_periods_from` method to return multiple items of monthly periods.
        """

        start = parse('24/12/2022')
        end = parse('01/12/2023', dayfirst=True)

        actual = self.util.get_periods_from(start, end, 'monthly')
        expected = [
            (parse('01/12/2022', dayfirst=True), parse('31/12/2022')),
            (parse('01/01/2023', dayfirst=True), parse('31/01/2023')),
            (parse('01/02/2023', dayfirst=True), parse('28/02/2023')),
            (parse('01/03/2023', dayfirst=True), parse('31/03/2023')),
            (parse('01/04/2023', dayfirst=True), parse('30/04/2023')),
            (parse('01/05/2023', dayfirst=True), parse('31/05/2023')),
            (parse('01/06/2023', dayfirst=True), parse('30/06/2023')),
            (parse('01/07/2023', dayfirst=True), parse('31/07/2023')),
            (parse('01/08/2023', dayfirst=True), parse('31/08/2023')),
            (parse('01/09/2023', dayfirst=True), parse('30/09/2023')),
            (parse('01/10/2023', dayfirst=True), parse('31/10/2023')),
            (parse('01/11/2023', dayfirst=True), parse('30/11/2023')),
            (parse('01/12/2023', dayfirst=True), parse('31/12/2023')),
        ]

        self.assertListEqual(actual, expected)

    def test_get_periods_from_6(self):
        """
        Test the `get_periods_from` method to return a single item of monthly period
        when the `start` and `end` dates are equal.
        """

        start = parse('01/11/2022', dayfirst=True)
        end = start

        actual = self.util.get_periods_from(start, end, 'monthly')
        expected = [
            (parse('01/11/2022', dayfirst=True), parse('30/11/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_weekly_periods_1(self):
        """
        Test the `get_weekly_periods` method to return a single item of weekly period.
        """

        start = parse('24/11/2022')
        end = parse('25/11/2022')

        actual = self.util.get_weekly_periods(start, end)
        expected = [
            (parse('21/11/2022'), parse('27/11/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_weekly_periods_2(self):
        """
        Test the `get_weekly_periods` method to return multiple items of weekly periods.
        """

        start = parse('24/11/2022')
        end = parse('01/12/2022', dayfirst=True)

        actual = self.util.get_weekly_periods(start, end)
        expected = [
            (parse('21/11/2022'), parse('27/11/2022')),
            (parse('28/11/2022'), parse('04/12/2022', dayfirst=True))
        ]

        self.assertListEqual(actual, expected)

    def test_get_weekly_periods_3(self):
        """
        Test the `get_weekly_periods` method to return a single item of weekly period
        when the `start` and `end` dates are equal.
        """

        start = parse('24/11/2022')
        end = start

        actual = self.util.get_weekly_periods(start, end)
        expected = [
            (parse('21/11/2022'), parse('27/11/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_monthly_periods_1(self):
        """
        Test the `get_monthly_periods` method to return a single item of monthly period.
        """

        start = parse('02/01/2022', dayfirst=True)
        end = parse('20/01/2022')

        actual = self.util.get_monthly_periods(start, end)
        expected = [
            (parse('01/01/2022', dayfirst=True), parse('31/01/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_monthly_periods_2(self):
        """
        Test the `get_monthly_periods` method to return multiple items of monthly periods.
        """

        start = parse('24/12/2022')
        end = parse('01/12/2023', dayfirst=True)

        actual = self.util.get_monthly_periods(start, end)
        expected = [
            (parse('01/12/2022', dayfirst=True), parse('31/12/2022')),
            (parse('01/01/2023', dayfirst=True), parse('31/01/2023')),
            (parse('01/02/2023', dayfirst=True), parse('28/02/2023')),
            (parse('01/03/2023', dayfirst=True), parse('31/03/2023')),
            (parse('01/04/2023', dayfirst=True), parse('30/04/2023')),
            (parse('01/05/2023', dayfirst=True), parse('31/05/2023')),
            (parse('01/06/2023', dayfirst=True), parse('30/06/2023')),
            (parse('01/07/2023', dayfirst=True), parse('31/07/2023')),
            (parse('01/08/2023', dayfirst=True), parse('31/08/2023')),
            (parse('01/09/2023', dayfirst=True), parse('30/09/2023')),
            (parse('01/10/2023', dayfirst=True), parse('31/10/2023')),
            (parse('01/11/2023', dayfirst=True), parse('30/11/2023')),
            (parse('01/12/2023', dayfirst=True), parse('31/12/2023')),
        ]

        self.assertListEqual(actual, expected)

    def test_get_monthly_periods_3(self):
        """
        Test the `get_monthly_periods` method to return a single item of monthly period
        when the `start` and `end` dates are equal.
        """

        start = parse('01/11/2022', dayfirst=True)
        end = start

        actual = self.util.get_monthly_periods(start, end)
        expected = [
            (parse('01/11/2022', dayfirst=True), parse('30/11/2022'))
        ]

        self.assertListEqual(actual, expected)

    def test_get_week_period_of(self):
        """
        Test the `get_week_period_of` method to return a correct weekly period
        from the specific date.
        """

        date = parse('24/11/2022')

        actual = self.util.get_week_period_of(date)
        expected = (
            parse('21/11/2022'), parse('27/11/2022')
        )

        self.assertEqual(actual, expected)

    def test_get_month_period_of(self):
        """
        Test the `get_month_period_of` method to return a correct monthly period
        from the specific date.
        """

        date = parse('24/11/2022')

        actual = self.util.get_month_period_of(date)
        expected = (
            parse('01/11/2022', dayfirst=True), parse('30/11/2022')
        )

        self.assertEqual(actual, expected)

    def test_parse_datestr(self):
        """
        Test the `parse_datestr` method to convert datestring
        and it must returns a correct date.
        """

        date_str = '2022/11/02'

        actual = self.util.parse_datestr(date_str)
        expected = parse('02/11/2022', dayfirst=True)

        self.assertEqual(actual, expected)
