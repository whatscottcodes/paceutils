from datetime import datetime
import pandas as pd
from paceutils.helpers import Helpers


class Enrollment(Helpers):
    """This is a class for running enrollment related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def census_today(self):
        """
        Census as of today.

        Args:
            None

        Returns:
            int: census as of date called
        """
        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE disenrollment_date IS NULL
        """

        return self.single_value_query(query)

    def census_during_period(self, params):
        """
        Count of ppts with an enrollment date prior to the end date
        and a disenrollment date that is null or after the start date.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: census over the time period indicated in the params
        """
        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?;"""

        return self.single_value_query(query, params)

    def census_on_end_date(self, params):
        """
        Count of ppts with an enrollment date before the start date
        and a disenrollment date that is null or after the end date.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: census as of end date in the params
        """
        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE enrollment_date <= ?
        AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL);"""

        return self.single_value_query(query, params)

    def member_months(self, params):
        """
        Sum of the first of the month census for each
        month between the two param dates.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: member months during the period
        """
        query = """SELECT SUM(total)
        FROM monthly_census
        WHERE month BETWEEN ? AND ?"""

        return self.single_value_query(query, params)

    def disenrolled(self, params):
        """
        Count of ppts with a disenrollment date between the two param dates.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: disenrolled count during the period
        """
        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE disenrollment_date BETWEEN ? AND ? """

        return self.single_value_query(query, params)

    def enrolled(self, params):
        """
        Count of ppts with a enrollment date between the two param dates.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: enrolled count during the period
        """
        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE enrollment_date BETWEEN ? AND ? """

        return self.single_value_query(query, params)

    def deaths(self, params):
        """
        Count of ppts with a disenrollment date between the two param dates
        and a disenroll type of 'Deceased'.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of deaths during the period
        """
        query = """SELECT COUNT(member_id) FROM enrollment
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND disenroll_type = 'Deceased';
                """

        return self.single_value_query(query, params)

    def net_enrollment_during_period(self, params):
        """
        Difference of the enrolled and disenrolled counts
        for the dates between the two param dates.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: net enrollment
        """
        return self.enrolled(params) - self.disenrolled(params)

    def net_enrollment(self, params):
        """
        Difference of the enrolled count of given period
        and disenrolled count of previous month.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: net enrollment
        """
        param_0 = pd.to_datetime(params[0]) - pd.DateOffset(months=1)
        param_1 = pd.to_datetime(params[1]) - pd.offsets.MonthEnd(1)

        prev_params = (param_0.strftime("%Y-%m-%d"), param_1.strftime("%Y-%m-%d"))
        return self.enrolled(params) - self.disenrolled(prev_params)

    def voluntary_disenrolled(self, params):
        """
        Count of ppts with a disenrollment date between
        the two param dates and a disenroll type of 'Voluntary'.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of voluntary disenrollments
        """
        query = """SELECT COUNT(member_id) FROM enrollment
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND disenroll_type = 'Voluntary'
                """

        return self.single_value_query(query, params)

    def voluntary_disenrolled_percent(self, params):
        """
        Voluntary Disenrollments divided by Disenrollments for disenrollment dates
        between the two param dates.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of disenrollments that are voluntary disenrollments
        """
        disenrolled = self.disenrolled(params)
        if disenrolled == 0:
            return 0
        return round(self.voluntary_disenrolled(params) / disenrolled * 100, 2)

    def avg_years_enrolled(self, params):
        """
        Average of the difference between end date or the ppt's disenrollment date
        and their enrollment date divided by 326.25.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: average years enrolled in PACE
        """
        params = [params[1]] + list(params)

        query = """
        with all_days as (select
            *, (julianday(disenrollment_date) - julianday(enrollment_date)) as days
        from
        enrollment
        where
        disenrollment_date IS NOT NULL
        UNION
        select
        *, (julianday(?) - julianday(enrollment_date)) as days
        from
        enrollment
        where
        disenrollment_date IS NULL)
        SELECT ROUND(AVG(days) / 365.25, 2)
        FROM all_days
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        """

        return self.single_value_query(query, params)

    def growth_rate(self, params):
        """
        Census as of the first of the previous month from the given param dates minus
        the census as of the first of the month for the given params divided by the census as of the
        first of the previous month from the given param dates multiplied by 100.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: growth rate
        """
        start_month, _ = self.prev_month_dates(
            params
        )  # get start of month before period starts
        end_month = datetime.strptime(params[0], "%Y-%m-%d").replace(
            day=1
        )  # get start of the month that ends the time period

        query = """SELECT total FROM monthly_census
        WHERE month = ?
        """
        starting_census = self.single_value_query(
            query, [start_month]
        )  # census on first of month before period

        ending_census = self.single_value_query(
            query, [end_month.strftime("%Y-%m-%d")]
        )  # census on first of month that ends period

        return round(((ending_census - starting_census) / starting_census) * 100, 2)

    def churn_rate(self, params):
        """
        Disenrolled count for the given params divided by
        the census as of the first date in params tuple.
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: churn rate
        """
        starting_query = """SELECT total FROM monthly_census
        WHERE month = ?
        """
        enrollment = Enrollment(self.db_filepath)
        disenrolled_over_period = enrollment.disenrolled(params)
        starting_census = self.single_value_query(starting_query, [params[0]])

        return round((disenrolled_over_period / starting_census) * 100, 2)

    def enrollment_by_town_table(self, params):
        """
        Count of distinct ppts grouped by town for ppts enrolled during the period.
        Returned as a pandas dataframe with columns `City/Town` and `Number of Ppts`.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: columns `City/Town` and `Number of Ppts`
        """
        query = f"""
            SELECT ad.city as 'City/Town', COUNT(DISTINCT(ad.member_id)) as 'Number of Ppts' FROM addresses ad
            JOIN enrollment e ON ad.member_id=e.member_id
            WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND enrollment_date <= ?
            GROUP BY city
            ORDER BY 'Number of Ppts' DESC;
            """

        df = self.dataframe_query(query, params)

        df.sort_values("Number of Ppts", ascending=False, inplace=True)

        return df

    def address_mapping_df(self):
        """
        Create two pandas dataframe with the columns `name`, `full_address`, `lat`, and `lon`,
        one for ppts enrolled during the period and one for those who are not.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrames: columns `name`, `full_address`, `lat`, and `lon'.
        """
        enrolled_address_query = """
            SELECT (p.first || ' ' || p.last) as name, (a.address || ', ' || a.city)
            as full_address, a.lat, a.lon
            FROM addresses a
            JOIN ppts p on a.member_id=p.member_id
            JOIN enrollment e on p.member_id=e.member_id
            WHERE e.disenrollment_date IS NULL
            GROUP BY a.member_id
            """

        disenrolled_address_query = """
            SELECT (p.first || ' ' || p.last) as name, (a.address || ', ' || a.city)
            as full_address, a.lat, a.lon
            FROM addresses a
            JOIN ppts p on a.member_id=p.member_id
            JOIN enrollment e on p.member_id=e.member_id
            WHERE e.disenrollment_date NOT NULL
            GROUP BY a.member_id
            """

        enrolled_df = self.dataframe_query(enrolled_address_query, params=None)
        disenrolled_df = self.dataframe_query(disenrolled_address_query, params=None)

        return enrolled_df, disenrolled_df

    def dual_enrolled(self, params):
        """
        Count of ppts with an enrollment date during the period,
        medicare status of 1 and a medicaid status of 1.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of newly enrolled ppts who are dual
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 1
                    """

        return self.single_value_query(query, params)

    def medicare_only_enrolled(self, params):
        """
        Count of ppts with an enrollment date during the period,
        medicare status of 1 and a medicaid status of 0.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of newly enrolled ppts who are medicare only
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 0
                    """

        return self.single_value_query(query, params)

    def medicaid_only_enrolled(self, params):
        """
        Count of ppts with an enrollment date during the period,
        medicare status of 0 and a medicaid status of 1.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of newly enrolled ppts who are medicaid only
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 1
                    """

        return self.single_value_query(query, params)

    def private_pay_enrolled(self, params):
        """
        Count of ppts with an enrollment date during the period,
        medicare status of 0 and a medicaid status of 0.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of newly enrolled ppts who are private pay
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 0
                    """

        return self.single_value_query(query, params)

    def dual_disenrolled(self, params):
        """
        Count of ppts with an disenrollment date during the period,
        medicare status of 1 and a medicaid status of 1.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of disenrolled ppts who are dual
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 1
                    """

        return self.single_value_query(query, params)

    def medicare_only_disenrolled(self, params):
        """
        Count of ppts with an disenrollment date during the period,
        medicare status of 1 and a medicaid status of 0.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of disenrolled ppts who are medicare only
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 0
                    """

        return self.single_value_query(query, params)

    def medicaid_only_disenrolled(self, params):
        """
        Count of ppts with an disenrollment date during the period,
        medicare status of 0 and a medicaid status of 1.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of disenrolled ppts who are medicaid only
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 1
                    """

        return self.single_value_query(query, params)

    def private_pay_disenrolled(self, params):
        """
        Count of ppts with an disenrollment date during the period,
        medicare status of 0 and a medicaid status of 0.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of disenrolled ppts who are private pay
        """
        query = """
                    SELECT COUNT(member_id) FROM enrollment
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 0
                    """

        return self.single_value_query(query, params)

    def inquiries(self, params):
        """
        Count of rows from the referral table where the
        referral_date is between the param dates.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: referrals during the period
        """
        query = """
        SELECT COUNT(*)
        FROM referrals
        WHERE referral_date BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def avg_days_to_enrollment(self, params):
        """
        Average difference of the enrollment_effective date and the referral date
        for referrals with an enrollment_effective date between the param dates.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: average day to enrollment for enrollments in period
        """
        query = """
        SELECT ROUND(AVG(julianday(enrollment_effective) - julianday(referral_date)), 2)
        FROM referrals
        WHERE enrollment_effective BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def conversion_rate_180_days(self, params):
        """
        Count of all referrals with a referral date between 180 days before the param start date
        and the param end date with a non-null enrollment effective date
        divided by the count of all referrals with a referral date between
        180 days before the param start date and the param end date.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: 180 day conversion rate
        """
        enrolled_query = """
        SELECT COUNT(*)
        FROM referrals
        WHERE (referral_date BETWEEN date(?, '-180 days') AND ?)
        AND enrollment_effective IS NOT NULL;
        """

        referral_query = """
        SELECT COUNT(*)
        FROM referrals
        WHERE (referral_date BETWEEN date(?, '-180 days') AND ?);
        """

        enrolled = self.single_value_query(enrolled_query, params)
        referrals = self.single_value_query(referral_query, params)

        if referrals == 0:
            return 0

        return round(enrolled / referrals, 2)

    def referral_source_count(self, params):
        """
        Count of referrals grouped by source for referrals with a referral date during the period.
        Returned as a pandas dataframe with columns `referral_source` and `referrals`.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: columns `referral_source` and `referrals`.
        """
        query = """
        SELECT referral_source, COUNT(*) as referrals
        FROM referrals
        WHERE (referral_date BETWEEN ? AND ?)
        GROUP BY referral_source
        ORDER BY COUNT(*) DESC;
        """

        return self.dataframe_query(query, params)

    def most_common_referral_source(self, params):
        """
        Creates the `referral_source_count` table and returns the first row as a tuple.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            tuple: top referral source, count of referrals
        """
        df = self.referral_source_count(params).fillna(0)
        return df.iloc[0][0], df.iloc[0][1]

    def referral_enrollment_rates_df(self, params):
        """
        Count of referrals with a non-null enrollment_effective date divided by the count of referrals grouped
        by source for referrals with a referral date during the period.
        Returned as a pandas dataframe with columns `referral_source` and `enrollment_rate`.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: columns `referral_source` and `enrollment_rate`.
        """
        query = """
        SELECT referral_source, Round((COUNT(enrollment_effective)*1.0  / COUNT(referral_date)*1.0 ), 2) as enrollment_rate
        FROM referrals
        WHERE (referral_date BETWEEN ? AND ?)
        GROUP BY referral_source
        ORDER BY enrollment_rate DESC;
        """
        return self.dataframe_query(query, params)

    def highest_enrollment_rate_referral_source(self, params):
        """
        Creates the `referral_enrollment_rates_df` table and returns the first row as a tuple.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            tuple: top referral source, enrollment rate
        """
        df = self.referral_enrollment_rates_df(params).fillna(0)
        return df.iloc[0][0], df.iloc[0][1]
