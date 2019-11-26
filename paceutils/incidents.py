import numpy as np
from paceutils.helpers import Helpers
from paceutils.enrollment import Enrollment


class Incidents(Helpers):
    """This is a class for running incident related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def incident_per_100MM(self, params, incident_table):
        """
        Count of incidents with a date_time_occurred date during the period divided by the
        sum of the first of the month census for each month during the period multiplied by 100.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: incidents per 100 member months
        """
        query = f"""SELECT ROUND(SUM(incidents)*100.0/SUM(census), 2) as rate
        FROM
        (SELECT total as census, month,
        strftime("%Y-%m", month) as month_year
        FROM monthly_census) as month_year_census
        LEFT JOIN
        (
        SELECT COUNT({incident_table}.date_time_occurred) as incidents,
        strftime("%Y-%m", date_time_occurred) as month_year
        FROM {incident_table}
        GROUP BY month_year
        ) as monthly_incidents
        ON month_year_census.month_year = monthly_incidents.month_year
        WHERE month BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def total_incidents(self, params, incident_table):
        """
        Count of incidents with a date_time_occurred date during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: count of incidents
        """
        query = f"""SELECT COUNT(*) FROM {incident_table}
        WHERE date_time_occurred BETWEEN ? AND ?;"""

        return self.single_value_query(query, params)

    def num_of_incident_repeaters(self, params, incident_table):
        """
        Count of ppts with more than 1 incident during period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: Count of ppts with more than 1 incident
        """
        query = f"""SELECT COUNT(*)
        FROM (
            SELECT member_id, COUNT(*) as num_incidents FROM {incident_table}
            WHERE date_time_occurred BETWEEN ? AND ?
            GROUP BY member_id
            HAVING num_incidents > 1
            );"""

        return self.single_value_query(query, params)

    def incidents_by_repeaters(self, params, incident_table):
        """
        Sum of the count of incidents attributed to ppts with more than 1 incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: Sum of the count of incidents attributed to ppts
                with more than 1 incident in the period.
        """
        query = f"""SELECT SUM(num_incidents)
        FROM (
            SELECT member_id, COUNT(*) as num_incidents FROM {incident_table}
            WHERE date_time_occurred BETWEEN ? AND ?
            GROUP BY member_id
            HAVING num_incidents > 1
            );"""

        return self.single_value_query(query, params)

    def ppts_w_incident(self, params, incident_table):
        """
        Count of distinct ppts with an incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: Count of distinct ppts with an incident in the period.
        """
        query = f"""SELECT COUNT(DISTINCT(member_id))
        FROM {incident_table}
        WHERE date_time_occurred BETWEEN ? AND ?;"""

        return self.single_value_query(query, params)

    def percent_by_repeaters(self, params, incident_table):
        """
        Sum of the count of incidents attributed to ppts with more than 1 incident in the period
        divided by the total_incidents in the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: percent of incidents attributed to repeaters
        """
        repeat_sum = self.incidents_by_repeaters(params, incident_table)
        total = self.total_incidents(params, incident_table)
        if total == 0:
            return 0

        return round(repeat_sum / total * 100, 2)

    def repeat_ppts_rate(self, params, incident_table):
        """
        Count of ppts with more than 1 incident during period divided by
        the count of distinct ppts with an incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: rate of ppts with an incident who are repeaters
        """
        repeat_ppts = self.num_of_incident_repeaters(params, incident_table)
        unique_ppts = self.ppts_w_incident(params, incident_table)
        if unique_ppts == 0:
            return 0
        return round(repeat_ppts / unique_ppts * 100, 2)

    def incident_avg_value(self, params, incident_table):
        """
        Average number of incidents for participants who have had an incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: Average number of incidents for participants
                who have had an incident in the period.
        """
        query = f"""SELECT AVG(num_incidents)
            FROM (
            SELECT member_id, COUNT(*) as num_incidents FROM {incident_table}
            WHERE date_time_occurred BETWEEN ? AND ?
            GROUP BY member_id
            HAVING num_incidents > 0
            );"""

        return self.single_value_query(query, params)

    def ppts_above_avg(self, params, incident_table):
        """
        Ppts with more incidents during the period than the calculated average during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: Ppts with more incidents during the period
                than the calculated average during the period.
        """
        average_per_faller = self.incident_avg_value(params, incident_table)
        params = list(params) + [average_per_faller]

        query = f"""SELECT COUNT(*)
            FROM (
            SELECT member_id, COUNT(*) as num_incidents FROM {incident_table}
            WHERE date_time_occurred BETWEEN ? AND ?
            GROUP BY member_id
            HAVING num_incidents > ?
            );"""

        return self.single_value_query(query, params)

    def percent_of_ppts_over_avg(self, params, incident_table):
        """
        Ppts with more incidents during the period than the calculated average during the period
        divided by the count of distinct ppts with an incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: percent of ppts with an incident count above average
        """
        num_above_avg = self.ppts_above_avg(params, incident_table)
        total = self.ppts_w_incident(params, incident_table)
        if total == 0:
            return 0
        return round(num_above_avg / total * 100, 2)

    def ppts_w_multiple_incidents(self, params, incident_table):
        """
        Creates a list of member_id and incident count tuples for ppts
        with more than 1 incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            list: list of tuples of (member_id, count of incidents)
        """

        query = f"""
        SELECT member_id, COUNT(*)
        FROM {incident_table}
        WHERE date_time_occurred BETWEEN ? AND ?
        GROUP BY member_id
        HAVING COUNT(*) > 1;
        """
        return self.fetchall_query(query, params)

    def adjusted_incident_count(self, params, incident_table):
        """
        Count of incidents during period minus any incidents by ppts with an
        incidents count greater than the mean plus 3 standard deviations

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: adjusted count of incidents
        """
        query = f"""
        SELECT member_id, COUNT(*)
        FROM {incident_table}
        WHERE date_time_occurred BETWEEN ? AND ?
        GROUP BY member_id
        """
        ppts_w_incident_counts = self.fetchall_query(query, params)

        if not ppts_w_incident_counts:
            return 0

        incident_mean = np.mean([val[1] for val in ppts_w_incident_counts])
        incident_sd = np.std([val[1] for val in ppts_w_incident_counts])

        outlier_num = incident_mean + (3 * incident_sd)

        falls_by_outliers = sum(
            [val[1] for val in ppts_w_incident_counts if val[1] >= outlier_num]
        )

        return self.total_incidents(params, incident_table) - falls_by_outliers

    def percent_without_incident_overall(self, params, incident_table):
        """
        Percent of ppts enrolled during period who never have an incident

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: Percent of ppts enrolled during period who never have an incident
        """
        query = f"""SELECT COUNT(*)
        FROM enrollment e
        WHERE NOT EXISTS (SELECT *
        FROM {incident_table} it
        WHERE it.member_id = e.member_id)
        AND (e.disenrollment_date >= ?
            OR e.disenrollment_date IS NULL)
        AND e.enrollment_date <= ?;"""

        enrollment = Enrollment(self.db_filepath)
        return round(
            (
                self.single_value_query(query, params)
                / enrollment.census_during_period(params)
                * 100
            ),
            2,
        )

    def percent_without_incident_in_period(self, params, incident_table):
        """
        Percent of ppts enrolled during period who did not have an incident
        in the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: Percent of ppts enrolled during period who did not have an incident
                in the period
        """
        params = list(params) + list(params)
        query = f"""SELECT COUNT(*)
        FROM enrollment e
        WHERE NOT EXISTS (SELECT 1 
        FROM {incident_table} it
        WHERE it.member_id = e.member_id
        AND date_time_occurred BETWEEN ? AND ?)
        AND (e.disenrollment_date >= ?
            OR e.disenrollment_date IS NULL)
        AND e.enrollment_date <= ?;"""

        enrollment = Enrollment(self.db_filepath)

        return round(
            (
                self.single_value_query(query, params)
                / enrollment.census_during_period(params[:2])
                * 100
            ),
            2,
        )

    def wounds_above_stage1(self, params):
        """
        Count of wounds with a date_time_occurred during the period and
        an ulcer stage of Stage 2, Stage 3, Stage 4, or Unstageable.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of wounds above stage 1
        """
        query = """SELECT COUNT(*)
        FROM wounds
        WHERE date_time_occurred BETWEEN ? AND ?
        AND ulcer_stage IN ('Stage 2', 'Stage 3', 'Stage 4', 'Unstageable')
        """
        return self.single_value_query(query, params)

    def unstageable_wounds_count(self, params):
        """
        Count of wounds with a date_time_occurred during the period
        and an ulcer stage of Unstageable.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of unstageable wounds
        """
        query = """SELECT COUNT(*)
        FROM wounds
        WHERE date_time_occurred BETWEEN ? AND ?
        AND ulcer_stage = 'Unstageable';
        """
        return self.single_value_query(query, params)

    def pressure_ulcer_count(self, params):
        """
        Count of wounds with a date_time_occurred during the period
        and a wound_type of Pressure Ulcer.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of pressure ulcers
        """
        query = """SELECT COUNT(*) FROM wounds
        WHERE wound_type = 'Pressure Ulcer' 
        AND (date_healed >= ? OR
            date_healed IS NULL)
        AND date_time_occurred <= ?;"""

        return self.single_value_query(query, params)

    def pressure_ulcer_per_100(self, params):
        """
        Pressure ulcer per 100 member months during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Pressure ulcer per 100 member months
        """
        enrollment = Enrollment(self.db_filepath)

        return round(
            (
                self.pressure_ulcer_count(params)
                / enrollment.member_months(params)
                * 100
            ),
            2,
        )

    def avg_wound_healing_time(self, params):
        """
        Average of the difference between the date_healed and the date_time_occurred
        for wounds with a date_healed during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: average wound healing time
        """
        query = """SELECT ROUND(AVG(julianday(date_healed) - julianday(date_time_occurred)), 2)
        FROM wounds
        WHERE date_healed BETWEEN ? AND ?;"""

        return self.single_value_query(query, params)

    def uti_count(self, params):
        """
        Counts of infections during the period with an
        infection_type of UTI, URI, or Sepsis-Urinary.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of UTIs
        """
        query = """
        SELECT COUNT(member_id)
        FROM infections
        WHERE (infection_type = 'UTI'
        OR infection_type = 'URI'
        OR infection_type = 'Sepsis-Urinary')
        AND date_time_occurred BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def uti_per_100(self, params):
        """
        Counts of infections during the period with an
        infection_type of UTI, URI, or Sepsis-Urinary divided by
        member months in the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: UTIs per 100 member months
        """
        enrollment = Enrollment(self.db_filepath)
        return round(self.uti_count(params) / enrollment.member_months(params) * 100, 2)

    def sepsis_count(self, params):
        """
        Count of infections during the period where 'sepsis'
        is in the infection_type.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of sepsis related infections
        """
        query = """
        SELECT COUNT(member_id)
        FROM infections
        WHERE (instr(infection_type, 'Sepsis') > 0)
        AND date_time_occurred BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def sepsis_per_100(self, params):
        """
        Counts of infections during the period where 'sepsis'
        is in the infection_type divided by
        member months in the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: sepsis related infections per 100 member months
        """
        enrollment = Enrollment(self.db_filepath)
        return round(
            self.sepsis_count(params) / enrollment.member_months(params) * 100, 2
        )

    def third_degree_burn_count(self, params):
        """
        Count of burns during period with a burn_degree of Third or Fourth.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of 3rd and 4th degree burns
        """
        query = """
        SELECT COUNT(member_id)
        FROM burns
        WHERE (burn_degree = 'Third'
        OR burn_degree = 'Fourth')
        AND date_time_occurred BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def burn_degree_counts(self, params):
        """
        Count of burns during period grouped by degree.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            list: list of tuples of (degree, count)
        """
        query = """SELECT burn_degree, COUNT(*)
        FROM burns
        WHERE date_time_occurred BETWEEN ? AND ?
        GROUP BY burn_degree
        ORDER BY COUNT(*) DESC;"""

        return self.fetchall_query(query, params)

    def major_harm_or_death_count(self, params, incident_table):
        """
        Count of incidents during period where the severity is equal to Major Harm or Death.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident (only Falls or Med Errors)

        Returns:
            int: number of incidents that result in major harm or death
        """
        query = f"""
            SELECT COUNT(*) FROM {incident_table}
            WHERE date_time_occurred BETWEEN ? AND ?
            AND (severity = 'Major Harm'
            OR severity = 'Death');
            """
        return self.single_value_query(query, params)

    def med_errors_responsibility_counts(self, params):
        """
        The sum of each of the responsibility columns (responsibility_pharmacy, responsibility_clinic,
        responsibility_home_care, responsibility_facility) in the med_errors table for
        med_errors occurring during the period.
        Returns a pandas dataframe with the columns `responsibility` and `count`.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: with columns `responsibility` and `count`
        """
        query = """SELECT SUM(responsibility_pharmacy) as Pharmacy,
        SUM(responsibility_clinic) as Clinic,
        SUM(responsibility_home_care) as 'Home Care',
        SUM(responsibility_facility) as Facility
        FROM med_errors
        WHERE date_time_occurred BETWEEN ? AND ?;"""

        df = (
            self.dataframe_query(query, params)
            .T.reset_index()
            .rename(columns={"index": "responsibility", 0: "count"})
        )

        return df

    def most_common_med_errors_responsibility(self, params):
        """
        Creates the `med_errors_responsibility_counts` table
        and returns the first row as a tuple.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            tuple: most common responsibility and count of errors that are related
        """
        df = self.med_errors_responsibility_counts(params).fillna(0)
        return df.iloc[0][0], df.iloc[0][1]

    def rn_assessment_following_burn_count(self, params):
        """
        Sum of the assessment_rn column from burns for burns occurring during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: Burns with an RN assessment as a follow up
        """
        query = """
        SELECT SUM(assessment_rn)
        FROM burns
        WHERE date_time_occurred BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def high_risk_med_error_count(self, params):
        """
        Finds descriptions that contain a high risk medication and returns the count
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of high risk medication related med errors
        """
        query = """SELECT description
        FROM med_errors
        WHERE date_time_occurred BETWEEN ? AND ?;"""

        df = self.dataframe_query(query, params)
        return df["description"].str.contains("insulin").sum()

    def major_harm_percent(self, params, incident_table):
        """
        Percent of incidents during period where the severity is equal to Major Harm or Death.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident (only Falls or Med Errors)

        Returns:
            float: Percent of incidents during period where
                the severity is equal to Major Harm or Death.
        """
        num_incidents = self.total_incidents(params, incident_table)

        if num_incidents == 0:
            return 0

        return round(
            (
                self.major_harm_or_death_count(params, incident_table)
                / num_incidents
                * 100
            ),
            2,
        )

    def adjusted_per_100MM(self, params, incident_table):
        """
        Count of incidents during period minus any incidents by ppts with an
        incidents count greater than the mean plus 3 standard deviations divided
        by the member months in the period multiplied by 100

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            float: adjusted count of incidents per 100 member months
        """
        enrollment = Enrollment(self.db_filepath)

        return round(
            (
                self.adjusted_incident_count(params, incident_table)
                / enrollment.member_months(params)
                * 100
            ),
            2,
        )

    def unstageable_wound_percent(self, params):
        """
        percent of wounds with a date_time_occurred during the period
        and an ulcer stage of Unstageable.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent wounds that are unstageable
        """
        num_incidents = self.total_incidents(params, "wounds")

        if num_incidents == 0:
            return 0

        return round(self.unstageable_wounds_count(params) / num_incidents, 2)

    def third_degree_burn_rate(self, params):
        """
        Rate of burns during period with a burn_degree of Third or Fourth.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: rate of 3rd and 4th degree burns
        """
        num_incidents = self.total_incidents(params, "burns")

        if num_incidents == 0:
            return 0

        return round(self.third_degree_burn_count(params) / num_incidents, 2)

    def rn_assessment_following_burn_percent(self, params):
        """
        Sum of the assessment_rn column from burns for burns occurring during the period
        divided by the total burns in the period multiplied by 100

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of burns with an RN assessment as a follow up
        """
        num_incidents = self.total_incidents(params, "burns")

        if num_incidents == 0:
            return 0

        return round(
            self.rn_assessment_following_burn_count(params) / num_incidents * 100, 2
        )
