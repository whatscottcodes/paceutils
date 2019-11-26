import datetime
from paceutils.helpers import Helpers
from paceutils.enrollment import Enrollment
from paceutils.utilization import Utilization
from paceutils.demographics import Demographics


class Quality(Helpers):
    """This is a class for running quality related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def need_pneumo_23_df(self, params):
        """
        Finds enrolled ppts who are eligible for pneumococcal vaccinations
        and need the Pneumococcal 23
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: returns a list of ppts who need the Pneumococcal 23 vaccination
        """
        params = list(params) + [params[1]]
        query = """SELECT e.member_id, last, first, team, center, enrollment_date,
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM enrollment e
        JOIN ppts p on e.member_id = p.member_id
        LEFT JOIN demographics d on p.member_id = d.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND e.member_id NOT IN (
        SELECT member_id
        FROM pneumo 
        WHERE vacc_series = 'Pneumococcal 23'
        AND dose_status=1);"""

        return self.dataframe_query(query, params)

    def need_pcv_13_df(self, params):
        """
        Finds enrolled ppts who are eligible for pneumococcal vaccinations
        and need the PCV 13

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: returns a list of ppts who need the PCV 13 vaccination
        """
        params = list(params) + [params[1]]

        query = """SELECT e.member_id, last, first, team, center, enrollment_date,
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM enrollment e
        JOIN ppts p on e.member_id = p.member_id
        LEFT JOIN demographics d on p.member_id = d.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND e.member_id NOT IN (
        SELECT member_id
        FROM pneumo 
        WHERE vacc_series = 'PCV 13'
        AND dose_status=1);
        """

        return self.dataframe_query(query, params)

    def need_pneumo_23_only_df(self, params):
        """
        Finds enrolled ppts who are eligible for pneumococcal vaccinations
        and need the Pneumococcal 23, but not PCV 13

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: returns a list of ppts who need the Pneumococcal 23, but not PCV 13
        """
        params = list(params) + [params[1]]

        query = """SELECT e.member_id, last, first, team, center, enrollment_date,
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM enrollment e
        JOIN ppts p on e.member_id = p.member_id
        LEFT JOIN demographics d on p.member_id = d.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND e.member_id NOT IN (
        SELECT member_id
        FROM pneumo 
        WHERE vacc_series = 'Pneumococcal 23'
        AND dose_status=1)
        AND e.member_id IN (SELECT member_id
        FROM pneumo 
        WHERE vacc_series = 'PCV 13'
        AND dose_status=1);"""

        return self.dataframe_query(query, params)

    def need_pcv_13_only_df(self, params):
        """
        Finds enrolled ppts who are eligible for pneumococcal vaccinations
        and need the PCV 13, but not Pneumococcal 23

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: returns a list of ppts who need the PCV 13, but not Pneumococcal 23
        """
        params = list(params) + [params[1]]

        query = """SELECT e.member_id, last, first, team, center, enrollment_date,
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM enrollment e
        JOIN ppts p on e.member_id = p.member_id
        LEFT JOIN demographics d on p.member_id = d.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND e.member_id NOT IN (
        SELECT member_id
        FROM pneumo 
        WHERE vacc_series = 'PCV 13'
        AND dose_status=1)
        AND e.member_id IN (SELECT member_id 
        FROM pneumo 
        WHERE vacc_series = 'Pneumococcal 23'
        AND dose_status=1);
        """

        return self.dataframe_query(query, params)

    def need_both_pneumo_vaccs_df(self, params, include_refused=True):
        """
        Finds enrolled ppts who are eligible for pneumococcal vaccinations
        and need both PCV 13 and Pneumococcal 23

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: returns a list of ppts who need PCV 13 and Pneumococcal 23
        """
        params = list(params) + [params[1]]

        if include_refused:
            query = """SELECT e.member_id, last, first, team, center, enrollment_date,
            ((julianday(?) - julianday(d.dob)) / 365.25) as age
            FROM enrollment e
            JOIN ppts p on e.member_id = p.member_id
            LEFT JOIN demographics d on p.member_id = d.member_id
            JOIN centers ON d.member_id=centers.member_id
            JOIN teams ON centers.member_id=teams.member_id
            WHERE age >= 65
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND enrollment_date <= ?
            AND e.member_id NOT IN (
            SELECT member_id
            FROM pneumo 
            WHERE vacc_series = 'Pneumococcal 23'
            AND dose_status=1)
            AND e.member_id NOT IN (SELECT member_id
            FROM pneumo 
            WHERE vacc_series = 'PCV 13'
            AND dose_status=1);
            """
        else:
            query = """SELECT e.member_id, last, first, team, center, enrollment_date,
            ((julianday(?) - julianday(d.dob)) / 365.25) as age
            FROM enrollment e
            JOIN ppts p on e.member_id = p.member_id
            LEFT JOIN demographics d on p.member_id = d.member_id
            JOIN centers ON d.member_id=centers.member_id
            JOIN teams ON centers.member_id=teams.member_id
            WHERE age >= 65
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND enrollment_date <= ?
            AND e.member_id NOT IN (
            SELECT member_id
            FROM pneumo);
            """

        return self.dataframe_query(query, params)

    def has_pneumo_vacc_count(self, params):
        """
        Count of all enrolled ppts over the age of 65 who have at least one of the pneumo vaccinations.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: Count of all enrolled ppts over the age of 65
                who have at least one of the pneumo vaccinations.
        """
        params = list(params) + [params[1]]

        query = """
        SELECT COUNT(DISTINCT(pneumo.member_id)),
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM pneumo
        JOIN enrollment e ON pneumo.member_id = e.member_id
        LEFT JOIN demographics d on e.member_id = d.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND dose_status = 1;
        """

        return self.single_value_query(query, params)

    def refused_pneumo_vacc_count(self, params):
        """
        Count of all enrolled ppts over the age of 65 who have refused
        the vaccination and not received at least one of the pneumo vaccinations.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: Count of all enrolled ppts over the age of 65 who have refused
                the vaccination and not received
                at least one of the pneumo vaccinations.
        """
        params = [params[1]] + list(params)

        query = """
        SELECT COUNT(DISTINCT(pneumo.member_id)),
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM pneumo
        JOIN enrollment e ON pneumo.member_id = e.member_id
        LEFT JOIN demographics d on e.member_id = d.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND dose_status = 0
        AND e.member_id NOT IN (
            SELECT member_id
            FROM pneumo
            WHERE dose_status=1);
        """

        return self.single_value_query(query, params)

    def pneumo_rate(self, params):
        """
        Rate of all enrolled ppts over the age of 65 who have or have refused
        the vaccinations.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Rate of received or refused pneumococcal vaccination
        """
        received = self.has_pneumo_vacc_count(params)
        refused = self.refused_pneumo_vacc_count(params)

        eligible = Demographics(self.db_filepath).age_above_65(params)

        rate = (received + refused) / eligible

        return round(rate, 2)

    def refused_pneumo_vacc_df(self, params):
        """
        Finds all enrolled ppts over the age of 65 who have refused
        the vaccination and not received at least one of the pneumo vaccinations.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: ppts who have refused and not received the vaccination
        """
        params = list(params) + [params[1]]

        query = """
        SELECT pneumo.member_id, vacc_series, date_administered,
        last, first, team, center, enrollment_date,
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM pneumo
        JOIN enrollment e ON pneumo.member_id = e.member_id
        LEFT JOIN demographics d on e.member_id = d.member_id
        LEFT JOIN ppts ON d.member_id = ppts.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND dose_status = 0
        AND e.member_id NOT IN (
            SELECT member_id
            FROM pneumo
            WHERE dose_status=1);
        """

        return self.dataframe_query(query, params)

    def has_pneumo_vacc_df(self, params):
        """
        Finds all enrolled ppts over the age of 65 who have received at least one vaccinations.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: ppts who have received at least one vaccination
        """
        params = list(params) + [params[1]]

        query = """
        SELECT pneumo.member_id, vacc_series, date_administered,
        last, first, team, center, enrollment_date,
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM pneumo
        JOIN enrollment e ON pneumo.member_id = e.member_id
        LEFT JOIN demographics d on e.member_id = d.member_id
        LEFT JOIN ppts ON d.member_id = ppts.member_id
        JOIN centers ON d.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE age >= 65
        AND (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND dose_status = 1;
        """

        return self.dataframe_query(query, params).drop_duplicates(
            subset=["member_id", "vacc_series"]
        )

    def need_influenza_vacc_df(self, params):
        """
        Returns a dataframe of ppts enrolled during the period who do not have an influenza
        action (administer or refused) during the related flu season.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: returns a list of ppts who need the to be
                offered influenza vaccination
        """
        yr = datetime.datetime.now().year
        if int(params[0][5:7]) < 4:
            yr -= 1
        params = list(params) + [f"{yr}-09-01"]

        query = """SELECT DISTINCT(e.member_id), last, first,
        team, center, enrollment_date
        FROM enrollment e 
        JOIN ppts p on e.member_id = p.member_id
        JOIN centers ON p.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND e.member_id NOT IN (
        SELECT member_id
        FROM influ
        WHERE date_administered >= ?
        );"""

        return self.dataframe_query(query, params)

    def has_influ_vacc_count(self, params):
        """
        Count of all enrolled ppts who have the influenza vaccination
        during the flu period related to the parameters.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: ppts with the influenza vaccination in period
        """
        yr = datetime.datetime.now().year
        if int(params[0][5:7]) < 4:
            yr -= 1
        params = list(params) + [f"{yr}-09-01"]

        query = """SELECT COUNT(DISTINCT(influ.member_id))
        FROM influ  
        JOIN enrollment e on influ.member_id = e.member_id
        JOIN centers ON e.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND date(date_administered) >= ?
        AND dose_status = 1;"""

        return self.single_value_query(query, params)

    def has_influ_vacc_df(self, params):
        """
        Finds the list of all enrolled ppts who have the influenza vaccination
        during the flu period related to the parameters.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: ppts with the influenza vaccination in period
        """
        yr = datetime.datetime.now().year
        if int(params[0][5:7]) < 4:
            yr -= 1
        params = list(params) + [f"{yr}-09-01"]

        query = """SELECT influ.member_id, vacc_series, date_administered,
        last, first, team, center, enrollment_date
        FROM influ  
        JOIN enrollment e on influ.member_id = e.member_id
        JOIN ppts p ON e.member_id = p.member_id
        JOIN centers ON p.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND date(date_administered) >= ?
        AND dose_status = 1;"""

        return self.dataframe_query(query, params)

    def refused_influ_vacc_count(self, params):
        """
        Count of all enrolled ppts who have refused the influenza vaccination
        during the flu period related to the parameters.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: ppts who refused the influenza vaccination in period
        """
        yr = datetime.datetime.now().year
        if int(params[0][5:7]) < 4:
            yr -= 1
        params = list(params) + [f"{yr}-09-01"]

        query = """SELECT COUNT(DISTINCT(influ.member_id))
        FROM influ  
        JOIN enrollment e on influ.member_id = e.member_id
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND date(date_administered) >= ?
        AND dose_status = 0;"""

        return self.single_value_query(query, params)

    def refused_influ_vacc_df(self, params):
        """
        Finds all enrolled ppts who have refused the influenza vaccination
        during the flu period related to the parameters.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: ppts who refused the influenza vaccination in period
        """
        yr = datetime.datetime.now().year
        if int(params[0][5:7]) < 4:
            yr -= 1
        params = list(params) + [f"{yr}-09-01"]

        query = """SELECT influ.member_id, vacc_series, date_administered,
        last, first, team, center, enrollment_date
        FROM influ 
        JOIN enrollment e on influ.member_id = e.member_id
        JOIN ppts p ON e.member_id = p.member_id
        JOIN centers ON p.member_id=centers.member_id
        JOIN teams ON centers.member_id=teams.member_id
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND date(date_administered) >= ?
        AND dose_status = 0;"""

        return self.dataframe_query(query, params)

    def influ_rate(self, params):
        """
        Rate of all enrolled ppts have or have refused
        the influenza vaccinations during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Rate of recieved or refused influenza vaccination
        """
        received = self.has_influ_vacc_count(params)
        refused = self.refused_influ_vacc_count(params)

        eligible = Enrollment(self.db_filepath).census_during_period(params)

        rate = (received + refused) / eligible

        return round(rate, 2)

    def mortality_rate(self, params, total=False):
        """
        Number of deaths in period divided by the census during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Rate of death
        """
        query = """SELECT COUNT(*) FROM enrollment
            WHERE disenroll_reason = 'Deceased'
            AND disenrollment_date BETWEEN ? and ?
            """

        total = self.single_value_query(query, params)

        if total:
            return total

        census = Enrollment(self.db_filepath).census_during_period(params)

        return round(total / census, 2)

    def mortality_within_30days_of_discharge_rate(self, params):
        """
        The rate of deaths that occurred within 30 days of discharge from a hospital.
        This is deaths within 30 days of discharge divided by total deaths during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Rate of death within 30 days of discharges
        """
        utilization = Utilization(self.db_filepath)
        deceased_query = """SELECT member_id, disenrollment_date FROM enrollment
        WHERE disenroll_reason = 'Deceased'
        AND disenrollment_date BETWEEN ? AND ?;"""

        deceased_during_period = self.fetchall_query(deceased_query, params)

        total_within_30 = sum(
            [
                utilization.check_for_admission_30days_before_death(*id_date)
                for id_date in deceased_during_period
            ]
        )
        num_deceased_during_period = len(deceased_during_period)

        if num_deceased_during_period == 0:
            return 0

        return round(total_within_30 / num_deceased_during_period, 2)

    def percent_of_discharges_with_mortality_in_30(self, params):
        """
        The rate of discharges that result in death within 30 days.
        This is deaths within 30 days of discharge divided by
        total hospital discharges during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of discharges resulting in death within 30 days
        """
        utilization = Utilization(self.db_filepath)

        deceased_query = """SELECT member_id, disenrollment_date FROM enrollment
        WHERE disenroll_reason = 'Deceased'
        AND disenrollment_date BETWEEN ? AND ?;"""

        deceased_during_period = self.fetchall_query(deceased_query, params)

        total_within_30 = sum(
            [
                utilization.check_for_admission_30days_before_death(*id_date)
                for id_date in deceased_during_period
            ]
        )
        discharges_in_period = utilization.discharges_count(params)

        if discharges_in_period == 0:
            return 0

        return round(total_within_30 / discharges_in_period * 100, 2)

    def no_hosp_admission_since_enrollment(self, params):
        """
        Percent of ppts who do not have a recorded acute hospital admissions.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts who do not have a recorded acute hospital admissions.
        """
        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE NOT EXISTS (SELECT 1
        FROM acute
        WHERE  acute.member_id = enrollment.member_id)
        AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?;"""
        enrollment = Enrollment(self.db_filepath)
        return round(
            (
                self.single_value_query(query, params)
                / enrollment.census_during_period(params)
                * 100
            ),
            2,
        )

    def no_hosp_admission_last_year(self, params):
        """
        Percent of ppts who do not have a recorded acute hospital admissions
        within the year prior to the provided end date.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts who do not have an admission in period
        """
        params = [params[1]] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE NOT EXISTS (SELECT 1
        FROM acute
        WHERE acute.member_id = enrollment.member_id
        AND acute.admission_date >= date(?, '-1 years'))
        AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?;
        """

        enrollment = Enrollment(self.db_filepath)
        return round(
            (
                self.single_value_query(query, params)
                / enrollment.census_during_period(params[1:])
                * 100
            ),
            2,
        )

    def avg_days_until_nf_admission(self, params):
        """
        The average of the difference between the first admission date to a 
        custodial stay and the ppt enrollment date for ppts enrolled in the period.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Average days until first custodial admission
        """
        query = """SELECT ROUND(AVG(julianday(admission_date) - julianday(enrollment_date)), 2)
        FROM
            (
            SELECT
                member_id, MIN(admission_date) AS First
            FROM
                custodial
            GROUP BY
                member_id
            ) fca
        JOIN
        custodial c ON fca.member_id = c.member_id AND fca.First = c.admission_date
        JOIN enrollment e on c.member_id = e.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND (enrollment_date <= ?)"""

        return self.single_value_query(query, params)
