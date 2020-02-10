from paceutils.helpers import Helpers
from paceutils.enrollment import Enrollment


class Demographics(Helpers):
    """This is a class for running demographics related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def dual_count(self, params):
        """
        Counts the number of ppts enrolled in the program
        during the period that have both Medicare and medicaid

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of rows with 1 in Medicare and medicaid column
        """

        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 1
        AND medicaid = 1;"""

        return self.single_value_query(query, params)

    def percent_dual(self, params):
        """
        Calculates the percent of ppts enrolled in the program
        during the period that have both Medicare and medicaid

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of ppts in period with both Medicare and medicaid
        """

        enrollment = Enrollment(self.db_filepath)
        return (
            round(self.dual_count(params) / enrollment.census_during_period(params), 2)
            * 100
        )

    def medicare_only_count(self, params):
        """
        Counts the number of ppts enrolled in the program
        during the period that have Medicare, but not medicaid

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of rows with 1 in Medicare column and 0 in medicaid column
        """

        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 1
        AND medicaid = 0;"""

        return self.single_value_query(query, params)

    def percent_medicare_only(self, params):
        """
        Calculates the percent of ppts enrolled in the program
        during the period that have Medicare only

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of ppts in period with Medicare only
        """

        enrollment = Enrollment(self.db_filepath)
        return (
            round(
                self.medicare_only_count(params)
                / enrollment.census_during_period(params),
                2,
            )
            * 100
        )

    def medicaid_only_count(self, params):
        """
        Counts the number of ppts enrolled in the program
        during the period that have medicaid, but not Medicare

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of rows with 1 in medicaid column and 0 in medicare column
        """

        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 0
        AND medicaid = 1;"""

        return self.single_value_query(query, params)

    def percent_medicaid_only(self, params):
        """
        Calculates the percent of ppts enrolled in the program
        during the period that have medicaid only

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of ppts in period with medicaid only
        """

        enrollment = Enrollment(self.db_filepath)
        return (
            round(
                self.medicaid_only_count(params)
                / enrollment.census_during_period(params),
                2,
            )
            * 100
        )

    def private_pay_count(self, params):
        """
        Counts the number of ppts enrolled in the program
        during the period that don't have medicaid or medicare

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: count of rows with 0 in both the medicaid and medicare column
        """

        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 0
        AND medicaid = 0;"""

        return self.single_value_query(query, params)

    def percent_private_pay(self, params):
        """
        Calculates the percent of ppts enrolled in the program
        during the period that don't have medicaid or medicare

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of ppts in period without medicaid and medicare
        """

        enrollment = Enrollment(self.db_filepath)
        return (
            round(
                self.private_pay_count(params)
                / enrollment.census_during_period(params),
                2,
            )
            * 100
        )

    def avg_age(self, params):
        """
        Average age at the end date of ppts enrolled during the period
        at the time the period ends

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: average age of ppts
        """

        params = list(params) + [params[1]]

        query = """SELECT ROUND(
            AVG(
                (julianday(?) - julianday(d.dob)) / 365.25
            ), 2)
        FROM demographics d
        JOIN enrollment e on d.member_id = e.member_id
        WHERE (e.disenrollment_date >= ?
        OR e.disenrollment_date IS NULL)
        AND e.enrollment_date <= ?;
        """
        return self.single_value_query(query, params)

    def age_below_65(self, params):
        """
        Number of ppts below the age of 65 at the end date of ppts enrolled
        during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: number of ppts below the age of 65
        """

        params = [params[1]] + list(params)

        query = """SELECT COUNT(DISTINCT(e.member_id)),
        ((julianday(?) - julianday(d.dob)) / 365.25) as age
        FROM demographics d
        JOIN enrollment e on d.member_id = e.member_id
        WHERE (e.disenrollment_date >= ?
        OR e.disenrollment_date IS NULL)
        AND e.enrollment_date <= ?
        AND age < 65
        """
        return self.single_value_query(query, params)

    def percent_age_below_65(self, params):
        e = Enrollment(self.db_filepath)
        return round(
            self.age_below_65(params) / e.census_during_period(params) * 100, 2
        )

    def age_above_65(self, params):
        """
        Number of ppts above the age of 65 at the end date of ppts enrolled
        during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: number of ppts above the age of 65
        """

        enrollment = Enrollment(self.db_filepath)
        return enrollment.census_during_period(params) - self.age_below_65(params)

    def percent_primary_non_english(self, params):
        """
        Percent of ppts who's primary language is not english, enrolled during the period

        Assigns 1 to the case where language is not English and divides the sum of this by
        the total number of rows in demographics for enrolled ppts

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts who's primary language is not english
        """

        query = """
            SELECT ROUND(
                SUM(
                    CASE when d.language != 'English' then 1 else 0 end) * 100.00 / 
                    count(*), 2)
            FROM demographics d
            JOIN enrollment e ON d.member_id = e.member_id
            WHERE (e.disenrollment_date >= ? OR
            e.disenrollment_date IS NULL)
            AND e.enrollment_date <= ?
            """

        return self.single_value_query(query, params)

    def percent_non_white(self, params):
        """
        Percent of ppts who's race is not caucasian, enrolled during the period

        Assigns 1 to the case where language is not Caucasian/White and divides the sum of this by
        the total number of rows in demographics for enrolled ppts

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts who's race is not caucasian
        """

        query = """
            SELECT ROUND(
                SUM(
                    CASE when d.race != 'Caucasian/White' then 1 else 0 end) * 100.00 / 
                    count(*), 2)
            FROM demographics d
            JOIN enrollment e ON d.member_id = e.member_id
            WHERE (e.disenrollment_date >= ? OR
            e.disenrollment_date IS NULL)
            AND e.enrollment_date <= ?
            """

        return self.single_value_query(query, params)

    def female_count(self, params):
        """
        Number of participants with a gender of female, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: Number of female ppts
        """

        query = """
            SELECT COUNT(d.member_id)
            FROM demographics d
            JOIN enrollment e ON d.member_id = e.member_id
            WHERE (e.disenrollment_date >= ? OR
            e.disenrollment_date IS NULL)
            AND e.enrollment_date <= ?
            AND d.gender = 1;
            """

        return self.single_value_query(query, params)

    def percent_female(self, params):
        e = Enrollment(self.db_filepath)
        return round(
            self.female_count(params) / e.census_during_period(params) * 100, 2
        )

    def behavorial_dx_count(self, params):
        """
        Percent of ppts with a behavioral health related diagnosis, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts with a behavioral health related diagnosis
        """

        query = f"""
            SELECT COUNT(DISTINCT(dx.member_id)) FROM dx
            JOIN enrollment e ON dx.member_id=e.member_id
            WHERE (instr(icd10, 'F2') > 0
                OR instr(icd10, 'F31') > 0
                OR instr(icd10, 'F32') > 0
                OR instr(icd10, 'F33') > 0
                OR instr(icd10, 'F4') > 0
                OR instr(icd10, 'F6') > 0)
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND (enrollment_date <= ?)
            """

        return self.single_value_query(query, params)

    def behavorial_dx_percent(self, params):
        e = Enrollment(self.db_filepath)
        return round(
            (self.behavorial_dx_count(params) / e.census_during_period(params)) * 100, 2
        )

    def dementia_dx_count(self, params):
        """
        Percent of ppts with a demantia related diagnosis, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts with a demantia related diagnosis
        """

        query = f"""
            SELECT COUNT(DISTINCT(dx.member_id)) FROM dx
            JOIN enrollment e ON dx.member_id=e.member_id
            WHERE (instr(icd10, 'F01.50') > 0
                OR instr(icd10, 'F01.51') > 0
                OR instr(icd10, 'F02.80') > 0
                OR instr(icd10, 'F02.81') > 0
                OR instr(icd10, 'F03.90') > 0
                OR instr(icd10, 'F03.91') > 0
                OR instr(icd10, 'F10.27') > 0
                OR instr(icd10, 'F10.97') > 0
                OR instr(icd10, 'F13.27') > 0
                OR instr(icd10, 'F13.97') > 0
                OR instr(icd10, 'F18.17') > 0
                OR instr(icd10, 'F18.27') > 0
                OR instr(icd10, 'F18.97') > 0     
                OR instr(icd10, 'F19.27') > 0
                OR instr(icd10, 'F19.97') > 0                                  
                OR instr(icd10, 'G31.09') > 0
                OR instr(icd10, 'G31.83') > 0
                OR instr(icd10, 'G30.00') > 0
                OR instr(icd10, 'G30.10') > 0
                OR instr(icd10, 'G30.08') > 0
                OR instr(icd10, 'G30.09') > 0
                )
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND (enrollment_date <= ?)
            """

        return self.single_value_query(query, params)

    def at_least_one_chronic_condition_count(self, params):
        """
        Number of ppts with a chronic condition, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: Number of ppts with a chronic condition
        """
        query = f"""
            SELECT COUNT(DISTINCT(dx.member_id)) FROM dx
            JOIN enrollment e ON dx.member_id=e.member_id
            WHERE (instr(icd10, 'G30') > 0
                OR instr(icd10, 'J4') > 0
                OR instr(icd10, 'I70') > 0
                OR instr(icd10, 'C0') > 0
                OR instr(icd10, 'C1') > 0
                OR instr(icd10, 'C2') > 0
                OR instr(icd10, 'C3') > 0
                OR instr(icd10, 'C4') > 0
                OR instr(icd10, 'C5') > 0
                OR instr(icd10, 'C6') > 0
                OR instr(icd10, 'C7') > 0
                OR instr(icd10, 'C8') > 0
                OR instr(icd10, 'C9') > 0
                OR instr(icd10, 'I6') > 0
                OR instr(icd10, 'K70') > 0
                OR instr(icd10, 'K73') > 0
                OR instr(icd10, 'K74') > 0
                OR instr(icd10, 'E10') > 0
                OR instr(icd10, 'E11') > 0
                OR instr(icd10, 'E13') > 0
                OR instr(icd10, 'I10') > 0
                OR instr(icd10, 'I12') > 0
                OR instr(icd10, 'I15') > 0
                OR instr(icd10, 'I0') > 0
                OR instr(icd10, 'I11') > 0
                OR instr(icd10, 'I13') > 0
                OR instr(icd10, 'I2') > 0
                OR instr(icd10, 'I3') > 0
                OR instr(icd10, 'I4') > 0
                OR instr(icd10, 'I50') > 0
                OR instr(icd10, 'I51') > 0
                OR instr(icd10, 'N00') > 0
                OR instr(icd10, 'N01') > 0
                OR instr(icd10, 'N02') > 0
                OR instr(icd10, 'N03') > 0
                OR instr(icd10, 'N04') > 0
                OR instr(icd10, 'N05') > 0
                OR instr(icd10, 'N06') > 0
                OR instr(icd10, 'N07') > 0
                OR instr(icd10, 'N17') > 0
                OR instr(icd10, 'N18') > 0
                OR instr(icd10, 'N19') > 0
                OR instr(icd10, 'N25') > 0
                OR instr(icd10, 'N26') > 0
                OR instr(icd10, 'N27') > 0
                OR instr(icd10, 'N71') > 0
                OR instr(icd10, 'N72') > 0
                OR instr(icd10, 'N73') > 0
                OR instr(icd10, 'N74') > 0
                OR instr(icd10, 'N75') > 0
                OR instr(icd10, 'N76') > 0
                OR instr(icd10, 'N77') > 0
                OR instr(icd10, 'N8') > 0
                OR instr(icd10, 'N9') > 0
                )
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND (enrollment_date <= ?)
            """

        return self.single_value_query(query, params)

    def chronic_condition_df(self, params):
        """
        Count of chronic conditions for each ppts, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: columns: member_id (int), count of chronic conditions (int)
        """

        query = f"""
            SELECT dx.member_id,
            COUNT(DISTINCT(substr(dx.icd10, 0, instr(dx.icd10, '.')))) as count
            FROM dx
            JOIN enrollment e ON dx.member_id=e.member_id
            WHERE (instr(icd10, 'G30') > 0
                OR instr(icd10, 'J4') > 0
                OR instr(icd10, 'I70') > 0
                OR instr(icd10, 'C0') > 0
                OR instr(icd10, 'C1') > 0
                OR instr(icd10, 'C2') > 0
                OR instr(icd10, 'C3') > 0
                OR instr(icd10, 'C4') > 0
                OR instr(icd10, 'C5') > 0
                OR instr(icd10, 'C6') > 0
                OR instr(icd10, 'C7') > 0
                OR instr(icd10, 'C8') > 0
                OR instr(icd10, 'C9') > 0
                OR instr(icd10, 'I6') > 0
                OR instr(icd10, 'K70') > 0
                OR instr(icd10, 'K73') > 0
                OR instr(icd10, 'K74') > 0
                OR instr(icd10, 'E10') > 0
                OR instr(icd10, 'E11') > 0
                OR instr(icd10, 'E13') > 0
                OR instr(icd10, 'I10') > 0
                OR instr(icd10, 'I12') > 0
                OR instr(icd10, 'I15') > 0
                OR instr(icd10, 'I0') > 0
                OR instr(icd10, 'I11') > 0
                OR instr(icd10, 'I13') > 0
                OR instr(icd10, 'I2') > 0
                OR instr(icd10, 'I3') > 0
                OR instr(icd10, 'I4') > 0
                OR instr(icd10, 'I50') > 0
                OR instr(icd10, 'I51') > 0
                OR instr(icd10, 'N00') > 0
                OR instr(icd10, 'N01') > 0
                OR instr(icd10, 'N02') > 0
                OR instr(icd10, 'N03') > 0
                OR instr(icd10, 'N04') > 0
                OR instr(icd10, 'N05') > 0
                OR instr(icd10, 'N06') > 0
                OR instr(icd10, 'N07') > 0
                OR instr(icd10, 'N17') > 0
                OR instr(icd10, 'N18') > 0
                OR instr(icd10, 'N19') > 0
                OR instr(icd10, 'N25') > 0
                OR instr(icd10, 'N26') > 0
                OR instr(icd10, 'N27') > 0
                OR instr(icd10, 'N71') > 0
                OR instr(icd10, 'N72') > 0
                OR instr(icd10, 'N73') > 0
                OR instr(icd10, 'N74') > 0
                OR instr(icd10, 'N75') > 0
                OR instr(icd10, 'N76') > 0
                OR instr(icd10, 'N77') > 0
                OR instr(icd10, 'N8') > 0
                OR instr(icd10, 'N9') > 0
                )
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND (enrollment_date <= ?)
            GROUP BY dx.member_id
            """

        return self.dataframe_query(query, params)

    def over_six_chronic_conditions_count(self, params):
        """
        Count of ppts with more than 6 chronic conditions, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: ppts with more than 6 chronic conditions
        """

        return self.chronic_condition_df(params).query("count >= 6").shape[0]

    def over_six_chronic_conditions_percent(self, params):
        e = Enrollment(self.db_filepath)
        return round(
            (
                self.over_six_chronic_conditions_count(params)
                / e.census_during_period(params)
                * 100
            ),
            2,
        )

    def living_in_community(self, params):
        """
        Count of ppts who are not in a SNF, enrolled during the period
        Does not account for ppts in the hospital

        Counts enrolled ppts who do not appear in a query of 
        ppts in custodial during the period.
        
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: ppts who are not in a SNF or ALF
        """

        params = list(params) + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        WHERE NOT EXISTS (SELECT 1
        FROM custodial nf
        WHERE  nf.member_id = enrollment.member_id
        AND (discharge_date > ? OR discharge_date IS NULL)
        AND admission_date < ?)
        AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?;"""

        return self.single_value_query(query, params)

    def living_in_community_percent(self, params):
        """
        Percent of ppts who are not in a SNF, enrolled during the period
        Does not account for ppts in the hospital.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of enrolled ppts who are not in a SNF
        """
        e = Enrollment(self.db_filepath)
        return round(
            self.living_in_community(params) / e.census_during_period(params) * 100, 2
        )

    def attending_day_center(self, params):
        """
        Count of ppts who are indicated to attend the day center in Cognify,
        enrolled during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: Count of ppts who are indicated to attend the day center
        """
        query = """
        SELECT COUNT(DISTINCT(cd.member_id))
        FROM center_days cd
        JOIN enrollment e
        ON cd.member_id = e.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND (enrollment_date <= ?)
        AND days != 'PRN';
        """

        return self.single_value_query(query, params)

    def percent_attending_dc(self, params):
        """
        Percent of ppts who are indicated to attend the day center in Cognify,
        enrolled during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of enrolled ppts who are indicated to attend the day center
        """
        e = Enrollment(self.db_filepath)
        return round(
            self.attending_day_center(params) / e.census_during_period(params) * 100, 2
        )
