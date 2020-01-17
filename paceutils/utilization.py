import pandas as pd
from paceutils.helpers import Helpers
from paceutils.enrollment import Enrollment


class Utilization(Helpers):
    """This is a class for running utilization related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def admissions_count(self, params, utilization_table="acute"):
        """
        Count of admissions with an admission_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: admissions
        """
        query = f"""
        SELECT count(*) from {utilization_table}
        WHERE admission_date BETWEEN ? and ?;
        """

        return self.single_value_query(query, params)

    def discharges_count(self, params, utilization_table="acute"):
        """
        Count of admissions with a discharge_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: discharges
        """
        query = f"""
        SELECT count(*) from {utilization_table}
        WHERE discharge_date BETWEEN ? and ?;
        """

        return self.single_value_query(query, params)

    def alos(self, params, utilization_table="acute"):
        """
        Average los (length of stay) for admissions with a discharge_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            float: average length of stay
        """
        query = f"""
        SELECT ROUND(AVG(los), 2) from {utilization_table}
        WHERE discharge_date BETWEEN ? and ?;
        """
        return self.single_value_query(query, params)

    def er_to_inp_rate(self, params):
        """
        Sum of the er column from the acute view divided by the sum of
        that value and the count of er_only admissions during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: rate of ER visit that become inpatient admissions
        """
        admit_query = """
        SELECT SUM(acute.er)
        FROM acute
        WHERE admission_date BETWEEN ? AND ?;
        """
        er_query = """
        SELECT COUNT(er_only.admission_date)
        FROM er_only
        WHERE admission_date BETWEEN ? AND ?;
        """

        admitted_from_er = self.single_value_query(admit_query, params)
        total_er = admitted_from_er + self.single_value_query(er_query, params)
        if total_er == 0:
            return 0
        return round(admitted_from_er / total_er, 2)

    def alos_for_er_admissions(self, params):
        """
        Average los for admissions in the acute with an 
        er value of 1 and an admission_date during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: average length of stay for ER visits that become inpatient admissions
        """
        query = """
        SELECT ROUND(AVG(los), 2)
        FROM acute
        WHERE er = 1
        AND admission_date BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def los_per_100mm(self, params, utilization_table="acute"):
        """
        Sum of los for admissions with an admission_date during the period
        divided by the sum of the first of the month census for 
        each month during the period multiplied by 100.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            float: length of stay per 100 member months
        """
        query = f"""SELECT ROUND(SUM(total_los)*100.0/SUM(census), 2) as rate
        FROM
        (SELECT total as census, month,
        strftime("%Y-%m", month) as month_year
        FROM monthly_census) as month_year_census
        LEFT JOIN
        (
        SELECT SUM({utilization_table}.los) as total_los,
        strftime("%Y-%m", admission_date) as month_year
        FROM {utilization_table}
        GROUP BY month_year
        ) as monthly_los
        ON month_year_census.month_year = monthly_los.month_year
        WHERE month BETWEEN ? AND ?;"""

        return self.single_value_query(query, params)

    def readmits_30day(self, params, utilization_table="acute"):
        """
        Count of admissions during the period with a day_since_last_admission value below 30.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: 30-day readmissions
        """
        query = f"""SELECT COUNT(*)
        FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?
        AND days_since_last_admission <= 30;
        """

        return self.single_value_query(query, params)

    def readmits_30day_rate(self, params, utilization_table="acute"):
        """
        Count of admissions during the period with a day_since_last_admission value below 30
        divided by all admissions in period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: 30-day readmission rate
        """
        try:
            rate = self.readmits_30day(
                params, utilization_table
            ) / self.admissions_count(params, utilization_table)
            return round(rate, 2)
        except ZeroDivisionError:
            return 0

    def ppts_in_utl(self, params, utilization_table):
        """
        Count of admissions with a discharge date that is null or
        greater than the start date and an admission_date less than the end date.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: number of ppts in the utilization type
        """
        query = f"""SELECT COUNT(*)
        FROM {utilization_table}
        WHERE (discharge_date >= ?
            OR discharge_date IS NULL)
        AND admission_date <= ?;"""

        return self.single_value_query(query, params)

    def ppts_in_utl_per_100MM(self, params, utilization_table):
        """
        Count of admissions with a discharge date that is null or
        greater than the start date and an admission_date less than the end date
        divided by the member months in the period multiplied by 100

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: number of ppts in the utilization type per 100 member months
        """
        e = Enrollment(self.db_filepath)
        return round(
            self.ppts_in_utl(params, utilization_table) / e.member_months(params) * 100,
            2,
        )

    def ppts_in_utl_percent(self, params, utilization_table):
        """
        Count of admissions with a discharge date that is null or
        greater than the start date and an admission_date less than the end date
        divided by the census in the period multiplied by 100

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: percent of enrolled ppts in the utilization type
        """
        e = Enrollment(self.db_filepath)
        return round(
            self.ppts_in_utl(params, utilization_table)
            / e.census_during_period(params)
            * 100,
            2,
        )

    def utilization_indicator_count(
        self,
        params,
        filter_col,
        utilization_table="acute",
        total=False,
        per_member=False,
    ):
        """
        Percent or total admissions with a 1 in a binary indicator column

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            filter_col: column to look for a 1
            utilization_table(str): table in database to use as utilization
            total(bool): If True returns total admissions, else returns percent
            of admissions or per 100 member month rate with related 1.
            per_member(bool): if True return 100 member month rate

        Returns:
            int/float: count, percent of admissions, or 100 member rate for
                admissions with filter_col value of 1
        """
        query = f"""SELECT COUNT(*)
        FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?
        AND {filter_col} = 1;"""
        if total:
            return self.single_value_query(query, params)

        if per_member:
            query = f"""SELECT COUNT(filter_count*100.0/census, 2) as rate
            FROM
            (SELECT total as census, month,
            strftime("%Y-%m", month) as month_year
            FROM monthly_census) as month_year_census
            LEFT JOIN
            (
                SELECT COUNT({utilization_table}.admission_date) as filter_count,
                strftime("%Y-%m", admission_date) as month_year
                FROM {utilization_table}
                GROUP BY month_year
            ) as monthly_admissions_filtered
            ON month_year_census.month_year = monthly_admissions_filtered.month_year
            WHERE month BETWEEN ? AND ?
            AND {filter_col} = 'Yes';
            """
            return self.single_value_query(query, params)
        admissions = self.admissions_count(params, utilization_table)

        if admissions == 0:
            return 0

        return round(self.single_value_query(query, params) / admissions, 2)

    def check_for_admission_30days_before_death(self, member_id, deceased_date):
        """
        Checks for an admission with a discharge date between the 
        decease_date and the date 30 days prior,
        returns 1 if an admission exists, 0 otherwise.

        Args:
            member_id(int): member_id of ppts
            deceased_date(str): date of YYYY-MM-DD that the ppts died

        Returns:
            int: return 1 if the ppt had an admission discharge within 30 days of death
        """
        had_admission_query = """SELECT CASE WHEN EXISTS (
        SELECT *
        FROM acute
        WHERE member_id = ?
        AND discharge_date BETWEEN ? and ?
        )
        THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
        END"""

        thirty_prior = pd.to_datetime(deceased_date) - pd.Timedelta("30 days")
        within_30_params = [member_id, thirty_prior.strftime("%Y-%m-%d"), deceased_date]

        return self.single_value_query(had_admission_query, within_30_params)

    def get_icd_10_desc(self, dx, icd10):
        """
        Looks up an ICD10 code and returns the description

        Args:
            dx(float): ICD10 code
            icd10(DataFrame): icd10 dataframe

        Returns:
            str: ICD10 description
        """
        if dx is not None:
            try:
                desc = icd10[icd10.ICD10 == str(dx).replace(".", "")]["Desc"].values[0]
                return desc
            except IndexError:
                return "None"
        else:
            return "None"

    def create_dx_desc_cols(
        self, df, dx_cols=None, icd10_path="C:\\Users\\snelson\\data\\icd10.csv"
    ):

        """
        Looks up an ICD10 code and returns the description

        ###TO DO: may move ICD10 csv to the database
        Args:
            df(DataFrame): DataFrame with ICD10 columns that need to be parsed
            icd10_path(str): path to ICD10 code to description file

        Returns:
            df(DataFrame): original dataframe with added description columns
        """
        icd10 = pd.read_csv(icd10_path)
        if dx_cols is None:
            dx_cols = [
                "diag_code1",
                "diag_code2",
                "diag_code3",
                "diag_code4",
                "principle_diag",
                "diagnosis68",
                "diagnosis69",
                "diagnosis70",
                "diagnosis71",
                "diagnosis72",
                "diagnosis73",
                "diagnosis74",
                "diagnosis75",
                "principal_dx",
                "admitting_dx",
            ]

        df_dx_cols = [col for col in df.columns if col in dx_cols]

        for col in df_dx_cols:
            df[f"{col}_desc"] = df[col].apply(self.get_icd_10_desc, icd10=icd10)
        return df

    def utilization_related_to_condition(self, params, condition, condition_abr=None):
        """
        Searches through the acute, er_only, admitting_claims, and claims_detail tables
        for any dx where the condition or condition_abr appear. Returns a pandas dataframe.
        
        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            condition(str): full string of condition ie; Congestive heart failure 
            condition_abr(str): abbreviation that is used for condition ie; CHF

        Returns:
            DataFrame: dataframe of all admissions with a related dx
        """
        params = list(params) * 3
        acute_query = """SELECT a.*, ppts.first, ppts.last,
        ac.principal_dx, ac.admitting_dx, cd.diag_code1, cd.diag_code2,
        cd.diag_code3, cd.diag_code4, cd.principle_diag, cd.diagnosis68,
        cd.diagnosis69, cd.diagnosis70, cd.diagnosis71, cd.diagnosis72,
        cd.diagnosis73, cd.diagnosis74, cd.diagnosis75
        FROM acute a
        LEFT JOIN admission_claims ac ON a.member_id = ac.member_id
        JOIN claims_detail cd ON ac.claim_id = cd.claim_id
        JOIN ppts ON cd.member_id = ppts.member_id
        WHERE admission_date BETWEEN ? AND ?
        AND ac.first_service_date BETWEEN ? AND ?
        AND cd.first_dos BETWEEN ? AND ?;"""

        acute_df = self.dataframe_query(acute_query, params)
        acute_df = self.create_dx_desc_cols(acute_df)

        dx_related_acute = acute_df[
            acute_df.apply(
                lambda row: row.astype(str)
                .str.lower()
                .str.contains(condition.lower())
                .any(),
                axis=1,
            )
        ].copy()

        dx_related_acute["visit_type"] = "inpatient"

        er_query = """SELECT er.*, ppts.first, ppts.last,
        ac.principal_dx, ac.admitting_dx, cd.diag_code1, cd.diag_code2,
        cd.diag_code3, cd.diag_code4, cd.principle_diag, cd.diagnosis68,
        cd.diagnosis69, cd.diagnosis70, cd.diagnosis71, cd.diagnosis72,
        cd.diagnosis73, cd.diagnosis74, cd.diagnosis75
        FROM er_only er
        LEFT JOIN admission_claims ac ON er.member_id = ac.member_id
        JOIN claims_detail cd ON ac.claim_id = cd.claim_id
        JOIN ppts ON cd.member_id = ppts.member_id
        WHERE admission_date BETWEEN ? AND ?
        AND ac.first_service_date BETWEEN ? AND ?
        AND cd.first_dos BETWEEN ? AND ?;"""

        er_df = self.dataframe_query(er_query, params)
        er_df = self.create_dx_desc_cols(er_df)

        dx_related_er = er_df[
            er_df.apply(
                lambda row: row.astype(str)
                .str.lower()
                .str.contains(condition.lower())
                .any(),
                axis=1,
            )
        ].copy()

        dx_related_er["visit_type"] = "er_only"

        dx_related_utl = dx_related_acute.append(dx_related_er, sort=False)

        if condition_abr is not None:
            dx_abr_acute = acute_df[
                acute_df.apply(
                    lambda row: row.astype(str)
                    .str.lower()
                    .str.contains(condition_abr.lower())
                    .any(),
                    axis=1,
                )
            ].copy()

            dx_abr_acute["visit_type"] = "inpatient"

            dx_abr_er = er_df[
                er_df.apply(
                    lambda row: row.astype(str)
                    .str.lower()
                    .str.contains(condition_abr.lower())
                    .any(),
                    axis=1,
                )
            ].copy()
            dx_abr_er["visit_type"] = "er_only"

            dx_related_utl = dx_related_utl.append(dx_abr_acute, sort=False).append(
                dx_abr_er, sort=False
            )

            dx_related_utl.reset_index(drop=True, inplace=True)

        dx_related_utl.drop_duplicates(inplace=True)

        return dx_related_utl

    def los_over_x_df(self, params, x, utilization_table):
        """
        Finds admissions with a discharge_date during the period and a los values above the provided *x*.
        
        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            x(int): LOS the stay must be as long or longer than
            utilization_table(str): table in database to use as utilization

        Returns:
            DataFrame: dataframe of all admissions with a LOS over x
        """
        params = list(params) + [x]

        query = f"""
        SELECT * FROM {utilization_table}
        WHERE (discharge_date >= ? OR discharge_date IS NULL)
        AND admission_date <= ?
        AND los >= ?;
        """

        return self.dataframe_query(query, params)

    def los_over_x_count(self, params, x, utilization_table):
        """
        Finds and counts admissions with a discharge_date during the
        period and a los values above the provided *x*.
        
        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            x(int): LOS the stay must be as long or longer than
            utilization_table(str): table in database to use as utilization

        Returns:
            int: number admissions with a LOS over x
        """
        params = list(params) + [x]

        query = f"""
        SELECT COUNT(*) FROM {utilization_table}
        WHERE discharge_date BETWEEN ? AND ?
        AND los >= ?;
        """

        return self.single_value_query(query, params)

    def days_over_x_df(self, params, x, utilization_table):
        """
        Days are calculated by subtracting the admission date from either the
        discharge date or the end date. 
        This function then returns the admissions during the period with days above *x*.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            x(int): number of days the stay must be as long or longer than
            utilization_table(str): table in database to use as utilization

        Returns:
            DataFrame: dataframe of all admissions with a number of days in period over x
        """
        params = [params[1]] + list(params) + [x]

        query = f"""
        SELECT member_id, admission_date, discharge_date, 
        ifnull(julianday(discharge_date), julianday(?)) - julianday(admission_date) as days,
        facility
        FROM {utilization_table}
        WHERE (discharge_date >= ?
        OR discharge_date IS NULL)
        AND admission_date <= ?
        AND days >= ?
        """

        return self.dataframe_query(query, params)

    def utilization_days(self, params, utilization_table):
        """
        First the days for admissions that occur during the params is calculated.
        Next the days for admissions occurring before the period are calculated using
        the start date of the period as the admissions date.
        The sum of these two values are returned as the total
        utilization days during a given period.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: days in period that ppts are in the indicated utilization
        """
        start_date, end_date = params

        query1 = [start_date, end_date, start_date, end_date]
        query2 = [start_date, start_date, start_date, end_date]
        query3 = [end_date, start_date, end_date, end_date]
        query4 = [end_date, start_date, start_date, end_date]

        all_params = query1 + query2 + query3 + query4
        query = f"""
            with all_days as (
            SELECT member_id, (julianday(discharge_date) - julianday(admission_date))+1 as days
            FROM {utilization_table}
            WHERE admission_date BETWEEN ? AND ?
            AND discharge_date BETWEEN ? AND ?
            UNION
            SELECT member_id, (julianday(discharge_date) - julianday(?))+1 as days
            FROM {utilization_table}
            WHERE admission_date < ?
            AND discharge_date BETWEEN ? AND ?
            UNION
            SELECT member_id, (julianday(?) - julianday(admission_date))+1 as days
            FROM {utilization_table}
            WHERE admission_date BETWEEN ? AND ?
            AND (discharge_date > ?
            OR discharge_date IS NULL)
            UNION
            SELECT member_id, (julianday(?) - julianday(?))+1 as days
            FROM {utilization_table}
            WHERE admission_date < ?
            AND (discharge_date > ?
            OR discharge_date IS NULL)
            )
            SELECT SUM(days)
            FROM all_days
            """
        return self.single_value_query(query, all_params)

    def days_per_100MM(self, params, utilization_table):
        """
        First the days for admissions that occur during the params is calculated.
        Next the days for admissions occurring before the period are calculated using
        the start date of the period as the admissions date.
        The sum of these two values are divided by the member months in the period.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: days in period that ppts are in the indicated utilization per 100 member months
        """
        days = self.utilization_days(params, utilization_table)

        enrollment = Enrollment(self.db_filepath)
        census = enrollment.member_months(params)

        return round((days / census) * 100, 2)

    def top_admit_reason_by_los(self, params, top_x=10):
        """
        Finds acute admissions with a discharge_date during the period,
        orders them by los and then returns the *top_x* number of rows
        from that query as a pandas dataframe.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            top_x(int): cut off for top LOS admissions

        Returns:
            DataFrame: top_x acute admissions by LOS with admit reason included
        """
        query = f"""
        SELECT admit_reason, los, facility, admission_date, visit_id FROM acute
        WHERE discharge_date BETWEEN ? AND ?
        AND admit_reason != 'None'
        ORDER BY los DESC
        LIMIT {top_x};
        """
        return self.dataframe_query(query, params)

    def top_10_er_users(self, params):
        """
        Counts the numbers of visits with an admission_date during the period
        grouped by participants and returns the top 10 rows as a pandas dataframe.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: 10 ppts with the most ER visits in the period.
        """
        query = """
        SELECT er_only.member_id, last, first, COUNT(*) as visits
        FROM er_only
        JOIN ppts on er_only.member_id=ppts.member_id
        WHERE admission_date BETWEEN ? AND ?
        GROUP BY er_only.member_id
        ORDER BY visits DESC
        LIMIT 10;    
        """
        return self.dataframe_query(query, params)

    def admissions_30day_readmit_df(self, params, utilization_table="acute"):
        """
        Returns a pandas dataframe of any acute admissions with an admission_date
        during the period and a day_since_last_admission value less than or equal to 30.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: All acute admissions that are a 30-day readmit
        """
        query = f"""
        SELECT *
        FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?
        AND days_since_last_admission <= 30
        """

        return self.dataframe_query(query, params)

    def admissions_resulting_in_30day_df(self, params, utilization_table="acute"):
        """
        Finds the admission prior to any admissions in the `acute_admissions_30day_readmit_df`.
        Returns these admissions as a pandas dataframe.

        Finds all 30-day readmits, and then find the admission with the same
        member_id and a discharge date that's difference to the
        the 30-day readmission admission date is equal to the readmissions
        days_since_last_admission value. 

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: All acute admissions that result in a 30-day readmit
        """
        query_for_30days = f"""
        SELECT member_id, admission_date, days_since_last_admission
        FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?
        AND days_since_last_admission <= 30
        """

        query_for_resulting_in_30days = f"""
        SELECT member_id, admission_date, discharge_date, facility, los, admit_reason
        FROM {utilization_table}
        WHERE member_id = ?
        AND (julianday(?) - julianday(discharge_date)) = ?;
        """

        thirty_day_readmits = self.fetchall_query(query_for_30days, params)

        visits_resulting_in_30day = pd.DataFrame(
            columns=[
                "member_id",
                "admission_date",
                "discharge_date",
                "facility",
                "los",
                "admit_reason",
            ]
        )

        for id_date_days in thirty_day_readmits:
            visits_resulting_in_30day = visits_resulting_in_30day.append(
                self.dataframe_query(query_for_resulting_in_30days, id_date_days)
            )

        return visits_resulting_in_30day

    def unique_admissions_count(self, params, utilization_table):
        """
        Returns a count of the distinct ppts with an admission during the period.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: number of ppts with an admission
        """
        query = f"""
        SELECT COUNT(DISTINCT(member_id))
        FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?;    
        """

        return self.single_value_query(query, params)

    def weekend_admissions_count(self, params, utilization_table):
        """
        Returns a count of the admissions during the period with a dow value of 'Saturday' or 'Sunday'

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: number of ppts with an admission on the weekend
        """
        query = f"""
        SELECT COUNT(*)
        FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?
        AND (dow = 'Saturday' OR dow = 'Sunday');
        """

        return self.single_value_query(query, params)

    def weekend_admission_percent(self, params, utilization_table):
        """
        Returns a count of the admissions during the period with a dow value of 'Saturday' or 'Sunday'
        divided by all admissions multiplied by 100

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            float: percent of admissions that are on the weekend
        """
        admissions = self.admissions_count(params, utilization_table)

        if admissions == 0:
            return 0

        return round(
            self.weekend_admissions_count(params, utilization_table) / admissions * 100,
            2,
        )

    def admissions_per_100MM(self, params, utilization_table):
        """
        Sum of the count of admissions with an admission_date during the period divided by the sum
        of the first of the month census for each month during the period multiplied by 100.

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            float: number of admissions per 100 member months
        """
        query = f"""SELECT ROUND(SUM(admissions)*100.0/SUM(census), 2) as rate
        FROM
        (SELECT total as census, month,
        strftime("%Y-%m", month) as month_year
        FROM monthly_census) as month_year_census
        LEFT JOIN
        (
        SELECT COUNT({utilization_table}.admission_date) as admissions,
        strftime("%Y-%m", admission_date) as month_year
        FROM {utilization_table}
        GROUP BY month_year
        ) as monthly_admissions
        ON month_year_census.month_year = monthly_admissions.month_year
        WHERE month BETWEEN ? AND ?;
        """

        return self.single_value_query(query, params)

    def nf_discharged_to_higher_loc(self, params, return_df=False):
        """
        Finds all nursing_home admissions or discharges during the period.
        Merges any discharges that have a disposition of "Nursing home or rehabilitation facility"
        with all nursing_home admissions on the discharge_date matching the admission_date.
        Filter to only include those discharges with a non-null admission_date and a new admit_reason
        of "custodial" where a previous admit_reason was not custodial
        and counts the discharges in this filtered group.
        It then sums up any discharges to "Acute care hospital or psychiatric facility".
        
        These two values are added together and returned. 

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: number of discharges to a higher level of care
        """
        query = """
        SELECT *
        FROM nursing_home
        WHERE (discharge_date >= ?
        OR discharge_date IS NULL)
        AND admission_date <= ?;
        """

        nfs = self.dataframe_query(query, params)

        to_nf = nfs[
            nfs["discharge_disposition"] == "Nursing home or rehabilitation facility"
        ][["member_id", "discharge_date", "discharge_disposition", "admit_reason"]]

        resulting_admissions = to_nf.merge(
            nfs,
            how="left",
            left_on=["member_id", "discharge_date"],
            right_on=["member_id", "admission_date"],
        )

        to_higher_nf = resulting_admissions[
            (resulting_admissions["admission_date"].notnull())
            & (resulting_admissions["admit_reason_x"] != "custodial")
            & (resulting_admissions["admit_reason_y"] == "custodial")
        ]

        to_hosp = nfs[
            nfs["discharge_disposition"]
            == "Acute care hospital or psychiatric facility"
        ].copy()

        if return_df:
            return to_higher_nf.append(to_hosp, sort=False)

        discharged_higher = to_higher_nf.shape[0] + to_hosp.shape[0]

        return discharged_higher

    def percent_nf_discharged_to_higher_loc(self, params):
        """
        Finds all nursing_home admissions or discharges during the period.
        Merges any discharges that have a disposition of "Nursing home or rehabilitation facility"
        with all nursing_home admissions on the discharge_date matching the admission_date.
        Filter to only include those discharges with a non-null admission_date and a new admit_reason
        of "custodial" where a previous admit_reason was not custodial
        and counts the discharges in this filtered group.
        It then sums up any discharges to "Acute care hospital or psychiatric facility".
        
        These two values are added together and divided by the total number of
        nursing home discharges. 

        Args:
            params(tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: percent of discharges to a higher level of care
        """

        nf_discharges = self.discharges_count(params, "nursing_home")
        if nf_discharges == 0:
            return 0
        return round(self.nf_discharged_to_higher_loc(params) / nf_discharges * 100, 2)

