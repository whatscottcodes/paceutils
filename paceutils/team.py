from collections import defaultdict
import pandas as pd
from paceutils.helpers import Helpers
from paceutils.utilization import Utilization


class Team(Helpers):
    """This is a class for running team related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def admissions_by_team(self, params, utilization_table):
        """
        Count of admissions with an admission_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            DataFrame: admissions by team
        """
        params = list(params) + list(params)

        query = f"""
        SELECT team, count(*) as '{utilization_table} admissions'
        FROM {utilization_table} ut
        JOIN teams ON ut.member_id= teams.member_id
        WHERE admission_date BETWEEN ? and ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def er_only_visits_by_team(self, params):
        """
        Count of admissions with an admission_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            DataFrame: admissions by team
        """
        params = list(params) + list(params)

        query = f"""
        SELECT team, count(*) as 'er_only'
        FROM er_only
        JOIN teams ON er_only.member_id= teams.member_id
        WHERE admission_date BETWEEN ? and ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def days_by_team(self, params, utilization_table):
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
            DataFrame: utilization days by team
        """
        admit_in_time_params = [params[1]] + list(params) + list(params)

        during_query = f"""
        SELECT team, SUM(ifnull(julianday(discharge_date), julianday(?)) - julianday(admission_date)) as days
        FROM {utilization_table} ut
        JOIN teams ON ut.member_id= teams.member_id
        WHERE admission_date BETWEEN ? AND ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        admit_before_time_params = [params[1], params[0], params[0], params[0]] + list(
            params
        )

        before_query = f"""
        SELECT team, SUM(julianday(?) - julianday(?)) as days
        FROM {utilization_table} ut
        JOIN teams ON ut.member_id= teams.member_id
        WHERE (discharge_date >= ?
        OR discharge_date IS NULL)
        AND admission_date < ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        during_df = self.dataframe_query(during_query, admit_in_time_params)
        before_df = self.dataframe_query(before_query, admit_before_time_params)

        all_days = during_df.merge(
            before_df,
            on="team",
            how="outer",
            suffixes=("_admit_during", "_admit_before"),
        )
        all_days[[col for col in all_days.columns if col != "team"]] = all_days[
            [col for col in all_days.columns if col != "team"]
        ].fillna(0)

        all_days[f"{utilization_table } days"] = (
            all_days["days_admit_during"] + all_days["days_admit_before"]
        )
        return all_days[["team", f"{utilization_table } days"]]

    def discharges_by_team(self, params, utilization_table):
        """
        Count of admissions with a discharge_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: discharges by team
        """
        params = list(params) + list(params)

        query = f"""
        SELECT team, COUNT(*) as '{utilization_table} discharges'
        FROM {utilization_table} ut
        JOIN teams ON ut.member_id= teams.member_id
        WHERE discharge_date BETWEEN ? AND ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def alos_for_discharges_by_team(self, params, utilization_table):
        """
        Average los (length of stay) for admissions with a discharge_date in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            float: average length of stay by team
        """
        params = list(params) + list(params)

        query = f"""
        SELECT team, ROUND(AVG(los), 2) as '{utilization_table} alos'
        FROM {utilization_table} ut
        JOIN teams ON ut.member_id= teams.member_id
        WHERE discharge_date BETWEEN ? AND ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def readmits_by_team(self, params, utilization_table="acute", days_since=30):
        """
        Count of admissions during the period with a day_since_last_admission value below 30.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization

        Returns:
            int: 30-day readmissions by team
        """
        params = list(params) + [days_since] + list(params)

        query = f"""SELECT team, COUNT(*) as '{utilization_table} {days_since} day readmits'
        FROM {utilization_table} ut
        JOIN teams ON ut.member_id= teams.member_id
        WHERE admission_date BETWEEN ? AND ?
        AND days_since_last_admission <= ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def avg_age_by_team(self, params):
        """
        Average age at the end date of ppts enrolled
        during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: average age of ppts by team
        """
        params = [params[0]] + list(params) + list(params)

        query = """SELECT team, ROUND(
            AVG(
                (julianday(?) - julianday(d.dob)) / 365.25
            ), 2) as age
        FROM demographics d
        JOIN enrollment e ON d.member_id= e.member_id
        JOIN teams ON e.member_id= teams.member_id
        WHERE (e.disenrollment_date >= ?
        OR e.disenrollment_date IS NULL)
        AND e.enrollment_date <= ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def percent_primary_non_english_by_team(self, params):
        """
        Percent of ppts who's primary language is not English, enrolled during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts who's primary language is not English by team
        """
        params = list(params) + list(params)

        query = """
            SELECT team, ROUND(
                SUM(
                    CASE when d.language != 'English' then 1 else 0 end) * 100.00 / 
                    count(*), 2) as 'percent non english'
            FROM demographics d
            JOIN enrollment e ON d.member_id= e.member_id
            JOIN teams on e.member_id= teams.member_id
            WHERE (e.disenrollment_date >= ? OR
            e.disenrollment_date IS NULL)
            AND e.enrollment_date <= ?
            AND (teams.end_date >= ? 
            OR teams.end_date IS NULL)
            AND teams.start_date <= ?
            GROUP BY team
            """

        return self.dataframe_query(query, params)

    def avg_years_enrolled_by_team(self, params):
        """
        Average of the difference between today's date or the ppt's disenrollment date
        and their enrollment date divided by 326.25.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            DataFrame: average years enrolled in PACE by team
        """
        params = [params[1]] + list(params) + list(params)

        query = """
        with all_days as (select
            *, (julianday(disenrollment_date) - julianday(enrollment_date)) as days
        from
        enrollment
        JOIN teams on enrollment.member_id=teams.member_id
        where
        disenrollment_date IS NOT NULL
        UNION
        select
        *, (julianday(?) - julianday(enrollment_date)) as days
        from
        enrollment
        JOIN teams on enrollment.member_id=teams.member_id
        where
        disenrollment_date IS NULL)
        SELECT team, ROUND(AVG(days) / 365.25, 2) as avg_years_enrolled
        FROM all_days
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND (end_date >= ? 
        OR end_date IS NULL)
        AND start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def ppts_in_custodial_by_team(self, params):
        """
        Count of custodial admissions with a discharge date that is null or
        greater than the start date and an admission_date less than the end date.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: number of ppts in custodial by team
        """
        params = list(params) + list(params)

        query = """SELECT team, COUNT(*) as 'ppts in custodial' FROM custodial
        JOIN teams on custodial.member_id= teams.member_id
        WHERE (discharge_date >= ?
            OR discharge_date IS NULL)
        AND admission_date <= ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;
        """

        return self.dataframe_query(query, params)

    def ppts_on_team(self, params):
        """
        Count of ppts with an enrollment date prior to the end date
        and a disenrollment date that is null or after the start date.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            int: census over the time period indicated in the params by team
        """
        params = list(params) + list(params)

        query = """SELECT team, COUNT(*) as participants FROM teams
        JOIN enrollment ON teams.member_id= enrollment.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team
        """

        return self.dataframe_query(query, params)

    def mortality_by_team(self, params, total=False):
        """
        Number of deaths in period divided by the census during the period

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Rate of death by team
        """
        params = list(params) + list(params)

        query = """SELECT teams.team, COUNT(*) as deaths FROM teams
            JOIN enrollment ON teams.member_id= enrollment.member_id
            WHERE disenroll_reason = 'Deceased'
            AND disenrollment_date BETWEEN ? and ?
            AND (teams.end_date >= ? 
            OR teams.end_date IS NULL)
            AND teams.start_date <= ?
            GROUP BY team
            """

        mortality = self.dataframe_query(query, params)
        if total:
            return mortality
        team_size = self.ppts_on_team(params[:2])
        mortality = mortality.merge(team_size, on="team", how="left")
        mortality["mortality rate"] = mortality["deaths"] / mortality["participants"]

        return mortality[["team", "mortality rate"]]

    def mortality_within_30days_of_discharge_by_team_df(self, params):
        """
        Count of deaths occurring within 30 days of discharge

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Count of deaths occurring within 30 days of discharge by team
        """
        params = list(params) + list(params)

        utilization = Utilization(self.db_filepath)
        deceased_query = """SELECT enrollment.member_id,
        enrollment.disenrollment_date,
        teams.team FROM enrollment
        JOIN teams ON enrollment.member_id= teams.member_id
        WHERE disenroll_reason = 'Deceased' 
        AND disenrollment_date BETWEEN ? AND ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?;"""

        deceased_during_period = self.fetchall_query(deceased_query, params)

        within_30_of_death = defaultdict(int)
        for id_date_team in deceased_during_period:
            within_30_of_death[
                id_date_team[2]
            ] += utilization.check_for_admission_30days_before_death(*id_date_team[:2])
        if not within_30_of_death:
            within_30_of_death[None] = 0

        within_30_of_death = (
            pd.DataFrame.from_dict(within_30_of_death, orient="index")
            .reset_index()
            .rename(columns={"index": "team", 0: "30day_death_discharge"})
        )

        return within_30_of_death

    def percent_of_discharges_with_mortality_in_30_by_team(self, params):
        """
        The rate of discharges that result in death within 30 days.
        This is deaths within 30 days of discharge divided by
        total hospital discharges during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of discharges resulting in death within 30 days
                by team
        """

        within_30_of_death = self.mortality_within_30days_of_discharge_by_team_df(
            params
        )

        within_30_of_death = within_30_of_death.merge(
            self.discharges_by_team(params, "acute"), on="team", how="left"
        )

        within_30_of_death["% of discharges with death within 30 days"] = (
            within_30_of_death["30day_death_discharge"]
            / within_30_of_death["acute discharges"]
        ) * 100

        return within_30_of_death[["team", "% of discharges with death within 30 days"]]

    def mortality_within_30days_of_discharge_rate_by_team(self, params):
        """
        The rate of deaths that occurred within 30 days of discharge from a hospital.
        This is deaths within 30 days of discharge divided by total deaths during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Rate of death within 30 days of discharges by team
        """
        within_30_of_death = self.mortality_within_30days_of_discharge_by_team_df(
            params
        )

        within_30_of_death = within_30_of_death.merge(
            self.mortality_by_team(params, total=True), on="team", how="left"
        )

        within_30_of_death["death within 30 days of discharge date"] = (
            within_30_of_death["30day_death_discharge"] / within_30_of_death["deaths"]
        )

        return within_30_of_death[["team", "death within 30 days of discharge date"]]

    def no_hosp_admission_since_enrollment_by_team(self, params):
        """
        Percent of ppts who do not have a recorded acute hospital admissions.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Percent of ppts who do not have a recorded acute hospital admissions
                by team.
        """
        params = list(params) + list(params)

        query = """SELECT team, COUNT(*) as no_admissions
        FROM enrollment
        JOIN teams on enrollment.member_id= teams.member_id
        WHERE NOT EXISTS (SELECT 1 
        FROM acute 
        WHERE  acute.member_id= enrollment.member_id)
        AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;"""

        df = self.dataframe_query(query, params).merge(
            self.ppts_on_team(params[:2]), on="team", how="right"
        )

        df["percent with no admissions since enrollment"] = (
            df["no_admissions"] / df["participants"] * 100
        )

        return df[["team", "percent with no admissions since enrollment"]]

    def pressure_ulcer_rate_by_team(self, params):
        """
        Pressure ulcer per member

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            float: Pressure ulcer member by team
        """
        params = list(params) + list(params)

        query = """SELECT team, COUNT(*) as pressure_wounds FROM wounds
        JOIN teams ON wounds.member_id= teams.member_id
        WHERE wound_type = 'Pressure Ulcer' 
        AND (date_healed >= ? OR
            date_healed IS NULL)
        AND date_time_occurred <= ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?;"""

        df = self.dataframe_query(query, params).merge(
            self.ppts_on_team(params[:2]), on="team", how="right"
        )
        df["Wound Rate"] = df["pressure_wounds"] / df["participants"]

        return df[["team", "Wound Rate"]]

    def total_incidents_by_team(self, params, incident_table):
        """
        Count of incidents with a date_time_occurred date during the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: count of incidents by team
        """
        params = list(params) + list(params)

        query = f"""SELECT team, COUNT(*) as {incident_table} FROM {incident_table} it
        JOIN teams on it.member_id= teams.member_id
        WHERE date_time_occurred BETWEEN ? AND ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;"""

        return self.dataframe_query(query, params)

    def incidents_per_member_by_team(self, params, incident_table):
        """
        Count of incidents with a date_time_occurred date during the period divided
            by the number of ppts on team during period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: count of incidents per member by team
        """
        df = self.total_incidents_by_team(params, incident_table).merge(
            self.ppts_on_team(params[:2]), on="team", how="right"
        )
        df[f"{incident_table} per 100 Ppts"] = (
            df[incident_table] / df["participants"]
        ) * 100

        return df[["team", f"{incident_table} per 100 Ppts"]]

    def ppts_w_incident_by_team(self, params, incident_table):
        """
        Count of distinct ppts with an incident in the period.

        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            incident_table(str): table in database to use as incident

        Returns:
            int: Count of distinct ppts with an incident in the period by team.
        """
        params = list(params) + list(params)

        query = f"""SELECT team, COUNT(DISTINCT(it.member_id)) as 'individuals w/ {incident_table}'
        FROM {incident_table} it
        JOIN teams ON it.member_id= teams.member_id
        WHERE date_time_occurred BETWEEN ? AND ?
        AND (teams.end_date >= ? 
        OR teams.end_date IS NULL)
        AND teams.start_date <= ?
        GROUP BY team;"""

        return self.dataframe_query(query, params)

    def loop_plot_team_df(
        self,
        indicator_func,
        params=(None, None),
        freq="MS",
        additional_func_args=None,
        col_suffix="",
    ):
        """
        Function for running a function with monthly or quarterly params
        over the period of params. Returns this as a pandas Dataframe, useful
        for plotting. Merges the resulting dataframes from team functions

        Args:
            indicator_func: python function
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            freq: "MS" or "QS" frequency for grouping the data
            additional_func_args: optional parameter because some function
                require additional parameters

        Returns:
            DataFrame: pandas DataFrame with columns 'Month' and columns
                for the value of each team.
                Month dates are the first of the month or quarter.
        """
        if not all(params):
            start_date = (
                pd.to_datetime("today") - pd.offsets.MonthBegin(2)
            ) - pd.DateOffset(years=1)
            end_date = pd.to_datetime("today") - pd.offsets.MonthEnd(1)
            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")
        else:
            start_date = pd.to_datetime(params[0]).strftime("%Y-%m-%d")
            end_date = pd.to_datetime(params[1]).strftime("%Y-%m-%d")

        if freq == "QS":
            month_move = 3
        else:
            month_move = 1

        df = self.ppts_on_team((start_date, end_date))
        df.drop("participants", axis=1, inplace=True)
        master_plot_df = pd.DataFrame()

        if additional_func_args is None:
            for month_start in pd.date_range(start_date, end_date, freq=freq):
                month_end = month_start + pd.offsets.MonthEnd(month_move)
                params = [
                    month_start.strftime("%Y-%m-%d"),
                    month_end.strftime("%Y-%m-%d"),
                ]
                plot_df = df.merge(indicator_func(params), on="team", how="left").T
                plot_df.columns = plot_df.loc["team"]
                plot_df.drop("team", inplace=True)
                plot_df["month"] = month_start.strftime("%Y-%m-%d")
                master_plot_df = master_plot_df.append(plot_df, sort=False)
        else:
            for month_start in pd.date_range(start_date, end_date, freq="MS"):
                month_end = month_start + pd.offsets.MonthEnd(month_move)
                params = [
                    month_start.strftime("%Y-%m-%d"),
                    month_end.strftime("%Y-%m-%d"),
                ]
                plot_df = df.merge(
                    indicator_func(params, *additional_func_args), on="team", how="left"
                ).T
                plot_df.columns = plot_df.loc["team"]
                plot_df.drop("team", inplace=True)
                plot_df["month"] = month_start.strftime("%Y-%m-%d")

                master_plot_df = master_plot_df.append(plot_df, sort=False)

        master_plot_df.columns = [
            f"{str(col).lower()}{col_suffix}" if col != "month" else col
            for col in master_plot_df.columns
        ]

        master_plot_df.fillna(0, inplace=True)
        master_plot_df.reset_index(inplace=True, drop=True)

        return master_plot_df
