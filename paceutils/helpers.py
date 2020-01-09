import sqlite3
import datetime
from calendar import monthrange
import pandas as pd
from dateutil.relativedelta import relativedelta


class Helpers(object):
    """This is a class of helper functions for running 
    other functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def __init__(self, db_filepath="V:\\Databases\\PaceDashboard.db"):
        """Constructor for Helpers class

        Args:
            db_filepath(str): path for the database
        """
        self.db_filepath = db_filepath

    def single_value_query(self, q, params=""):
        """
        Function for running a query on the database
        that returns a single value

        Args:
            q(str): SQL query
            params (str or tuple): parameters for query

        Returns:
            single value of query
        """
        conn = sqlite3.connect(self.db_filepath)
        c = conn.cursor()
        val = c.execute(q, params).fetchone()
        conn.close()
        if val is None:
            return 0
        if val[0] is None:
            return 0
        return val[0]

    def fetchall_query(self, q, params=""):
        """
        Function for running a query on the database
        that returns a list of tuples
        
        Args:
            q(str): SQL query
            params (str or tuple): parameters for query

        Returns:
            list: list of tuples
        """
        conn = sqlite3.connect(self.db_filepath)
        c = conn.cursor()

        result = c.execute(q, params).fetchall()

        conn.close()
        return result

    def dataframe_query(self, q, params=None, parse_dates=None):
        """
        Function for running a query on the database
        that returns a pandas DataFrame
        
        Args:
            q(str): SQL query
            params (str or tuple): parameters for query

        Returns:
            DataFrame: pandas DataFrame
        """
        conn = sqlite3.connect(self.db_filepath)
        df = pd.read_sql(q, conn, params=params, parse_dates=parse_dates)
        conn.close()
        return df

    def loop_plot_df(
        self, indicator_func, params=(None, None), freq="MS", additional_func_args=None
    ):
        """
        Function for running a function with monthly or quarterly params
        over the period of params. Returns this as a pandas Dataframe, useful
        for plotting.

        Args:
            indicator_func: python function
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            freq: "MS" or "QS" frequency for grouping the data
            additional_func_args: optional parameter because some function
                require additional parameters

        Returns:
            DataFrame: pandas DataFrame with columns 'Month' and 'Value'
                Month dates are the first of the month or quarter.
        """
        if not all(params):
            start_date = (
                pd.to_datetime("today") - pd.offsets.MonthBegin(2)
            ) - pd.DateOffset(years=1)
            end_date = pd.to_datetime("today") - pd.offsets.MonthEnd(1)
        else:
            start_date, end_date = params

        count_dict = {}
        if freq == "QS":
            month_move = 3
        else:
            month_move = 1

        if additional_func_args is None:
            for month_start in pd.date_range(start_date, end_date, freq=freq):
                month_end = month_start + pd.offsets.MonthEnd(month_move)
                params = [
                    month_start.strftime("%Y-%m-%d"),
                    month_end.strftime("%Y-%m-%d"),
                ]
                count_dict[month_start.strftime("%Y-%m-%d")] = indicator_func(params)
        else:
            for month_start in pd.date_range(start_date, end_date, freq=freq):
                month_end = month_start + pd.offsets.MonthEnd(month_move)
                params = [
                    month_start.strftime("%Y-%m-%d"),
                    month_end.strftime("%Y-%m-%d"),
                ]
                count_dict[month_start.strftime("%Y-%m-%d")] = indicator_func(
                    params, *additional_func_args
                )

        plot_df = pd.DataFrame.from_dict(count_dict, orient="index").reset_index()
        plot_df.rename(columns={"index": "Month", 0: "Value"}, inplace=True)
        plot_df.fillna(0, inplace=True)

        return plot_df

    def create_plot_df(self, table, date_col, summary_type, additional_filter=""):
        # here incase we a real slow load, but %timeit says this and
        # loop plot_df take the same amount of time ¯\_(ツ)_/¯
        conn = sqlite3.connect(self.db_filepath)

        if summary_type == "percent":
            plot_df = self.create_plot_df(table, date_col, "count", additional_filter)
            total_df = self.create_plot_df(table, date_col, "count", "")
            plot_df["Value"] = plot_df["Value"] / total_df["Value"] * 100
            return plot_df

        query = f"""
            SELECT member_id, {date_col} from {table}
            WHERE {date_col} BETWEEN date('now','start of month', '-1 year', '-1 month')
            AND date('now','start of month', '-1 day')
            {additional_filter};
            """

        df = pd.read_sql(query, conn, parse_dates=[date_col])
        df["month"] = pd.to_datetime(df[date_col]).dt.to_period("M")

        if summary_type == "pmpm":
            census_q = """SELECT month, total FROM monthly_census
            WHERE month BETWEEN date('now','start of month', '-1 year', '-1 month')
            AND date('now','start of month', '-1 day');"""
            census_df = pd.read_sql(census_q, conn, parse_dates=["month"])

            plot_df = df.groupby("month").count().reset_index()
            plot_df["member_id"] = plot_df["member_id"] / census_df["total"] * 100

        if summary_type == "sum":
            plot_df = df.groupby("month").sum().reset_index()

        if summary_type == "avg":
            plot_df = df.groupby("month").mean().reset_index()

        if summary_type == "count":
            plot_df = df.groupby("month").count().reset_index()

        plot_df.reset_index(inplace=True)
        plot_df.rename(columns={"month": "Month", "member_id": "Value"}, inplace=True)
        plot_df.drop(
            [col for col in plot_df.columns if col not in ["Month", "Value"]],
            axis=1,
            inplace=True,
        )
        conn.close()

        return plot_df

    def month_to_date(self):
        """
        Gets the start of the current month and today to return
        as parameters
        
        Returns:
            tuple: start_date, end_date
        """
        start_date = datetime.datetime.now().replace(day=1)
        end_date = datetime.datetime.now()

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def last_month(self):
        """
        Gets the start and end date of the last month from today
        
        Returns:
            tuple: start_date, end_date
        """
        today = datetime.datetime.now().replace(day=1)
        start_date = today - relativedelta(months=1)
        end_date = today - datetime.timedelta(days=1)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def last_three_months(self):
        """
        Gets the start and end date of the last 3-month period from today
        
        Returns:
            tuple: start_date, end_date
        """
        today = datetime.datetime.now().replace(day=1)
        start_date = today - relativedelta(months=3)
        end_date = today - datetime.timedelta(days=1)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def last_six_months(self):
        """
        Gets the start and end date of the last 6-month period from today
        
        Returns:
            tuple: start_date, end_date
        """
        today = datetime.datetime.now().replace(day=1)
        start_date = today - relativedelta(months=6)
        end_date = today - datetime.timedelta(days=1)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def last_year(self):
        """
        Gets the start and end date of the last year period from today
        
        Returns:
            tuple: start_date, end_date
        """
        today = datetime.datetime.now().replace(day=1)
        start_date = today - relativedelta(months=12)
        end_date = today - datetime.timedelta(days=1)

        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def get_quarter_dates(self, q, yr):
        """
        Gets the start and end date of the given quarter
        
        Args:
            q(int): quarter number; 1,2,3,4
            yr(int): year of the quarter
        Returns:
            tuple: start_date, end_date
        """
        q_month_start = 3 * q - 2
        q_month_end = 3 * q

        q_month_start_str = str(q_month_start)
        q_month_end_str = str(q_month_end)

        if len(q_month_start_str) == 1:
            q_month_start_str = "0" + q_month_start_str

        if len(q_month_end_str) == 1:
            q_month_end_str = "0" + q_month_end_str

        start_date = f"{yr}-{q_month_start_str}-01"
        end_date = f"{yr}-{q_month_end_str}-{monthrange(yr, q_month_end)[1]}"

        return start_date, end_date

    def last_quarter(self, return_q=False):
        """
        Gets the start and end date of the last year quarter from today
        Can also return the q, yr of the last quarter.
        
        Args:
            return_q(bool): True if function should return the quarter and year

        Returns:
            tuple: start_date, end_date if return_q is False, (q, yr) if return_q is True
        """
        today = datetime.datetime.now().replace(day=1)
        q = (today.month - 1) // 3 + 1
        yr = today.year

        if q == 1:
            last_q = 4
            yr = yr - 1
        else:
            last_q = q - 1

        if return_q:
            return last_q, yr

        return self.get_quarter_dates(last_q, yr)

    def quarter_to_date(self):
        """
        Gets the start of the current quarter and today to return
        as parameters
        
        Returns:
            tuple: start_date, end_date
        """
        today = datetime.datetime.now().replace(day=1)
        q = (today.month - 1) // 3 + 1
        yr = today.year

        return self.get_quarter_dates(q, yr)

    def prev_month_dates(self, params):
        """
        Gets the start and end dates of the month prior to the
        given parameter dates.
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            tuple: start_date, end_date
        """
        param_0 = datetime.datetime.strptime(params[0], "%Y-%m-%d") - relativedelta(
            months=1
        )
        param_1 = datetime.datetime.strptime(params[1], "%Y-%m-%d") - relativedelta(
            months=1
        )

        return param_0.strftime("%Y-%m-%d"), param_1.strftime("%Y-%m-%d")

    def prev_quarter_dates(self, params):
        """
        Gets the start and end dates of the quarter prior to the
        given parameter dates.
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'

        Returns:
            tuple: start_date, end_date
        """
        param_0 = datetime.datetime.strptime(params[0], "%Y-%m-%d") - relativedelta(
            months=3
        )
        param_1 = datetime.datetime.strptime(params[1], "%Y-%m-%d") - relativedelta(
            months=3
        )

        return param_0.strftime("%Y-%m-%d"), param_1.strftime("%Y-%m-%d")
