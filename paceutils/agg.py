from paceutils.helpers import Helpers


class Agg(Helpers):
    """This is a class for running functions on agg database

    Attributes:
        db_filepath (str): path for the aggregate database
    """

    def __init__(self, db_filepath="V:\\Databases\\agg.db"):
        """Constructor for Agg class

        Args:
            db_filepath(str): path for the aggregate database
        """
        super().__init__()
        self.db_filepath = db_filepath

    def get_plot_df(self, table, col, params=(None, None)):
        """Create a pandas dataframe useful for plotting

        Args:
            table(str): table to query in the agg database
            col(str): column to query in the agg database
            params(tuple): start date, end date tuple; dates of form YYYY-MM-DD
        
        Returns:
            a pandas dataframe of with the columns (month, value). 
            Can be used to plot trends over time.
        """
        if not all(params):
            params = self.last_year()

        return self.dataframe_query(
            f"""SELECT month, {col} FROM {table}
        WHERE month BETWEEN ? and ?""",
            params,
        )

    def team_plot_df(self, table, col, params=(None, None)):
        """Create a pandas dataframe useful for plotting

        Args:
            table(str): table to query in the agg database
            col(str): column to query in the agg database
            params(tuple): start date, end date tuple; dates of form YYYY-MM-DD
        
        Returns:
            a pandas dataframe of with a month column and a column containing the
            value for each team in that month.
            Can be used to plot trends over time.
        """
        return self.get_plot_df(
            table,
            f"""none_{col} as None,
            central_{col} as Central,
            east_{col} as East,
            north_{col} as North,
            south_{col} as South""",
            params,
        )
