from paceutils.helpers import Helpers


class Participant(Helpers):
    """This is a class for running participant related
    functions on the database

    Attributes:
        db_filepath (str): path for the database
    """

    def utilization(self, params, utilization_table, member_id):
        """
        Creates a dataframe of all admissions during the period
        for the given utilization table related to the ppt

        ### TO DO: may want to make this ALL utilization, then
        a table does not need to be specified.
        Args:
            params (tuple): start date and end date in format 'YYYY-MM-DD'
            utilization_table(str): table in database to use as utilization
            member_id(int): member_id of ppts to look up

        Returns:
            DataFrame: all data from the utilization table related
                to the ppt in the parameter period.
        """
        params = list(params) + [member_id]

        query = f"""
        SELECT * FROM {utilization_table}
        WHERE admission_date BETWEEN ? AND ?
        AND member_id = ?;
        """

        return self.dataframe_query(query, params)

    def name(self, member_id):
        """
        Returns a list of tuples with the ppts first name and last name

        Args:
            member_id(int): member_id of ppts to look up

        Returns:
            list: [(first,), (last,)]
        """
        query = """
        SELECT first, last
        FROM ppts
        WHERE member_id = ?
        """

        return self.fetchall_query(query, [member_id])
