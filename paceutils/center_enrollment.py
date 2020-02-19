from datetime import datetime
from paceutils.helpers import Helpers


class CenterEnrollment(Helpers):
    def census_today(self, center):
        query = """SELECT COUNT(*)
        FROM enrollment 
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE disenrollment_date IS NULL
        AND centers.center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?
        """

        return self.single_value_query(query, params=[center])

    def census_during_period(self, params, center):
        params = list(params) + [center] + list(params)
        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND centers.center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def census_on_end_date(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(DISTINCT(enrollment.member_id))
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE enrollment_date <= ?
        AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND centers.center = ?
        AND (centers.end_date >= ?
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def disenrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE disenrollment_date BETWEEN ? AND ?
        AND centers.center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?"""

        return self.single_value_query(query, params)

    def voluntary_disenrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND disenroll_type = 'Voluntary'
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?;
                    """

        return self.single_value_query(query, params)

    def enrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE enrollment_date BETWEEN ? AND ?
        AND centers.center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def deaths(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND disenroll_type = 'Deceased'
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def net_enrollment_during_period(self, params, center):
        return self.enrolled(params, center) - self.disenrolled(params, center)

    def avg_years_enrolled(self, params, center):
        params = [params[0]] + list(params) + [center] + list(params)

        query = """SELECT ROUND(
            AVG(
                (ifnull(julianday(disenrollment_date), julianday(?))
                - julianday(enrollment_date)) / 365.25
                ), 2)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE (disenrollment_date >= ?
        OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND centers.center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;
        """

        return self.single_value_query(query, params)

    def growth_rate(self, params, center):
        today = datetime.now().strftime("%Y-%m-%d")

        starting_query = f"""SELECT {center} FROM monthly_census
        WHERE month = ?
        """

        starting_census = self.single_value_query(starting_query, [params[0]])

        if today == params[1]:
            ending_census = self.census([params[1], params[1]], center)

        else:
            ending_query = f"""SELECT {center} FROM monthly_census
            WHERE month = date(?, 'start of month', '+1 month');
            """
            ending_census = self.single_value_query(ending_query, [params[1]])
        return round(((ending_census - starting_census) / starting_census) * 100, 2)

    def churn_rate(self, params, center):

        starting_query = f"""SELECT {center} FROM monthly_census
        WHERE month = ?
        """

        disenrolled_over_period = self.disenrolled(params, center)
        starting_census = self.single_value_query(starting_query, [params[0]])

        return round((disenrolled_over_period / starting_census) * 100, 2)

    def enrollment_by_town_table(self, params, center):
        params = list(params) + [center] + list(params)
        query = f"""
            SELECT ad.city as 'City/Town', COUNT(*) as 'Number of Ppts' FROM addresses ad
            JOIN enrollment e ON ad.member_id=e.member_id
            JOIN centers on e.member_id=centers.member_id
            WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND enrollment_date <= ?
            AND ad.active = 1
            AND centers.center = ?
            AND (centers.end_date >= ? 
            OR centers.end_date IS NULL)
            AND centers.start_date <= ?
            GROUP BY city
            ORDER BY 'Number of Ppts' DESC;
            """
        df = self.dataframe_query(query, params)

        df.sort_values("Number of Ppts", ascending=False, inplace=True)

        return df

    def address_mapping_df(self, center):

        enrolled_address_query = """
            SELECT (p.first || ' ' || p.last) as name, (a.address || ', ' || a.city)
            as full_address, a.lat, a.lon
            FROM addresses a
            JOIN ppts p on a.member_id=p.member_id
            JOIN enrollment e on p.member_id=e.member_id
            JOIN centers ON e.member_id=centers.member_id
            WHERE e.disenrollment_date IS NULL
            AND centers.center = ?
            GROUP BY a.member_id
            """

        disenrolled_address_query = """
            SELECT (p.first || ' ' || p.last) as name, (a.address || ', ' || a.city)
            as full_address, a.lat, a.lon
            FROM addresses a
            JOIN ppts p on a.member_id=p.member_id
            JOIN enrollment e on p.member_id=e.member_id
            JOIN centers ON e.member_id=centers.member_id
            WHERE e.disenrollment_date NOT NULL
            AND centers.center = ?
            GROUP BY a.member_id
            """
        enrolled_df = self.dataframe_query(enrolled_address_query, params=[center])
        disenrolled_df = self.dataframe_query(
            disenrolled_address_query, params=[center]
        )

        return enrolled_df, disenrolled_df

    def dual_enrolled(self, params, center):
        params = list(params) + [center] + list(params)
        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 1
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def medicare_only_enrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 0
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def medicaid_only_enrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 1
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def private_pay_enrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE enrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 0
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def dual_disenrolled(self, params, center):
        params = list(params) + [center] + list(params)
        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 1
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def medicare_only_disenrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 1
                    AND medicaid = 0
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def medicaid_only_disenrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 1
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)

    def private_pay_disenrolled(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
                    SELECT COUNT(enrollment.member_id) FROM enrollment
                    JOIN centers on enrollment.member_id=centers.member_id
                    WHERE disenrollment_date BETWEEN ? AND ?
                    AND medicare = 0
                    AND medicaid = 0
                    AND centers.center = ?
                    AND (centers.end_date >= ? 
                    OR centers.end_date IS NULL)
                    AND centers.start_date <= ?
                    """

        return self.single_value_query(query, params)
