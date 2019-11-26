from paceutils.helpers import Helpers
from paceutils.center_enrollment import CenterEnrollment


class CenterDemographics(Helpers):
    def dual_count(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 1
        AND medicaid = 1
        AND center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def percent_dual(self, params, center):
        center_enrollment = CenterEnrollment(self.db_filepath)
        return (
            round(
                self.dual_count(params, center)
                / center_enrollment.census(params, center),
                2,
            )
            * 100
        )

    def medicare_only_count(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 1
        AND medicaid = 0
        AND center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def percent_medicare_only(self, params, center):
        center_enrollment = CenterEnrollment(self.db_filepath)
        return (
            round(
                self.medicare_only_count(params, center)
                / center_enrollment.census(params, center),
                2,
            )
            * 100
        )

    def medicaid_only_count(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 0
        AND medicaid = 1
        AND center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def percent_medicaid_only(self, params, center):
        center_enrollment = CenterEnrollment(self.db_filepath)
        return (
            round(
                self.medicaid_only_count(params, center)
                / center_enrollment.census(params, center),
                2,
            )
            * 100
        )

    def private_pay_count(self, params, center):
        params = list(params) + [center] + list(params)

        query = """SELECT COUNT(*)
        FROM enrollment
        JOIN centers on enrollment.member_id=centers.member_id
        WHERE (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
        AND enrollment_date <= ?
        AND medicare = 0
        AND medicaid = 0
        AND center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;"""

        return self.single_value_query(query, params)

    def percent_private_pay(self, params, center):
        center_enrollment = CenterEnrollment(self.db_filepath)
        return (
            round(
                self.private_pay_count(params, center)
                / center_enrollment.census(params, center),
                2,
            )
            * 100
        )

    def avg_age(self, params, center):
        params = [params[0]] + list(params) + [center] + list(params)

        query = """SELECT ROUND(
            AVG(
                (julianday(?) - julianday(d.dob)) / 365.25
            ), 2)
        FROM demographics d
        JOIN enrollment e on d.member_id = e.member_id
        JOIN center ON e.member_id=centers.member_id
        WHERE (e.disenrollment_date >= ?
        OR e.disenrollment_date IS NULL)
        AND e.enrollment_date <= ?
        AND center = ?
        AND (centers.end_date >= ? 
        OR centers.end_date IS NULL)
        AND centers.start_date <= ?;
        """

        return self.single_value_query(query, params)

    def percent_primary_non_english(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
            SELECT ROUND(
                SUM(
                    CASE when d.language != 'English' then 1 else 0 end) * 100.00 / 
                    count(*), 2)
            FROM demographics d
            JOIN enrollment e ON d.member_id = e.member_id
            JOIN centers ON e.member_id=centers.member_id
            WHERE (e.disenrollment_date >= ? OR
            e.disenrollment_date IS NULL)
            AND e.enrollment_date <= ?
            AND center = ?
            AND (centers.end_date >= ? 
            OR centers.end_date IS NULL)
            AND centers.start_date <= ?
            """

        return self.single_value_query(query, params)

    def percent_non_white(self, params, center):
        params = list(params) + [center] + list(params)

        query = """
            SELECT ROUND(
                SUM(
                    CASE when d.race != 'Caucasian/White' then 1 else 0 end) * 100.00 / 
                    count(*), 2)
            FROM demographics d
            JOIN enrollment e ON d.member_id = e.member_id
            JOIN centers ON e.member_id=centers.member_id
            WHERE (e.disenrollment_date >= ? OR
            e.disenrollment_date IS NULL)
            AND e.enrollment_date <= ?
            AND center = ?
            AND (centers.end_date >= ? 
            OR centers.end_date IS NULL)
            AND centers.start_date <= ?
            """

        return self.single_value_query(query, params)
