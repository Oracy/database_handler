import json

import pandas as pd
import pymssql


class DatabaseHandler:
    def __init__(self, access_information: json) -> None:
        self._host = access_information["host"]
        self._user = access_information["user"]
        self._password = access_information["password"]
        self._connection = self._connect()

    @property
    def connection(self):
        """
        Connection attribute
        """
        return self._connection

    def _connect(self):
        """
        Establish connection with the database
        """
        connection_parameters = {
            "host": self._host,
            "user": self._user,
            "password": self._password,
        }
        return pymssql.connect(
            host=connection_parameters["host"],
            user=connection_parameters["user"],
            password=connection_parameters["password"],
        )

    def _reconnect(self):
        """Reconnect to database"""
        self._connection = self._connect()

    def close(self):
        """
        Close the connection
        """
        self._connection.close()

    def cursor(self):
        """
        Create cursors
        """
        return self.connection.cursor(as_dict=True)

    def db_connector(func):
        """
        Decorator to check connection before try to run queries

        Args:
            func: Database related function whuch uses DatabaseHandler connection.
        Returns:
            N/A
        """

        def with_connection(self, *args, **kwargs):
            self._reconnect()
            try:
                result = func(self, *args, **kwargs)
            except Exception as error:
                print(f"Error: {error}")
            return result

        return with_connection

    @db_connector
    def fetch(self, query, params=None, max_tries=5):
        """
        Fetch query results using query

        Args:
            query: Query to run at database
            params: Query params
            max_tries: Max number of query retries before broke
        Returns:
            Query results as a list of dicts
        """
        attempt_no = 0
        while attempt_no < max_tries:
            attempt_no += 1
            cursor = self.cursor()
            try:
                with self.connection:
                    with cursor:
                        cursor.execute(query, params)
                        return cursor.fetchall()
            except Exception as error:
                print(f"ERROR: In pymssql.cursor.fetchall(): {error}")
        return []

    @db_connector
    def query_to_df(self, sql, params=None, max_tries=5):
        """
        Create a pandas DataFrame object from a query result

        Args:
            sql: Query statements
            params: A list or a tuple of parameters that will be passed to the query execution
            max_tries: Max number of query retries before broke
        Returns:
            Pandas DataFrame object
        """
        attempt_no = 0
        while attempt_no < max_tries:
            cursor = self.cursor()
            attempt_no += 1
            try:
                with self.connection:
                    with cursor:
                        return pd.read_sql_query(sql, self, params=params)
            except Exception as error:
                print(f"Query to DataFrame error: {error}.")

    @staticmethod
    def load_query(path):
        with open(path, "r") as query_file:
            return query_file.read()

    @staticmethod
    def create_date_table(start, end, tz="utc", freq="Min"):
        """
        Generates a date table

        Args:
            start: start date
            end: end date
            tz: time zone
            freq: can be D(Days), Min(Minutes), or any other value specified in pandas "pd.date_range" docs
        Returns:
            Pandas DataFrame object
        """
        df = pd.DataFrame({"dimension_date": pd.date_range(start, end, freq=freq)})
        df["date_id"] = df.dimension_date.dt.strftime("%Y%m%d")
        df["dimension_timestamp_utc"] = df.dimension_date.apply(
            lambda x: pd.Timestamp(x).tz_localize("utc")
        )
        df["year"] = df.dimension_date.dt.year
        df["month"] = df.dimension_date.dt.month
        df["day"] = df.dimension_date.dt.day
        df["week"] = df.dimension_date.dt.weekofyear
        return df
