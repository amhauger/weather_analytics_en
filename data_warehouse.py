import psycopg2
import pandas as pd
import datetime
import logging
import boto3

from error_strings import *
from query_strings import *
from error_types import ConnectionError, RedshiftDWError
from elastic_net_model import ElasticNetModel
from config import Config

boto3.set_stream_logger("", logging.DEBUG)
log = logging.getLogger(__name__)

earliest_weather_year = 2023
earliest_weather_month = 9
earliest_weather_day = 6


class RedshiftDW:
    host: str
    port: int
    name: str
    username: str
    password: str

    """
    DATA SETTERS
    """

    def set_region_coefficient_vals(self, m: ElasticNetModel, region_number: str) -> None:
        log.info(f"setting coeffient values in data warehouse for region {region_number}")
        coefficient_values = m.coefficients

        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(begin)
            cursor.execute(insert_region_coefficient_fields.format(
                region_number,
                coefficient_values[0],
                coefficient_values[1],
                coefficient_values[2],
                coefficient_values[3],
                coefficient_values[4],
                coefficient_values[5],
                coefficient_values[6],
                coefficient_values[7],
                coefficient_values[8],
                coefficient_values[9],
            ))
        except Exception as e:
            raise RedshiftDWError(
                data={
                    "host": self.host,
                    "port": self.port,
                    "name": self.name,
                    "username": self.username,
                    "query": insert_region_coefficient_fields.format(
                        region_number,
                        coefficient_values[0],
                        coefficient_values[1],
                        coefficient_values[2],
                        coefficient_values[3],
                        coefficient_values[4],
                        coefficient_values[5],
                        coefficient_values[6],
                        coefficient_values[7],
                        coefficient_values[8],
                        coefficient_values[9],
                    ),
                    "err": e,
                },
                message=error_setting_region_coefficients,
            )
        finally:
            cursor.close()
            self.close()

    def set_store_elastic_net_values(self, location_number: str, l1_ratio: float, alpha: float, mae: float):
        log.info(f"setting elastic net hyperparameters and mae for elastic net model for store number {location_number}")

        self.connect()
        cursor = self.connection.cursor()
        try:
            cursor.execute(begin)
            cursor.execute(update_store_en_fields.format(l1_ratio, alpha, mae, location_number))
            cursor.execute(commit)
        except Exception as e:
            raise RedshiftDWError(
                data={
                    "host": self.host,
                    "port": self.port,
                    "name": self.name,
                    "username": self.username,
                    "query": update_store_en_fields.format(l1_ratio, alpha, mae, location_number),
                    "err": e,
                },
                message=error_updating_store_hyperparameters,
            )
        finally:
            cursor.close()
            self.close()

    """
    DATA GETTERS
    """

    def get_orders_by_store_number(self, store_number: str) -> pd.DataFrame:
        log.info(f"retrieving orders for store {store_number}")
        time_period_start, time_period_end = self.get_datetimes_for_order_query()

        self.connect()
        cursor = self.connection.cursor()
        try:
            orders_df = self.get_rows(
                cursor,
                get_orders_by_store_number.format(
                    time_period_start, time_period_end, store_number
                ),
            )
        except Exception as e:
            raise RedshiftDWError(
                data={
                    "host": self.host,
                    "port": self.port,
                    "name": self.name,
                    "username": self.username,
                    "query": get_orders_by_store_number.format(store_number),
                    "err": e,
                },
                message=error_executing_store_orders_query,
            )
        finally:
            cursor.close()
            self.close()

        log.info(f"retrieved {orders_df.shape[0]} orders for store {store_number}")
        return orders_df

    def get_stores_by_zip_code(self, zipcode: str) -> pd.DataFrame:
        log.info(f"retrieving stores for the zipcode {zipcode}")
        self.connect()
        cursor = self.connection.cursor()
        try:
            store_df = self.get_rows(cursor, get_stores_by_zipcode.format(zipcode))
        except Exception as e:
            raise RedshiftDWError(
                data={
                    "host": self.host,
                    "port": self.port,
                    "name": self.name,
                    "username": self.username,
                    "query": get_stores_by_zipcode.format(zipcode),
                    "err": e,
                },
                message=error_executing_store_params_query,
            )
        finally:
            cursor.close()
            self.close()

        log.info(f"successfully retrieved stores for the zipcode {zipcode}")
        return store_df

    def get_historic_weather_by_zip_code(self, zipcode: str) -> pd.DataFrame:
        log.info(f"getting historic weather data for zipcode {zipcode}")
        self.connect()
        cursor = self.connection.cursor()
        try:
            weather_df = self.get_rows(
                cursor, get_historic_weather_for_zip_code.format(zipcode)
            )
        except Exception as e:
            raise RedshiftDWError(
                data={
                    "host": self.host,
                    "port": self.port,
                    "name": self.name,
                    "username": self.username,
                    "query": get_historic_weather_for_zip_code.format(zipcode),
                    "err": e,
                },
                message=error_executing_historic_weather_query,
            )
        finally:
            cursor.close()
            self.close()

        log.info(f"successfully retrieved historic weather data for zipcode {zipcode}")
        return self.convert_daily_weather_to_hourly_dataframe(weather_df)

    def get_distinct_zip_codes_for_stores(self) -> pd.DataFrame:
        log.info("retrieving distinct zipcodes")
        self.connect()
        cursor = self.connection.cursor()
        try:
            zip_code_df = self.get_rows(cursor, get_distinct_zip_codes)
        except Exception as e:
            raise RedshiftDWError(
                data={
                    "host": self.host,
                    "port": self.port,
                    "name": self.name,
                    "username": self.username,
                    "query": get_distinct_zip_codes,
                    "err": e,
                },
                message=error_executing_zip_code_query,
            )
        finally:
            cursor.close()
            self.close()

        log.info("retrieved distinct zipcodes")
        return zip_code_df

    """
    TRANSLATION HELPERS
    """

    def get_rows(self, cursor, query: str) -> pd.DataFrame:
        cursor.execute(query)
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

    def convert_daily_weather_to_hourly_dataframe(
        self, daily_weather_df: pd.DataFrame
    ) -> pd.DataFrame:
        log.info("converting retrieved weather data to hourly data")
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        converted_df = pd.DataFrame(
            [], columns=["date_time", "condition", "precipitation"]
        )
        for row in daily_weather_df.iterrows():
            date = row[1]["weather_date"]

            if yesterday - date < datetime.timedelta(0):
                continue

            condition = row[1]["condition_text"]
            precipitation = (
                row[1]["total_precipitation"]
                if row[1]["total_precipitation"] != None
                else 0
            )

            hourly_weather_array = []
            for hour in range(0, 24):
                hour_timestamp = datetime.datetime(
                    date.year, date.month, date.day, hour
                )
                hourly_weather_array.append(
                    [hour_timestamp, condition, precipitation / 24]
                )

            converted_df = pd.concat(
                [
                    converted_df,
                    pd.DataFrame(
                        hourly_weather_array,
                        columns=["date_time", "condition", "precipitation"],
                    ),
                ]
            )

        log.info("converted retrieved weather data to hourly data")
        return converted_df

    def get_datetimes_for_order_query(self) -> tuple[str, str]:
        time_period_start = datetime.datetime(
            year=earliest_weather_year,
            month=earliest_weather_month,
            day=earliest_weather_day,
        ).strftime("%Y-%m-%d")
        time_period_end = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%d"
        )
        return time_period_start, time_period_end

    """
    CONNECTION HELPERS
    """

    def connect(self) -> None:
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.name,
                user=self.username,
                password=self.password,
            )
        except:
            raise ConnectionError(
                data={
                    "db-host": self.host,
                    "db-port": self.port,
                    "db-name": self.name,
                    "db-user": self.username,
                },
                message=unable_to_connect_to_redshift,
            )

        if conn.status != psycopg2.extensions.STATUS_READY:
            raise ConnectionError(
                data={
                    "db-host": self.host,
                    "db-port": self.port,
                    "db-name": self.name,
                    "db-user": self.username,
                    "db-status": conn.status,
                },
                message=unexpected_connection_status,
            )

        self.connection = conn

    def close(self) -> None:
        self.connection.close()
        self.connection = None

    def __init__(self, c: Config):
        self.host = c.db_host
        self.port = c.db_port
        self.name = c.db_name
        self.username = c.db_user
        self.password = c.db_password
