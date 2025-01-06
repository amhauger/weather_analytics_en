import logging
import os

from error_types import ConfigError
from error_strings import *

log = logging.getLogger(__name__)


class Config:
    """
    data warehouse configs
    """

    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: int
    db_password: str

    """
    weather configs
    """
    weatherapi_key: str

    def get_weatherapi_key_from_environment(self) -> None:
        log.info("getting weatherapi api key from environment")
        if os.environ.get("WEATHERAPIKEY", None) == None:
            log.error(no_weather_key)
            raise ConfigError(
                data={"weatherapi_key": os.environ.get("WEATHERAPIKEY", None)},
                message=no_weather_key,
            )
        self.weatherapi_key = os.environ.get("WEATHERAPIKEY")
        log.info("weatherapi key set in config")

    def get_database_configs_from_environment(self) -> None:
        log.info("getting database information from dictionary")
        if os.environ.get("DBHOST", None) == None:
            log.error(no_db_host_name)
            raise ConfigError(
                data={},
                message=no_db_host_name,
            )
        self.db_host = os.environ.get("DBHOST")
        log.info("database host set in config")

        if os.environ.get("DBPORT", None) == None:
            log.error(no_db_port)
            raise ConfigError(
                data={
                    "db_host": self.db_host,
                },
                message=no_db_port,
            )
        self.db_port = os.environ.get("DBPORT")
        log.info("database port set in config")

        if os.environ.get("DBNAME", None) == None:
            log.error(no_db_name)
            raise ConfigError(
                data={
                    "db_host": self.db_host,
                    "db_port": self.db_port,
                },
                message=no_db_name,
            )
        self.db_name = os.environ.get("DBNAME")
        log.info("database name set in config")

        if os.environ.get("DBUSER", None) == None:
            log.error(no_db_username)
            raise ConfigError(
                data={
                    "db_host": self.db_host,
                    "db_port": self.db_port,
                    "db_name": self.db_name,
                },
                message=no_db_username,
            )
        self.db_user = os.environ.get("DBUSER")
        log.info("database username set in config")

        if os.environ.get("DBPASSWORD", None) == None:
            log.error(no_db_password)
            raise ConfigError(
                data={
                    "db_host": self.db_host,
                    "db_port": self.db_port,
                    "db_name": self.db_name,
                },
                message=no_db_password,
            )
        self.db_password = os.environ.get("DBPASSWORD")
        log.info("database password set in config")

    def __init__(self):
        log.info("creating config object")

        for func in [
            self.get_weatherapi_key_from_environment,
            self.get_database_configs_from_environment,
        ]:
            func()

        log.info("finished creating config object")
