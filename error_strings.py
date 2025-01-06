"""
CONFIG ERRORS
"""

no_weather_key = "no weatherapi key present in config yaml"
no_db_host_name = "no host string for database connection present in config yaml"
no_db_name = "no database name for database connection present in config yaml"
no_db_username = "no username for database connection present in config yaml"
no_db_password = "no password for database connection present in config yaml"
no_db_port = "no port for database connection present in config yaml"

"""
CONNECTION ERRORS
"""
unable_to_connect_to_redshift = (
    "unable to connect to redshift using provided credentials"
)

unexpected_connection_status = (
    "unexpected connection status for returned redshift connection"
)

"""
QUERY ERRORS
"""
error_executing_zip_code_query = (
    "error retrieving distinct zip codes from the data warehouse"
)
error_executing_store_params_query = (
    "error retrieving store parameters from the data warehouse"
)
error_executing_historic_weather_query = (
    "error retrieving historic weather parameters from the data warehouse"
)
error_executing_store_orders_query = (
    "error retrieving historic hourly orders for store from the data warehouse"
)
error_updating_store_hyperparameters = (
    "error updating l1 ratio and alpha hyperparameters, and the elastic net MAE for store"
)
error_setting_region_coefficients = (
    "error inserting values for the region's fit coefficients into the data warehouse"
)