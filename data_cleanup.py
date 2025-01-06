import numpy as np
import pandas as pd
import datetime
import pytz

from weather_dictionaries import *

"""
START WEATHER FUNCTIONS
"""


def enumerate_weather(df: pd.DataFrame):
    for enum_fn in [
        clean_condition_strings,
        create_weather_condition_columns,
        enumerate_cloud,
        enumerate_rain,
        enumerate_sleet,
        enumerate_snow,
        enumerate_ice,
        enumerate_thunder,
    ]:
        enum_fn(df)


def enumerate_cloud(df: pd.DataFrame):
    for key in cloud_enumeration_dict.keys():
        df.loc[df["condition"] == key, "cloud_cover"] = cloud_enumeration_dict[key]


def enumerate_rain(df: pd.DataFrame) -> None:
    for key in rain_enumeration_dict.keys():
        df.loc[df["condition"] == key, "rain_intensity"] = rain_enumeration_dict[key]


def enumerate_sleet(df: pd.DataFrame) -> None:
    for key in sleet_enumeration_dict.keys():
        df.loc[df["condition"] == key, "sleet_intensity"] = sleet_enumeration_dict[key]


def enumerate_snow(df: pd.DataFrame) -> None:
    for key in snow_enumeration_dict.keys():
        df.loc[df["condition"] == key, "snow_intensity"] = snow_enumeration_dict[key]


def enumerate_ice(df: pd.DataFrame) -> None:
    for key in ice_enumeration_dict.keys():
        df.loc[df["condition"] == key, "ice_intensity"] = ice_enumeration_dict[key]


def enumerate_thunder(df: pd.DataFrame) -> None:
    for key in thunder_enumeration_dict.keys():
        df.loc[df["condition"] == key, "thunder_intensity"] = thunder_enumeration_dict[
            key
        ]


def create_weather_condition_columns(df: pd.DataFrame) -> None:
    n = df.shape[0]
    df.insert(len(df.columns), "cloud_cover", np.zeros(n))
    df.insert(len(df.columns), "rain_intensity", np.zeros(n))
    df.insert(len(df.columns), "sleet_intensity", np.zeros(n))
    df.insert(len(df.columns), "snow_intensity", np.zeros(n))
    df.insert(len(df.columns), "ice_intensity", np.zeros(n))
    df.insert(len(df.columns), "thunder_intensity", np.zeros(n))


def clean_condition_strings(df: pd.DataFrame) -> None:
    for condition in df.condition.unique():
        if type(condition) == float:
            continue
        df.loc[df["condition"] == condition, "condition"] = condition.strip().lower()


"""
END WEATHER FUNCTIONS
"""

"""
BEGIN DATETIME FUNCTIONS
"""

def add_hour_column(df: pd.DataFrame) -> None:
    n = df.shape[0]
    df.insert(len(df.columns), "hour", np.zeros(n))


def add_and_modify_hour_column(df: pd.DataFrame) -> None:
    add_hour_column(df)
    for i, date in enumerate(df["date_time"]):
        df.loc[i, "hour"] = date.hour

def create_holiday_array() -> list:
    return pd.to_datetime(
        [
            "2023-12-25",
            "2024-12-25",
            "2025-12-25",
            "2024-11-23",
            "2024-11-28",
            "2025-11-27",
            "2026-11-26",
        ]
    )


def create_adj_hours_array() -> list:
    return pd.to_datetime(
        [
            "2024-01-01",
            "2025-01-01",
            "2026-01-01",
            "2024-07-04",
            "2025-07-04",
            "2026-07-04",
            "2023-10-31",
            "2024-10-31",
            "2025-10-31",
            "2026-10-31",
            "2023-12-24",
            "2024-12-24",
            "2025-12-24",
            "2026-12-24",
        ]
    )


def create_holiday_condition_columns(df: pd.DataFrame) -> None:
    n = df.shape[0]
    df.insert(len(df.columns), "is_holiday", np.zeros(n))
    df.insert(len(df.columns), "adj_hours", np.zeros(n))


def add_and_modify_holiday_fields(df: pd.DataFrame) -> None:
    create_holiday_condition_columns(df)
    holidays = create_holiday_array()
    adj_hours = create_adj_hours_array()

    for i, date in enumerate(df["date_time"]):
        if date != None:
            continue
        else:    
            date = date.strftime("%Y-%m-%d")
        if date in holidays:
            df.loc[i, "is_holiday"] = 1
        if date in adj_hours:
            df.loc[i, "adj_hours"] = 1


def is_dst(year: int, month: int, day: int, tz: str) -> bool:
    # from gist https://gist.github.com/dpapathanasiou/09bd2885813038d7d3eb
    non_dst = datetime.datetime(year=year, month=1, day=1)
    non_dst_tz_aware = pytz.timezone(tz).localize(non_dst)
    return not (
        non_dst_tz_aware.utcoffset()
        == pytz.timezone(tz)
        .localize(datetime.datetime(year=year, month=month, day=day))
        .utcoffset()
    )


def remove_non_business_hour_datetimes(df: pd.DataFrame) -> None:
    tz = df["time_zone"][0]
    summer_open = df["summer_hours_open"][0]
    summer_close = df["summer_hours_close"][0]
    winter_open = df["winter_hours_open"][0]
    winter_close = df["winter_hours_close"][0]
    for i, date in enumerate(df["date_time"]):
        if df["is_closed_sunday"][i] and date.dayofweek == 6:
            df.drop(index=i, inplace=True)

        if is_dst(date.year, date.month, date.day, tz):
            if date.hour < summer_open or date.hour >= summer_close:
                df.drop(index=i, inplace=True)
        else:
            if date.hour < winter_open or date.hour >= winter_close:
                df.drop(index=i, inplace=True)


def modify_datetime_fields(df: pd.DataFrame) -> None:
    for fn in [remove_non_business_hour_datetimes, add_and_modify_hour_column, add_and_modify_holiday_fields]:
        fn(df)


"""
END DATETIME FUNCTIONS
"""


def drop_na_rows(df: pd.DataFrame) -> None:
    df.dropna(inplace=True)
    df.reset_index(inplace=True, drop=True)


def clean_data(df: pd.DataFrame) -> None:
    for cleaning_fn in [
        drop_na_rows,
        modify_datetime_fields,
        enumerate_weather,
    ]:
        cleaning_fn(df)
