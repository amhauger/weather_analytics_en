import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from multiprocessing import Pool
from itertools import cycle


from config import Config
from data_warehouse import RedshiftDW
from elastic_net_model import ElasticNetModel, regressors, predictor
from weather_dictionaries import *
from data_cleanup import clean_data

log = logging.getLogger(__name__)

labels = [
    "Hour",
    "Precipitation",
    "Holiday",
    "Adjusted Hours",
    "Cloud Cover",
    "Rain Intensity",
    "Sleet Intensity",
    "Snow Intensity",
    "Ice Intensity",
    "Thunder Intensity",
]

colors = [
        mcolors.CSS4_COLORS["dodgerblue"],
        mcolors.CSS4_COLORS["red"],
        mcolors.CSS4_COLORS["darkseagreen"],
        mcolors.CSS4_COLORS["cyan"],
        mcolors.CSS4_COLORS["black"],
        mcolors.CSS4_COLORS["blue"],
        mcolors.CSS4_COLORS["deeppink"],
        mcolors.CSS4_COLORS["brown"],
        mcolors.CSS4_COLORS["slategrey"],
        mcolors.CSS4_COLORS["orange"],
        ]

def create_store_info_df(store_info: pd.Series, num_rows: int) -> pd.DataFrame:
    store_info_keys = store_info.keys()
    df = pd.DataFrame(
        [store_info.loc[store_info_keys[0]]], columns=[store_info_keys[0]]
    )
    for i in range(1, len(store_info_keys)):
        df.insert(df.shape[1], store_info_keys[i], store_info.loc[store_info_keys[i]])

    return pd.DataFrame(np.tile(df.values, [num_rows, 1]), columns=df.columns)


def get_store_data(dw: RedshiftDW):
    all_data = pd.DataFrame([])
    zips = dw.get_distinct_zip_codes_for_stores()
    for row in zips.iterrows():
        zipcode = row[1]["zip_code"]
        """
        weather is x = x_n-1: weather conditions + x_n: datetime
        """
        weather = dw.get_historic_weather_by_zip_code(zipcode)
        """
        stores is region_number + location_number + store_hyperparameters
        """
        stores = dw.get_stores_by_zip_code(zipcode)
        for store in stores.iterrows():
            """
            orders is y = cars per hour
            orders.datetime
            orders.carcount
            """
            orders = dw.get_orders_by_store_number(store[1]["location_number"])
            """
            store_data = [x | Y]
            """
            data = pd.merge(weather, orders, how="left", on="date_time")
            store_info_df = create_store_info_df(store[1], data.shape[0])
            for column in data.columns:
                store_info_df.insert(len(store_info_df.columns), column, data[column])

            clean_data(store_info_df)

            if len(all_data.columns) == 0:
                all_data = store_info_df.copy()
            else: 
                all_data = pd.concat([all_data, store_info_df], ignore_index=True)
    return all_data


def fit_stores(dw: RedshiftDW, location_number: str, data: pd.DataFrame):
    log.info(f"fitting elastic net model for store {location_number}")
    m = ElasticNetModel(data)
    m.tune_model()
    dw.set_store_elastic_net_values(location_number, m.l1_ratio, m.alpha, m.mae)


def plot_and_coefficient_vals(m: ElasticNetModel, region_number: str) -> None:
    log.info(f"plotting coefficient values for region {region_number}")
    coefficient_values = m.coefficients
    fig = plt.figure()
    fig.bar(labels, coefficient_values, label=labels, color=colors)
    fig.title(f"Elastic-Net Coefficients for Region {region_number}")
    fig.legend()
    fig.savefig(f"./images/vals/region_{region_number}_coefficient_values.png", format='png')


def plot_and_save_coefficient_path(m: ElasticNetModel, region_number: str) -> None:
    log.info(f"plotting coefficient path for region {region_number}")
    alphas, coeffs = m.coefficient_path()
    fig = plt.figure()
    for coef, c, label in zip(coeffs[0], cycle(colors), labels):
        fig.semilogx(alphas, coef, linestyle="-", c=c, label=label)
        
    fig.axvline(x=m.alpha, ymin=-0.5, ymax=3.5, label='Optimal Alpha', linestyle='--', c=mcolors.CSS4_COLORS["khaki"])
    fig.xlabel("alpha")
    fig.ylabel("coefficients")
    fig.title(f"Elastic-Net Coefficient Paths for Region {region_number}")
    fig.legend()
    fig.savefig(f"./images/paths/coeff_path_region_{region_number}.png", format='png')


def fit_region(dw: RedshiftDW, region_number: str, data: pd.DataFrame) -> None:
    log.info(f"fitting elastic net model for region {region_number}")
    m = ElasticNetModel(data)
    m.tune_model()
    # plot_and_save_coefficient_path(m, region_number)
    plot_and_coefficient_vals(m, region_number)
    dw.set_region_coefficient_vals(m, region_number)


def run():
    config = Config()
    dw = RedshiftDW(config)
    store_data = get_store_data(dw)

    # store_items = [(dw, location_number, store_data.loc[store_data["location_number"] == location_number, regressors+predictor]) for location_number in store_data.location_number.unique()]
    region_items = [(dw, region_number, store_data.loc[store_data["region_number"] == region_number, regressors+predictor]) for region_number in store_data.region_number.unique()]

    # with Pool(6) as pool:
    #     pool.starmap(fit_stores, store_items)
    with Pool(6) as pool:
        pool.starmap(fit_region, region_items)
    

if __name__ == "__main__":
    run()
