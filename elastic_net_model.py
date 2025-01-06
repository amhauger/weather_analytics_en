import logging
import boto3
import pandas as pd
import numpy as np
from sklearn.linear_model import ElasticNet, enet_path
from sklearn.model_selection import RepeatedKFold, GridSearchCV

from weather_dictionaries import *

boto3.set_stream_logger("", logging.DEBUG)
log = logging.getLogger(__name__)

regressors = [
    "hour",
    "precipitation",
    "is_holiday",
    "adj_hours",
    "cloud_cover",
    "rain_intensity",
    "sleet_intensity",
    "snow_intensity",
    "ice_intensity",
    "thunder_intensity",
]
predictor = ["car_count"]


class ElasticNetModel:
    x_train: pd.DataFrame
    y_train: pd.Series
    model: ElasticNet
    _l1_ratio: float
    _alpha: float
    _coefficients: list
    _mae: float

    @property
    def l1_ratio(self):
        return self._l1_ratio

    @property
    def alpha(self):
        return self._alpha

    @property
    def coefficients(self):
        return self._coefficients

    @property
    def mae(self):
        return self._mae

    def coefficient_path(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        alphas, coeffs, _ = enet_path(X=self.x_train, y=self.y_train, l1_ratio=self._l1_ratio, alphas=np.linspace(0, 1, num=100))
        return alphas, coeffs


    def tune_model(self):
        log.info("tuning model")
        m = ElasticNet(
            fit_intercept=True,
            max_iter=100000,
        )
        grid = dict()
        grid["l1_ratio"] = np.linspace(0, 1, num=100)
        grid["alpha"] = np.linspace(0, 1, num=100)
        cv = RepeatedKFold(n_splits=10, n_repeats=3)
        search = GridSearchCV(
            m, grid, scoring="neg_mean_absolute_error", cv=cv, n_jobs=-1
        )
        results = search.fit(self.x_train, self.y_train)
        self._model = results.best_estimator_
        self._coefficients = self._model.coef_
        self._l1_ratio = results.best_params_["l1_ratio"]
        self._alpha = results.best_params_["alpha"]
        self._mae = np.abs(results.best_score_)


    def __init__(self, data: pd.DataFrame) -> None:
        if data[regressors].shape[0] == 0:
            log.warning(f"no data to fit model on")
            return
        elif data[regressors].shape[0] <= 10:
            log.warning(f"not enough data to cross validate")
            return
        
        self.x_train = data.loc[:, regressors]
        self.y_train = data.loc[:, predictor]
