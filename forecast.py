
import numpy as np
import pandas as pd
import pmdarima as pm
import sys

def forecast_accuracy(forecast, actual):
        mape = np.mean(np.abs(forecast - actual) / np.abs(actual))  # MAPE
        me = np.mean(forecast - actual)             # ME
        mae = np.mean(np.abs(forecast - actual))    # MAE
        mpe = np.mean((forecast - actual) / actual)   # MPE
        rmse = np.mean((forecast - actual) ** 2) ** .5  # RMSE

        corr = np.corrcoef(forecast, actual)[0,1]   # corr
        mins = np.amin(np.hstack([forecast[:,None], 
                                actual[:,None]]), axis=1)

        maxs = np.amax(np.hstack([forecast[:,None], 
                                actual[:,None]]), axis=1)
        minmax = 1 - np.mean(mins / maxs)             # minmax
        #acf1 = acf(forecast-actual)[1] # ACF1
        return 1 - mape, corr

def forecast_one(history_value, future_periods, season_cycle):
    # modeling
    model = pm.auto_arima(history_value, start_p=1, start_q=1,
                    test='adf',       # use adftest to find optimal 'd'
                    max_p=3, max_q=3, # maximum p and q
                    m=season_cycle,   # frequency of series
                    d=None,           # let model determine 'd'
                    seasonal=True,   # Seasonality
                    start_P=0, 
                    #D=1,
                    trace=False,
                    error_action='ignore',  
                    suppress_warnings=True, 
                    stepwise=True)
    aic = model.aic()
    fit_value = model.predict_in_sample()
    accu, corr = forecast_accuracy(fit_value, history_value)

    # forecast
    forecast_value, confidence_interval = model.predict(n_periods=future_periods, return_conf_int=True)

    return forecast_value, confidence_interval[:, 0], confidence_interval[:, 1], fit_value, aic, accu, corr

def forecast(history_value, future_periods, season_cycle):
    cycles = [1,season_cycle]
    best_c = 1
    best_aic = sys.maxsize
    best_forecast_value = None
    best_confidence_interval_low = None
    best_confidence_interval_up = None
    best_fit_value = None
    best_accu = None
    best_corr = None
    for c in cycles:
        forecast_value, confidence_interval_low, confidence_interval_up, fit_value, aic, accu, corr = forecast_one(history_value, future_periods, c)
        
        if aic < best_aic:
            best_c = c
            best_aic = aic
            best_forecast_value = forecast_value
            best_confidence_interval_low = confidence_interval_low
            best_confidence_interval_up = confidence_interval_up
            best_fit_value = fit_value
            best_accu = accu
            best_corr = corr
    return best_forecast_value, best_confidence_interval_low, best_confidence_interval_up, best_fit_value, best_aic, best_accu, best_corr

    