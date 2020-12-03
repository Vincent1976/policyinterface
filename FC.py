import numpy as np
import pandas as pd
import pmdarima as pm
import sys

# Accuracy metrics
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


###############
# df = pd.read_csv('wwwusage2.csv', names=['value'], header=0)
actualvalue = [3.526591,3.180891,3.252221,3.611003,3.565869,4.306371,5.088335,2.81452,2.985811,3.20478,3.127578,3.270523,3.737851, 3.558776,3.777202,3.92449, 4.386531, 5.810549, 6.192068, 3.450857, 3.772307, 3.734303, 3.905399, 4.049687,4.315566, 4.562185, 4.608662, 4.667851, 5.093841, 7.179962, 6.731473, 3.841278, 4.394076, 4.075341,4.540645, 4.645615, 4.752607, 5.350605, 5.204455, 5.301651, 5.773742, 6.204593, 6.749484, 4.216067,4.949349, 4.823045, 5.194754, 5.170787, 5.256742, 5.855277, 5.490729, 6.115293, 6.088473, 7.416598, 8.329452, 5.069796, 5.262557, 5.597126, 6.110296, 5.689161, 6.486849, 6.300569, 6.467476, 6.828629, 6.649078, 8.606937, 8.524471, 5.277918, 5.714303, 6.214529, 6.411929, 6.667716, 7.050831, 6.704919, 7.250988, 7.819733, 7.398101, 10.096233, 8.798513, 5.918261, 6.534493, 6.675736, 7.064201, 7.383381, 7.813496, 7.431892, 8.275117, 8.260441, 8.596156, 10.558939, 10.391416, 6.421535, 8.062619, 7.297739, 7.936916, 8.165323, 8.71742, 9.070964, 9.177113, 9.251887, 9.933136, 11.532974, 12.511462, 7.457199, 8.591191, 8.474, 9.386803, 9.560399, 10.834295,10.643751,9.908162, 11.710041, 11.340151, 12.079132, 14.497581, 8.049275, 10.312891, 9.753358, 10.850382, 9.961719, 11.443601, 11.659239, 10.64706, 12.652134, 13.674466, 12.965735, 16.300269, 9.053485, 10.002449, 10.78875, 12.106705, 10.954101, 12.844566, 12.1965, 12.854748, 13.542004, 13.28764, 15.134918, 16.82835, 9.800215, 10.816994, 10.654223, 12.512323, 12.16121, 12.998046, 12.517276, 13.268658, 14.733622, 13.669382, 16.503966, 18.003768, 11.93803, 12.9979, 12.882645, 13.943447, 13.989472, 15.339097, 15.370764, 16.142005, 16.685754, 17.636728, 18.869325, 20.778723, 12.154552, 13.402392, 14.459239, 14.795102, 15.705248, 15.82955, 17.554701,18.100864, 17.496668, 19.347265, 20.031291,23.486694, 12.536987, 15.467018, 14.233539, 17.783058, 16.291602, 16.980282, 18.612189, 16.623343, 21.430241, 23.575517, 23.334206, 28.038383, 16.763869, 19.792754, 16.427305, 21.000742, 20.681002, 21.83489, 23.930204,22.930357, 23.26334, 25.25003, 25.80609, 29.665356, 21.654285, 18.264945, 23.107677, 22.91251,19.43174]
df = pd.DataFrame(actualvalue,columns=['value'])

forecast_value, confidence_interval_low, confidence_interval_up, fit_value, aic, accu, corr = forecast(df.value, 12, 6)

