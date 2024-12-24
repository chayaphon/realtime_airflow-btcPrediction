import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta,timezone
import time
from bson import ObjectId  # Import ObjectId to check and convert
import pandas as pd
import numpy as np
import requests
import pendulum
from airflow.models import Variable
from airflow.models.dataset import Dataset

interval = 12
freq_ ='5T' # 5 minutes each time slot
local_tz = pendulum.timezone("Asia/Bangkok")

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def predictBTC(ti, **kwargs):
    current_time_th = pendulum.now(local_tz)
    counter = int(Variable.get("task_execution_counter", default_var=1))
    if counter >= interval:
        import pickle
        import xgboost as xgb
        
        try:    
            modelFile = '/usr/local/airflow/output/models.pkl'
            with open(modelFile, 'rb') as f:
                models = pickle.load(f)
        except Exception as e:
            print(f"Error loading the model: {e}")
            return
        
        # predict ofr next 12 times starting at next 5 minutes.
        # this will be use in model due to model was train with utc time
        predicted_time = datetime.utcnow() +  timedelta(minutes=5) + timedelta(seconds=1)
        # this will be use for display time in out.csv in thai time
        predicted_time_th = current_time_th +  timedelta(minutes=5) + timedelta(seconds=1)
        
        ###################################
        ### Prediction 3 model Ensemble ###
        ###################################
        
        # This will generate forecast time slot starting from predicted_time with period interval by freq_ (future closeTime)
        forecast_closeTime = pd.date_range(start=predicted_time, periods=interval, freq=freq_)
        
        # Prophet Prediction for next interval period
        future_dates = pd.DataFrame({'ds': forecast_closeTime})
        prophet_forecast = models['prophet'].predict(future_dates)
        prophet_predictions = prophet_forecast['yhat'].values
        # ------------------------------------ #
        
        # SARIMA Prediction for next interval period
        # sarima_predictions = models['sarima'].forecast(steps=interval)
        sarima_predictions = models['sarima'].forecast(steps=interval, index=forecast_closeTime)
        # ------------------------------------ #
        
        # XGBoost Prediction for next interval period
        # Prepare features for XGBoost
        future_features = pd.DataFrame()
        future_features['closeTime'] = forecast_closeTime 
        future_features['closeTime'] = future_features['closeTime'].astype('int64') // 10**6 # turn it to timestamp ms due to model was train by this format
        future_features['lag1'] = models['last_prices'][-1]
        future_features['lag2'] = models['last_prices'][-2]
        future_features['lag3'] = models['last_prices'][-3]
        future_features['rolling_mean_6'] = np.mean(models['last_prices'])
        future_features['rolling_std_6'] = np.std(models['last_prices'])
        
        # Convert future_features to DMatrix
        dmatrix_future_features = xgb.DMatrix(future_features[models['features']])
        # Predict using the XGBoost model
        xgb_predictions = models['xgb'].predict(dmatrix_future_features)
        # ------------------------------------ #
        
        # Ensemble predictions (simple average)
        final_predictions = (prophet_predictions + sarima_predictions + xgb_predictions) / 3
        # ------------------------------------ #
        
        # Create DataFrame with predictions
        prediction_df = pd.DataFrame({
            'createdTime' : current_time_th,
            'closeTime': pd.date_range(start=predicted_time_th, periods=interval, freq=freq_),
            'predicted': final_predictions,
            'prophet_pred': prophet_predictions,
            'sarima_pred': sarima_predictions,
            'xgb_pred': xgb_predictions
        })
        
        # save latest 12 predictions to out.csv
        file_path1 = '/usr/local/airflow/output/out.csv'
        prediction_df.to_csv(file_path1, index=False)
        
        # insert prediction into Mongo DB btc_prediction for later analysis
        try:
            # Connect to MongoDB
            hook = MongoHook(mongo_conn_id='mongoid')
            client = hook.get_conn()
            db = client.MyDB
            btc_prediction = db.btc_prediction
            print(f"Connected to MongoDB - {client.server_info()}")

            # Convert DataFrame to a list of dictionaries
            data = prediction_df.to_dict('records')

            # Insert the data into MongoDB
            if data: 
                btc_prediction.insert_many(data)
                print("Data inserted successfully into MongoDB!")
            else:
                print("No data to insert into MongoDB!")
        except Exception as e:
            print(f"Error connecting to MongoDB -- {e}")
 
 
def evaluateModel(**kwargs):
    current_time_th = pendulum.now(local_tz)
    current_time_utc = datetime.utcnow()
    print(current_time_th)
    counter = int(Variable.get("task_execution_counter", default_var=1))
    print("starting counter = ",counter)
    errorList = Variable.get("error_list", default_var="[]")
    errorList = json.loads(errorList)
    
    # Evaluate Predict with Actual price in each 12 slots (5 minutes each slot)
    # Check if the file exists
    file_path = '/usr/local/airflow/output/out.csv'
    if os.path.exists(file_path):
        
        # Read CSV current prediction file into a  DataFrame
        df = pd.read_csv(file_path)
        
        # Get current BTC price
        url = 'https://api.binance.com/api/v3/ticker?type=MINI&symbol=BTCUSDT&windowSize=1h'
        response = requests.get(url)
        data = response.json()
        
        print(data)
        print("CSV file loaded successfully.")
        print('predictedPrice=', df.loc[counter-1,'predicted'])
        print('lastPrice=',data['lastPrice'])
        
        y_hat = float(df.loc[counter-1,'predicted'])
        y_actual = float(data['lastPrice'])
        
        # Calculate ape
        import math
        errorList.append(abs(y_hat-y_actual)/y_actual)
        print(errorList)
        
    else:
        # incese want to reset begining prediction, then remove /usr/local/airflow/output/out.csv file then the task will create out.csv predict next 12 slot and reset count to 1
        print(f"File '{file_path}' does not exist.")
        try:
            counter = interval
            Variable.set("task_execution_counter", counter)
            predictBTC(**kwargs)
            # counter = 0 because the next condition will do count += 1 so at the end it will be counter = 1 as default for begining state.
            counter = 0
            errorList = []
            print(f"call fucntion predictBTC, and reset variable succesfully !")
        except Exception as e:
            print(f"cant call fucntion predictBTC, {e}")

    # if it's at the max interval calculate MAPE (interval=12)
    # use (counter >= interval) instead of (counter = interval), to prevent unintentional error if counter double to be like 13 it would still go in reset condition) 
    if counter >= interval:
        # use current time utc becuase A.Ekarat require schedule each day 22:00 - 06:00 next day (so in utc it's still the same day so we can append data to the same file date)
        str_date = str(current_time_utc.date())
        mape_path = f'/usr/local/airflow/output/mape_{str_date}.txt'
        
        if not os.path.exists(mape_path):
            with open(mape_path, 'a') as output:
                output.write(f"time, mape\n")
            
        print("Calculate MAPE.")
        
        # Ensure errorList is not empty before calculating MAPE
        if len(errorList) > 0:
            mape = (sum(errorList) / len(errorList)) * 100
            print('mape =', mape)
            
            with open(mape_path, 'a') as output:
                output.write(f"{current_time_th}, {mape}\n")
        else:
            print("list is empty. Skipping MAPE calculation.")
            
        # Reset for next iteration
        counter = 1
        errorList = []
    else:
        counter += 1
    
    Variable.set("error_list", json.dumps(errorList))
    Variable.set("task_execution_counter", counter)
    print('ending counter : ', counter)

with DAG(
    dag_id="Predict_BTC_Price",
    schedule_interval="*/5 22-23,0-6 21-24 12 *",
    #schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 12, 21, 21, 55, tzinfo=local_tz),
    end_date=datetime(2024, 12, 24, 6, 5, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["crypto"],
    default_args={
        "owner": "Chayaphon S.",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": on_failure_callback
    }
) as dag:
    
    t1 = PythonOperator(
        task_id="predictBTC",
        python_callable=predictBTC,
        provide_context=True,
    )
    
    t2 = PythonOperator(
        task_id="evaluateModel",
        python_callable=evaluateModel,
        provide_context=True,
    )

    t1
    t2
