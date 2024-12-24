import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime,timedelta
from bson import ObjectId  # Import ObjectId to check and convert
import pandas as pd
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

def on_failure_callback(**context):
    print(f"Task {context['task_instance_key_str']} failed.")

def downloadFromMongo(ti, **kwargs):
    try:
        hook = MongoHook(mongo_conn_id='mongo_default')
        client = hook.get_conn()
        db = client.MyDB
        btc_collection = db.btc_collection
        print(f"Connected to MongoDB - {client.server_info()}")

        # Convert ObjectId to string
        def convert_object_id(data):
            if isinstance(data, list):
                return [convert_object_id(item) for item in data]
            elif isinstance(data, dict):
                return {key: convert_object_id(value) for key, value in data.items()}
            elif isinstance(data, ObjectId):
                return str(data)
            else:
                return data

        # Fetch and convert data
        # Calculate timestamp for 1 day ago
        one_day_ago = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
        #### Query to get only data with closeTime in the last 1 day ####
        query = {"closeTime": {"$gte": one_day_ago}}
        raw_data = list(btc_collection.find(query))
        converted_data = convert_object_id(raw_data)
        results = json.dumps(converted_data)
        print(results)

        ti.xcom_push(key="data", value=results)  # Use 'ti' to push to XCom

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")


def trainModel(ti, **kwargs):
    data = ti.xcom_pull(
        task_ids="Get_BTC_from_Mongodb",
        key="data"
    )
    df = pd.DataFrame(json.loads(data))
    
    # prep data
    df['datetime'] = pd.to_datetime(df['closeTime'], unit='ms')
    df = df[~df['datetime'].duplicated(keep='first')]
    df.set_index('datetime', inplace=True, drop=False)
    df = df.sort_index()
    df['lastPrice'] = pd.to_numeric(df['lastPrice'], errors='coerce')
    df['lastPrice'] = df['lastPrice'].ffill() # Forward fill
    
    ##############################
    ### Model 1: Prophet Model ###
    ##############################
    from prophet import Prophet
    # Prepare data for Prophet
    prophet_df = pd.DataFrame()
    prophet_df['ds'] = df['datetime']
    prophet_df['y'] = df['lastPrice']
    
    model = Prophet(
        changepoint_prior_scale=0.001,
        seasonality_prior_scale=0.1,
        seasonality_mode='additive',
        daily_seasonality=True,
        weekly_seasonality=True
    )
    model.fit(prophet_df)
    #-----------------------------------------#
    
    ##############################
    ### Model 2: SARIMA Model ###
    ##############################
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    # Fit SARIMA model
    sarima_model = SARIMAX(
        df['lastPrice'],
        order=(0, 1, 3),  # ARIMA parameters (p,d,q)
        seasonal_order=(0, 1, 1, 24),  # Seasonal parameters (P,D,Q,s)
    )
    sarima_results = sarima_model.fit()
    #-----------------------------------------#
    
    ##################################################
    ### Model 3: XGBoost with Time Series Features ###
    ##################################################
    import xgboost as xgb
    import numpy as np
    from sklearn.metrics import mean_squared_error
    
    # Feature Engineering for Time Series
    df['lag1'] = df['lastPrice'].shift(1) # previous 1 price as features
    df['lag2'] = df['lastPrice'].shift(2) # previous 2 price as features
    df['lag3'] = df['lastPrice'].shift(3) # previous 3 price as features
    df['rolling_mean_6'] = df['lastPrice'].rolling(window=6).mean() # moving average for 6 lastPrice
    df['rolling_std_6'] = df['lastPrice'].rolling(window=6).std() # moving sd for 6 lastPrice

    # Features and Target
    features = ['closeTime', 'lag1', 'lag2', 'lag3', 'rolling_mean_6', 'rolling_std_6']
    X = df[features].dropna()
    y = df['lastPrice'].loc[X.index]

    # Train-test split
    split_index = int(0.8 * len(X))
    X_train = X[:split_index]
    X_test = X[split_index:]
    y_train = y[:split_index]
    y_test = y[split_index:]
    
    print(f"Training set size: {X_train.shape}, Test set size: {X_test.shape}")

    # XGBoost parameters
    params = {
        'objective': 'reg:squarederror',
        'learning_rate': 0.01,
        'max_depth': 5,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'reg_alpha': 1.0,
        'reg_lambda': 2.0,
        'min_child_weight': 1,
        'eval_metric': 'rmse'
    }
    
    # Create DMatrix objects
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    
    # Set up evaluation list
    evallist = [(dtrain, 'train'), (dtest, 'eval')]
    
    # Train with early stopping
    xgb_model = xgb.train(
        params,
        dtrain,
        num_boost_round=1000,
        evals=evallist,
        early_stopping_rounds=50,
        verbose_eval=True
    )
    
    # Calculate final metrics
    train_pred = xgb_model.predict(dtrain)
    test_pred = xgb_model.predict(dtest)
    
    final_metrics = {
        'train_rmse': float(np.sqrt(mean_squared_error(y_train, train_pred))),
        'test_rmse': float(np.sqrt(mean_squared_error(y_test, test_pred))),
        'best_iteration': xgb_model.best_iteration,
        'feature_importance': dict(zip(features, [float(v) for v in xgb_model.get_score().values()])),
        'params': params
    }
    
    print(f"Final Metrics: {json.dumps(final_metrics, indent=2)}")
    #-----------------------------------------#
    
    
    # Save all models
    import pickle
    models = {
        'prophet': model,
        'sarima': sarima_results,
        'xgb': xgb_model,
        'features': features,
        'last_timestamp': df['datetime'].max(),
        'last_prices': df['lastPrice'].tail(6).tolist()  # Save last 6 prices for lag features
    }
    
    modelFile = '/usr/local/airflow/output/models.pkl'
    with open(modelFile, 'wb') as f:
        pickle.dump(models, f)


with DAG(
    dag_id="Train_Model",
    schedule_interval= "56 * * * *",
    start_date=datetime(2024, 12, 21, 21, 0, tzinfo=local_tz),
    end_date=datetime(2024, 12, 24, 6, 0, tzinfo=local_tz),
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
        task_id="Get_BTC_from_Mongodb",
        python_callable=downloadFromMongo,
        provide_context=True,  # Enable context
    )

    t2 = PythonOperator(
        task_id="train_model",
        python_callable=trainModel,
        provide_context=True,  # Enable context
    )

    t1 >> t2