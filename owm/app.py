import pymongo
import pandas as pd
from io import StringIO
import time
import boto3
import json

AWS_REGION_NAME = 'ap-southeast-1'
session = boto3.session.Session(region_name=AWS_REGION_NAME)
s3_resource = session.resource('s3')

# get data from mongodb
conn_str = "mongodb+srv://bdasm3:bdasm3@cluster0.alu5reb.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(conn_str)
db = client.awspipeline
owm_collection = db["owm"]
owm_records_hourly = list(owm_collection.find())
df = pd.DataFrame(owm_records_hourly)

# s3 config
bucket = 'bdasm3-daily-data'  # already created on S3

# get today
now = time.localtime()
today = time.strftime(
    "%Y-%m-%d", now)


def delta(current, hour):
    return abs(current - hour)


def lambda_handler(event, context):
    try:
        # create column for hour
        today_df = df[df['Date'] == today]
        today_df['Hour'] = today_df['Time'].str[:2]
        today_df['Hour'] = today_df['Hour'].astype(int)

        # conver kelvin degree to celcius for temp value
        today_df['MaxTemp'] = today_df['MaxTemp'] - 273.15
        today_df['MinTemp'] = today_df['MinTemp'] - 273.15

        # create column for delta time to 9am and 3pm
        today_df['delta_9am'] = today_df['Hour'].apply(lambda x: delta(x, 9))
        today_df['delta_3pm'] = today_df['Hour'].apply(lambda x: delta(x, 15))

        drop_col = ['_id', 'Time', 'Raining', 'MaxTemp', 'MinTemp', 'WindDeg',
                    'WindGustSpeed', 'Hour', 'delta_9am', 'delta_3pm']

        dict_col_for9am = {'Cloud': 'cloud9am', 'Humidity': 'humidity9am', 'Pressure': 'pressure9am',
                           'Temp': 'temp9am', 'WindDir': 'winddir9am', 'WindSpeed': 'windspeed9am'}
        dict_col_for3pm = {'Cloud': 'cloud3pm', 'Humidity': 'humidity3pm', 'pPressure': 'pressure3pm',
                           'Temp': 'temp3pm', 'WindDir': 'winddir3pm', 'WindSpeed': 'windspeed3pm'}

        df_for9am = today_df[today_df.delta_9am ==
                             today_df.delta_9am.min()].drop(drop_col, axis=1).rename(columns=dict_col_for9am)
        df_for3pm = today_df[today_df.delta_3pm ==
                             today_df.delta_3pm.min()].drop(drop_col, axis=1).rename(columns=dict_col_for3pm)

        df_res = today_df.groupby('Date').agg(
            mintemp=('MinTemp', 'min'), maxtemp=('MaxTemp', 'max'))

        df_res = pd.concat([df_res, df_for9am.set_index('Date'),
                            df_for3pm.set_index('Date')], axis=1)

        # get year - day - month columns
        df_res['year'] = df_res.index.str[0:4].astype(int)
        df_res['month'] = df_res.index.str[5:7].astype(int)
        df_res['day'] = df_res.index.str[8:10].astype(int)

        # get RainToday value
        df_res['raintoday'] = 1 if 1.0 in today_df['Raining'] else 0

        # get location
        df_res['location'] = 'syd'

        csv_buffer = StringIO()
        df_res.to_csv(csv_buffer)
        time.sleep(1)
        s3_resource.Object(bucket, '{}.csv'.format(today)).put(Body=csv_buffer.getvalue())
        time.sleep(1)

        current_weather = df_res.to_json(orient="records")
        parsed = json.loads(current_weather)
        data = json.dumps(parsed)
        return {
            "records": data
        }
    except Exception as e:
        print(str(e))