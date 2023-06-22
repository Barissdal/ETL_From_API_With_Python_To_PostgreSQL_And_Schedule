import psycopg2
import numpy as np
import psycopg2.extras as extras
import pandas as pd
import time
import threading
import requests
import json

def get_weather_data(api_key, city_id,id_number):
    api_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "id": city_id,
        "units": "metric",
        "appid": api_key
    }
    response = requests.get(api_url, params=params)
    data = response.json()
    city = data["name"]
    temperature = data["main"]["temp"]
    wind_speed = data["wind"]["speed"]
    description=data["weather"][0]["description"]
    optime= pd.Timestamp('now').strftime("%Y-%m-%d %H:%M:%S")
    weather_ankara = pd.DataFrame({
    'id': [id_number],
    'city': [city],
    'temperature': [temperature],
    'wind_speed': [wind_speed],
    'weather_type' : [description],
    'optime' : [optime]
    })
    return weather_ankara

def get_hourly_weather_data(sira):
    while True:
        id_number = sira
        target_data = get_weather_data(api_key, city_id, id_number)
        target_data["id"] = id_number + 1
        print(target_data.head(5))
        return target_data



baslama=0

def execute_values(conn, table):
    while True:
        global baslama
        sayac = 0
        print("Değer:",baslama)
        girdi = baslama + sayac
        print("Girdi:",girdi)
        df=get_hourly_weather_data(girdi)
        tuples = [tuple(x) for x in df.to_numpy()]

        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = conn.cursor()
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is inserted")
        sayac +=1
        baslama = girdi + sayac
        print("Değer Son:",baslama)
        if sayac > 100:
            print("sayac:",sayac)
            cursor.close()
            return 1
        else:
            time.sleep(5)

api_key = "<YOUR_API_KEY>"
city_id = "323786"  # Ankara

print("Connection doing!")

conn = psycopg2.connect(
database='<YOUR_DB>', user='<YOUR_USER>', password='<YOUR_PASS>', host='localhost', port='5432'
)

thread = threading.Thread(target=execute_values(conn, 'weather'))
thread.start()
print('Waiting for the thread...')
thread.join()
