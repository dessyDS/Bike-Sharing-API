import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd

from flask import Flask, request
app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()


@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()


@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()


@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trip = get_trip_id(trip_id, conn)
    return trip.to_json()

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result    

@app.route('/trips/average_duration')
def route_avg_trips():
    conn = make_connection()
    avg_trips = get_avg_trips(conn)
    return avg_trips.to_json()

@app.route('/trips/average_duration/<bikeid>')
def route_bike_id(bikeid):
    conn = make_connection()
    bikeid = get_bike_id(bikeid, conn)
    return bikeid.to_json()


@app.route('/json', methods=['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/subs/add', methods=['POST']) 
def route_subs_type():
    input_data = request.get_json(force=True) 
    subs_type = input_data['subscriber_type']
    conn = make_connection()

    query = f"""SELECT subscriber_type, duration_minutes
            FROM stations 
            LEFT JOIN trips
            WHERE subscriber_type LIKE '{subs_type}%'"""
    selected_data = pd.read_sql_query(query, conn)

    result = selected_data.groupby('subscriber_type').agg({
    'duration_minutes' : 'mean', 
}).round(2)
    return result.to_json()

###### Function #######

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE trip_id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_avg_trips(conn):
    query = f"""
            SELECT bikeid, ROUND(AVG(duration_minutes)) AS average_durations, 
            SUM(duration_minutes) AS total_durations 
            FROM trips 
            GROUP BY bikeid
            """
    result = pd.read_sql_query(query, conn)
    return result

def get_bike_id(bikeid, conn):
        return (
            pd.read_sql_query(
                f"""
                SELECT start_time, bikeid, duration_minutes
                FROM trips
                WHERE bikeid = {bikeid}
                """,
                conn,
                parse_dates='start_time'
            ).assign(yearmonth=lambda df:df.start_time.dt.year          
            ).groupby(['yearmonth', 'bikeid']).agg({
            'duration_minutes': ['mean', 'sum']}).round(2).sort_values('yearmonth', ascending=False)
        )
    
    

if __name__ == '__main__':
    app.run(debug=True, port=5000)