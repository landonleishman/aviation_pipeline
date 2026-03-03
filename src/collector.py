import requests 
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
import time

load_dotenv()

#creating cursor object for postgres connection
try:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5433")  
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=os.getenv("DB_NAME", "aviation_db"),
        user=os.getenv("DB_USER", "landon"),
        password=os.getenv("DB_PASS", "password123") 
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL database successfully.")
except Exception as e:
    print(f"Error connecting to PostgreSQL database: {e}")
    exit(1)

# -------creating tables-------
#live_flights
query  = """CREATE TABLE IF NOT EXISTS live_flights (
    icao24 VARCHAR(6) Primary Key,
    callsign VARCHAR(10),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    baro_altitude FLOAT,
    velocity FLOAT,
    true_track FLOAT,
    on_ground BOOLEAN,
    last_update TIMESTAMP
);"""
cursor.execute(query)
conn.commit()

#flight history 
query = """CREATE TABLE IF NOT EXISTS flight_history (
    id SERIAL PRIMARY KEY,
    icao24 VARCHAR(6),
    callsign VARCHAR(10),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    baro_altitude FLOAT,
    velocity FLOAT,
    true_track FLOAT,
    on_ground BOOLEAN,
    last_update TIMESTAMP
);"""
cursor.execute(query)
conn.commit()

token = None
#need to build out robust token management strategy later

while True:
    print(f'{datetime.now()}: Starting data collection cycle...')

    #getting access token
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    payload = {"grant_type": "client_credentials", "client_id": os.getenv("OPENSKY_CLIENT_ID"), "client_secret": os.getenv("OPENSKY_CLIENT_SECRET")}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=payload, headers=headers)
    token = response.json().get("access_token")
    
    try:
        #making sure connection is valid
        if conn.closed:
            conn = psycopg2.connect(
                host="localhost",         
                database=os.getenv("DB_NAME", "aviation_db"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", "postgres"),
                port=os.getenv("DB_PORT", "5433"))
            cursor = conn.cursor()
            print("Reconnected to PostgreSQL database successfully.")

        #fetching data
        opensky_url = "https://opensky-network.org/api/states/all"
        params = {"lamin": 37.0, "lomin": -114.0, "lamax": 42.0, "lomax": -109.0} #bounding box for Utah
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.get(opensky_url, params=params, headers=headers)
        data = response.json()
        states = data.get("states")

        #-------queries------
        #flight history
        history_query = """INSERT INTO flight_history (icao24, callsign, latitude, longitude, baro_altitude, velocity, true_track, on_ground, last_update) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        #inserting for history table
        if states is not None:
            for s in states:
                data = (s[0], s[1].strip() if s[1] else None,
                    s[6], s[5], s[7], s[9], s[10], s[8], 
                    datetime.fromtimestamp(s[3]) 
                )
                cursor.execute(history_query, data)
            conn.commit()
            print(f"Inserted/Updated {len(states)} records into live_flights table.")
        else:
            print("No flight data available for the specified area.")

        #live_flights
        live_query = """
        INSERT INTO live_flights (
        icao24, callsign, latitude, longitude, 
            baro_altitude, velocity, true_track, on_ground, last_update
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (icao24) DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            baro_altitude = EXCLUDED.baro_altitude,
            velocity = EXCLUDED.velocity,
            true_track = EXCLUDED.true_track,
            on_ground = EXCLUDED.on_ground,
            last_update = EXCLUDED.last_update;
        """

        #inserting data into postgres
        if states is not None:
            for s in states:
                data = (s[0], s[1].strip() if s[1] else None,
                    s[6], s[5], s[7], s[9], s[10], s[8], 
                    datetime.fromtimestamp(s[3]) 
                )
                cursor.execute(live_query, data)
            conn.commit()
            print(f"Inserted/Updated {len(states)} records into live_flights table.")
        else:
            print("No flight data available for the specified area.")

        #-------cleaning up live_flights table------
        cleanup_query = """DELETE FROM live_flights
        WHERE last_update < (NOW()- interval '7 hours') - INTERVAL '15 minutes';"""
        cursor.execute(cleanup_query)
        conn.commit()

    except Exception as e:
        print("Error during data collection or database operation:", e)
    
    #wait for 5 minutes before next collection
    time.sleep(60)

