import requests 
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime
load_dotenv()

#creating cursor object for postgres connection
conn = psycopg2.connect(
    host="localhost",         
    database=os.getenv("DB_NAME", "aviation_db"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "postgres"),
    port=os.getenv("DB_PORT", "5433"))

cursor = conn.cursor()


# creating table.
query  = """CREATE TABLE IF NOT EXISTS live_flights (
    flight_id Serial PRIMARY KEY,
    icao24 VARCHAR(6) Unique not null,
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

#getting acces token
url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

payload = {"grant_type": "client_credentials", "client_id": os.getenv("OPENSKY_CLIENT_ID"), "client_secret": os.getenv("OPENSKY_CLIENT_SECRET")}

headers = {"Content-Type": "application/x-www-form-urlencoded"}

response = requests.post(url, data=payload, headers=headers)

token = response.json().get("access_token")

#fetching data
opensky_url = "https://opensky-network.org/api/states/all"
params = {"lamin": 41.6, "lomin": -112.0, "lamax": 42.0, "lomax": -111.6} #bounding box for Logan
headers = {"Authorization": f"Bearer {token}"}

response = requests.get(opensky_url, params=params, headers=headers)

data = response.json()

states = data.get("states")

#setting up query
upsert_sql = """
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
        cursor.execute(upsert_sql, data)
    conn.commit()
    print(f"Inserted/Updated {len(states)} records into live_flights table.")
else:
    print("No flight data available for the specified area.")




