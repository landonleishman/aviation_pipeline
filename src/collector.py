import requests 
import os
from dotenv import load_dotenv

load_dotenv()

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

print(response.json())