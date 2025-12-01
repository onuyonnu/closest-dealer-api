from flask import Flask, request, jsonify
import pandas as pd
from flask_cors import CORS
from dotenv import load_dotenv
import os
import math
import time
import threading
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import socket

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("closest-dealer-api")

# --- Load environment variables ---
load_dotenv()

# --- Flask setup ---
app = Flask(__name__)
CORS(app)

# ==========================
#   FORCED IPV4 SESSION
# ==========================

class IPv4Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        kwargs["socket_options"] = [
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ]
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs["socket_options"] = [
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ]
        return super().proxy_manager_for(*args, **kwargs)

# global IPv4-enforced session
session = requests.Session()

retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)

adapter = IPv4Adapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# ==========================
#   LOAD EXCEL FILE
# ==========================

EXCEL_FILE = "locations_with_coords.xlsx"
try:
    df = pd.read_excel(EXCEL_FILE)
except FileNotFoundError:
    raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")

required_cols = ["Name", "Latitude", "Longitude"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"{col} column missing in {EXCEL_FILE}")

if "Phone" not in df.columns:
    df["Phone"] = ""

# ==========================
#   HAVERSINE
# ==========================

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ==========================
#  NOMINATIM GEOCODING
# ==========================

geocode_lock = threading.Lock()
last_geocode_time = 0

def geocode_address(query):
    """Direct Nominatim call via IPv4 session (no geopy)."""
    global last_geocode_time

    for attempt in range(3):

        try:
            with geocode_lock:
                elapsed = time.time() - last_geocode_time
                if elapsed < 1:
                    time.sleep(1 - elapsed)

                params = {
                    "q": query,
                    "format": "json",
                    "limit": 1,
                    "countrycodes": "us"
                }

                r = session.get(NOMINATIM_URL, params=params, timeout=10)
                last_geocode_time = time.time()

            r.raise_for_status()
            data = r.json()

            if not data:
                return None

            return float(data[0]["lat"]), float(data[0]["lon"])

        except Exception as e:
            logger.warning(f"Geocode attempt {attempt+1} failed for '{query}': {e}")
            time.sleep((attempt + 1) * 1)

    logger.error(f"Geocoding failed for '{query}' after retries")
    return None

# ==========================
#   ROUTES
# ==========================

@app.route("/")
def home():
    return {"message": "Drive Time Locator API is running"}

@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json() or {}
    user_address = data.get("address")
    logger.info(f"find-closest called from {request.remote_addr} with address: '{user_address}'")

    if not user_address:
        return jsonify({"error": "No address provided"}), 400

    # --- geocode user address ---
    result = geocode_address(user_address)
    if not result:
        return jsonify({"error": "Address not found or geocoding unavailable"}), 400

    user_lat, user_lon = result
    logger.info(f"User coords: {user_lat}, {user_lon}")

    # --- find distances using haversine ---
    df["approx_distance"] = df.apply(
        lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
    )

    candidates = df.sort_values("approx_distance").head(50)

    results = []
    for _, row in candidates.iterrows():
        dest_lat = row["Latitude"]
        dest_lon = row["Longitude"]

        if pd.isna(dest_lat) or pd.isna(dest_lon):
            continue

        approx_km = float(row["approx_distance"])
        approx_miles = approx_km * 0.621371
        approx_time_min = approx_km / 80 * 60

        if approx_miles > 500:
            continue

        phone = str(row.get("Phone", "")) if not pd.isna(row.get("Phone")) else ""

        results.append({
            "name": str(row["Name"]),
            "phone": phone,
            "drive_time": round(approx_time_min, 1),
            "distance_km": round(approx_km, 2)
        })

    results.sort(key=lambda x: x["drive_time"])
    final_results = results[:5]

    logger.info(f"Returning {len(final_results)} results")
    return jsonify(final_results)

@app.route("/autocomplete", methods=["GET"])
def autocomplete():
    query = request.args.get("q", "")
    logger.info(f"autocomplete called q='{query}'")

    if not query:
        return jsonify([])

    global last_geocode_time

    with geocode_lock:
        elapsed = time.time() - last_geocode_time
        if elapsed < 1:
            time.sleep(1 - elapsed)

        params = {
            "q": query,
            "format": "json",
            "limit": 5,
            "countrycodes": "us"
        }

        try:
            r = session.get(NOMINATIM_URL, params=params, timeout=10)
            last_geocode_time = time.time()
        except Exception as e:
            logger.error(f"Autocomplete error: {e}")
            return jsonify([])

    try:
        data = r.json()
    except:
        return jsonify([])

    suggestions = [entry["display_name"] for entry in data]

    logger.info(f"Autocomplete suggestions: {suggestions}")
    return jsonify(suggestions)

# ==========================
#   RUN SERVER
# ==========================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

