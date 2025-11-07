from flask import Flask, request, jsonify
import pandas as pd
import openrouteservice
from geopy.geocoders import Nominatim
from flask_cors import CORS
from dotenv import load_dotenv
import os
import math
import time
import threading

# --- Load environment variables ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise EnvironmentError("ORS_API_KEY not found in environment variables or .env file")

# --- Flask setup ---
app = Flask(__name__)
CORS(app)

# --- ORS client ---
client = openrouteservice.Client(key=ORS_API_KEY)

# --- Load Excel file ---
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

# --- Haversine ---
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# --- Thread lock for Nominatim throttling ---
geocode_lock = threading.Lock()
last_geocode_time = 0

def safe_geocode(geolocator, query, retries=3, delay=1.0):
    """Safe geocoding with throttling, retries, and backoff."""
    global last_geocode_time

    for attempt in range(retries):
        try:
            with geocode_lock:
                elapsed = time.time() - last_geocode_time
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                location = geolocator.geocode(
                    query,
                    country_codes="us",
                    timeout=10
                )
                last_geocode_time = time.time()
            if location:
                return location
        except Exception as e:
            print(f"Geocode attempt {attempt+1} failed for {query}: {e}")
            time.sleep(delay * (attempt + 1))  # exponential backoff
    return None


# --- Routes ---
@app.route("/")
def home():
    return {"message": "Drive Time Locator API is running"}


@app.route("/find-closest", methods=["POST"])
def find_closest():
    try:
        data = request.get_json()
        user_address = data.get("address")

        if not user_address:
            return jsonify({"error": "No address provided"}), 400

        # --- Geocode user address ---
        search = client.pelias_search(text=user_address)
        if not search or not search.get("features"):
            return jsonify({"error": "Address not found"}), 400

        coords = search["features"][0]["geometry"]["coordinates"]
        user_lon, user_lat = coords[0], coords[1]
        print(f"User coords: {user_lat}, {user_lon}")

        # --- Approximate distances ---
        df["approx_distance"] = df.apply(
            lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
        )
        candidates = df.sort_values("approx_distance").head(10)

        results = []
        for _, row in candidates.iterrows():
            dest = (row["Longitude"], row["Latitude"])
            try:
                route = client.directions(
                    coordinates=[(user_lon, user_lat), dest],
                    profile="driving-car",
                    format="geojson"
                )
                summary = route["features"][0]["properties"]["summary"]
                distance = summary["distance"] / 1000
                duration = summary["duration"] / 60

                print(f"{row['Name']} — ORS distance: {distance:.2f} km, duration: {duration:.1f} min")

                results.append({
                    "name": row["Name"],
                    "phone": row.get("Phone", ""),
                    "drive_time": round(duration, 1),
                    "distance_km": round(distance, 2)
                })
            except Exception as e:
                print(f"Error getting route for {row['Name']}: {e}")
                continue

        if not results:
            return jsonify({"error": "No valid routes found"}), 500

        results.sort(key=lambda x: x["drive_time"])
        return jsonify(results[:5])

    except Exception as e:
        print(f"Error processing request: {e}")
        return jsonify({"error": str(e)}), 500
    
@app.route("/autocomplete", methods=["GET"])
def autocomplete():
    query = request.args.get("q", "")
    if not query:
        return jsonify([])

    geolocator = Nominatim(user_agent="geoapi")
    suggestions = []

    try:
        with geocode_lock:
            elapsed = time.time() - last_geocode_time
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            locations = geolocator.geocode(
                query,
                country_codes="us",
                exactly_one=False,
                limit=5,
                timeout=10
            )
    except Exception as e:
        print(f"Autocomplete error: {e}")
        return jsonify([])

    if locations:
        suggestions = [loc.address for loc in locations]

    return jsonify(suggestions)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
