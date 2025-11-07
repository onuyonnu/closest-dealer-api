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
EXCEL_FILE = "locations_with_cords.xlsx"
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
    data = request.get_json()
    user_address = data.get("address")

    if not user_address:
        return jsonify({"error": "No address provided"}), 400

    geolocator = Nominatim(user_agent="geoapi")

    # safer throttled geocoding
    location = safe_geocode(geolocator, user_address)
    if not location:
        return jsonify({"error": "Address not found or geocoding service unavailable"}), 400

    user_lat, user_lon = location.latitude, location.longitude

    # Step 1: approximate distances
    df["approx_distance"] = df.apply(
        lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
    )

    # Step 2: top 10 nearest candidates
    candidates = df.sort_values("approx_distance").head(10)

    results = []
    for _, row in candidates.iterrows():
        dest_coords = (row["Longitude"], row["Latitude"])
        approx_km = row["approx_distance"]
    
        # Log diagnostic info for each candidate
        print(f"\n--- Route Debug ---")
        print(f"User address: {user_address}")
        print(f"User coords: ({user_lat}, {user_lon})")
        print(f"Destination: {row['Name']}")
        print(f"Dest coords: {dest_coords} (lat={row['Latitude']}, lon={row['Longitude']})")
        print(f"Approx distance (Haversine): {approx_km:.2f} km")
    
        try:
            route = client.directions(
                coordinates=[(user_lon, user_lat), dest_coords],
                profile="driving-car",
                format="geojson"
            )
    
            summary = route["features"][0]["properties"]["summary"]
            duration = summary["duration"] / 60  # min
            distance = summary["distance"] / 1000  # km
    
            print(f"ORS route distance: {distance:.2f} km, duration: {duration:.2f} min")
    
            results.append({
                "name": row["Name"],
                "phone": row.get("Phone", ""),
                "drive_time": round(duration, 1),
                "distance_km": round(distance, 2)
            })
    
        except Exception as e:
            print(f"Error getting route for {row['Name']}: {e}")


            # fallback: approximate drive time (80 km/h)
            fallback_time = row["approx_distance"] / 80 * 60
            results.append({
                "name": row["Name"],
                "phone": row.get("Phone", ""),
                "drive_time": round(fallback_time, 1),
                "distance_km": round(row["approx_distance"], 2)
            })

    results.sort(key=lambda x: x["drive_time"])
    return jsonify(results[:5])


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
