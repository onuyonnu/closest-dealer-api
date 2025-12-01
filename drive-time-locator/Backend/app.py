from flask import Flask, request, jsonify
import pandas as pd
from geopy.distance import geodesic
from flask_cors import CORS
from dotenv import load_dotenv
import os
import math
import logging
import requests

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("closest-dealer-api")

# --- Load environment variables ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise ValueError("ORS_API_KEY not set in environment variables")

# --- Flask setup ---
app = Flask(__name__)
CORS(app)

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

# --- Routes ---
@app.route("/autocomplete", methods=["GET"])
def autocomplete():
    query = request.args.get("q", "")
    logger.info(f"autocomplete called q='{query}'")
    if not query:
        return jsonify([])

    url = "https://api.openrouteservice.org/geocode/autocomplete"
    params = {
        "api_key": ORS_API_KEY,
        "text": query,
        "boundary.country": "US",
        "size": 5
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        suggestions = [feat["properties"]["label"] for feat in data.get("features", [])]
        logger.info(f"Autocomplete suggestions: {suggestions}")
        return jsonify(suggestions)
    except requests.RequestException as e:
        logger.error(f"Autocomplete error: {e}")
        return jsonify([])

@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json() or {}
    user_address = data.get("address")
    logger.info(f"find-closest called from {request.remote_addr} with address: '{user_address}'")

    if not user_address:
        return jsonify({"error": "No address provided"}), 400

    # --- Geocode user address with ORS ---
    url = "https://api.openrouteservice.org/geocode/search"
    params = {
        "api_key": ORS_API_KEY,
        "text": user_address,
        "boundary.country": "US",
        "size": 1
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        if not features:
            return jsonify({"error": "Address not found"}), 400

        user_lat, user_lon = features[0]["geometry"]["coordinates"][1], features[0]["geometry"]["coordinates"][0]
        logger.info(f"User coords: {user_lat}, {user_lon}")
    except requests.RequestException as e:
        logger.error(f"Geocoding error: {e}")
        return jsonify({"error": "Geocoding service unavailable"}), 400

    # --- Approx distances ---
    df["approx_distance"] = df.apply(
        lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
    )

    # --- Candidates ---
    candidates = df.sort_values("approx_distance").head(50)
    results = []
    for _, row in candidates.iterrows():
        dest_lat = row["Latitude"]
        dest_lon = row["Longitude"]
        approx_km = float(row["approx_distance"])
        approx_miles = approx_km * 0.621371
        approx_time_min = approx_km / 80 * 60  # 80 km/h assumed

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
    return jsonify(results[:5])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)








