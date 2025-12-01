from flask import Flask, request, jsonify
from openrouteservice import Client
import os
import threading
import time
import pandas as pd
import math
import logging

# --- Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("closest-dealer-api")

# --- Flask setup ---
app = Flask(__name__)

# --- ORS client ---
ORS_API_KEY = os.getenv("eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjQxMzZlZTAyN2VhMjRhODI4MDk2NDJiMmNmZjJlODAyIiwiaCI6Im11cm11cjY0In0=")
ors_client = Client(key=ORS_API_KEY)

# --- Thread lock and timestamp for throttling ---
throttle_lock = threading.Lock()
last_request_time = 0
THROTTLE_INTERVAL = 1  # seconds between ORS requests

def throttled_ors_request(func, **kwargs):
    """Throttle ORS requests to avoid exceeding rate limits."""
    global last_request_time
    with throttle_lock:
        elapsed = time.time() - last_request_time
        if elapsed < THROTTLE_INTERVAL:
            time.sleep(THROTTLE_INTERVAL - elapsed)
        result = func(**kwargs)
        last_request_time = time.time()
    return result

# --- Load Excel file ---
EXCEL_FILE = "locations_with_coords.xlsx"
df = pd.read_excel(EXCEL_FILE)

# Ensure columns exist
for col in ["Name", "Latitude", "Longitude"]:
    if col not in df.columns:
        raise ValueError(f"{col} missing in {EXCEL_FILE}")
if "Phone" not in df.columns:
    df["Phone"] = ""

# --- Haversine function ---
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
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    try:
        results = throttled_ors_request(ors_client.pelias_search, text=query, size=5)
        suggestions = [f"{f['properties']['label']}" for f in results.get("features", [])]
        return jsonify(suggestions)
    except Exception as e:
        logger.error(f"Autocomplete error: {e}")
        return jsonify([]), 500

@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json() or {}
    user_address = data.get("address")
    if not user_address:
        return jsonify({"error": "No address provided"}), 400

    try:
        geocode_result = throttled_ors_request(ors_client.pelias_search, text=user_address, size=1)
        features = geocode_result.get("features", [])
        if not features:
            return jsonify({"error": "Address not found"}), 400
        user_lat = features[0]["geometry"]["coordinates"][1]
        user_lon = features[0]["geometry"]["coordinates"][0]
    except Exception as e:
        logger.error(f"Geocoding error: {e}")
        return jsonify({"error": "Geocoding service failed"}), 500

    # Approximate distances
    df["approx_distance_km"] = df.apply(lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1)
    candidates = df.sort_values("approx_distance_km").head(50)

    results = []
    for _, row in candidates.iterrows():
        approx_km = float(row["approx_distance_km"])
        approx_miles = approx_km * 0.621371
        approx_time_hr = approx_km / 80  # hours

        if approx_miles > 500:
            continue

        phone = str(row.get("Phone", "")) if not pd.isna(row.get("Phone")) else ""
        results.append({
            "name": str(row["Name"]),
            "phone": phone,
            "drive_time_hr": round(approx_time_hr, 2),
            "distance_miles": round(approx_miles, 2)
        })

    results.sort(key=lambda x: x["drive_time_hr"])
    return jsonify(results[:5])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)








