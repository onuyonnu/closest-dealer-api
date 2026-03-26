from flask import Flask, request, jsonify
import pandas as pd
from openrouteservice import Client
from flask_cors import CORS
from dotenv import load_dotenv
import os
import math
import time
import threading
import logging

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("closest-dealer-api")

# --- Load environment variables ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")
# ORS_API_KEY is optional now because we use approximate-only mode locally

# --- Flask setup ---
app = Flask(__name__)
CORS(app)

# --- ORS client ---
client = None
if ORS_API_KEY:
    client = Client(key=ORS_API_KEY)
    logger.info("ORS client initialized for geocoding fallback")
else:
    logger.info("ORS API key not found - geocoding fallback disabled")

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

# --- Geocoding with ORS ---
def safe_geocode(query, retries=3, delay=1.0):
    """Safe geocoding using ORS with retries and backoff, restricted to US."""
    if not client:
        logger.error(f"ORS client not available for geocoding '{query}'")
        return None

    for attempt in range(retries):
        try:
            logger.info(f"Attempting ORS geocoding for '{query}'")
            ors_result = client.pelias_search(text=query, size=1, boundary={'country': 'US'})
            if ors_result['features']:
                feature = ors_result['features'][0]
                lon, lat = feature['geometry']['coordinates']  # GeoJSON: [lon, lat]
                address = feature['properties']['label']
                logger.info(f"ORS geocoded '{query}' -> {lat}, {lon}")
                return {'lat': lat, 'lon': lon, 'address': address}
        except Exception as e:
            logger.warning(f"Geocode attempt {attempt+1} failed for '{query}': {e}")
            time.sleep(delay * (attempt + 1))  # exponential backoff
    
    logger.error(f"Geocoding failed for '{query}' after {retries} attempts with ORS")
    return None


def ors_autocomplete(query, retries=3, delay=1.0, limit=5):
    """ORS autocomplete suggestions, restricted to US."""
    if not client:
        logger.warning(f"ORS client not available for autocomplete")
        return []

    for attempt in range(retries):
        try:
            logger.info(f"Autocomplete attempt {attempt+1} for '{query}'")
            ors_results = client.pelias_search(text=query, size=limit, boundary={'country': 'US'})
            suggestions = [feature['properties']['label'] for feature in ors_results['features']]
            logger.info(f"ORS autocomplete for '{query}': {len(suggestions)} suggestions")
            return suggestions
        except Exception as e:
            logger.warning(f"Autocomplete attempt {attempt+1} failed for '{query}': {e}")
            time.sleep(delay * (attempt + 1))  # exponential backoff
    
    logger.error(f"Autocomplete failed for '{query}' after {retries} attempts with ORS")
    return []


# --- Routes ---
@app.route("/")
def home():
    return {"message": "Drive Time Locator API is running"}


@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json() or {}
    user_address = data.get("address")
    logger.info(f"find-closest called from {request.remote_addr} with address: '{user_address}'")

    if not user_address:
        logger.warning("No address provided in request")
        return jsonify({"error": "No address provided"}), 400

    # Geocode using ORS
    location = safe_geocode(user_address)
    if not location:
        logger.error(f"Address not found or geocoding service unavailable for '{user_address}'")
        return jsonify({"error": "Address not found or geocoding service unavailable"}), 400

    user_lat, user_lon = location['lat'], location['lon']
    logger.info(f"User coords: {user_lat}, {user_lon}")

    # Step 1: approximate distances (km)
    df["approx_distance"] = df.apply(
        lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
    )

    # Step 2: choose nearest candidates by approx distance (increase window slightly then filter)
    candidates = df.sort_values("approx_distance").head(50)

    results = []
    for _, row in candidates.iterrows():
        dest_lat = row["Latitude"]
        dest_lon = row["Longitude"]
        
        # Skip entries with invalid coordinates
        if pd.isna(dest_lat) or pd.isna(dest_lon):
            logger.warning(f"Skipping '{row['Name']}' - invalid coordinates")
            continue

        approx_km = float(row["approx_distance"])
        # Skip if distance calculation failed
        if pd.isna(approx_km):
            logger.warning(f"Skipping '{row['Name']}' - invalid distance calculation")
            continue

        approx_miles = approx_km * 0.621371
        approx_time_min = approx_km / 80 * 60

        # Log approximate values for each candidate
        logger.info(
            f"Candidate '{row['Name']}' at ({dest_lat}, {dest_lon}) -> approx {approx_km:.2f} km "
            f"({approx_miles:.1f} mi), approx_time {approx_time_min:.1f} min"
        )

        # Skip candidates over 500 miles
        if approx_miles > 500:
            logger.info(f"Skipping '{row['Name']}' (approx {approx_miles:.1f} mi > 500 mi)")
            continue

        # Clean phone number - replace NaN with empty string
        phone = str(row.get("Phone", "")) if not pd.isna(row.get("Phone")) else ""

        # Use approximate values directly (no ORS routing)
        results.append({
            "name": str(row["Name"]),  # Convert to string to handle numeric names
            "phone": phone,
            "drive_time": round(approx_time_min, 1),
            "distance_km": round(approx_km, 2)
        })

    results.sort(key=lambda x: x["drive_time"])
    final_results = results[:5]
    logger.info(f"Returning top {len(final_results)} results (approximate only)")
    return jsonify(final_results)


@app.route("/autocomplete", methods=["GET"])
def autocomplete():
    query = request.args.get("q", "")
    logger.info(f"autocomplete called q='{query}'")
    if not query:
        return jsonify([])

    suggestions = ors_autocomplete(query)
    return jsonify(suggestions)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
