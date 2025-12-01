from flask import Flask, request, jsonify
import pandas as pd
# removed openrouteservice usage - using approximate distances only
#from openrouteservice import Client
from geopy.geocoders import Nominatim
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
# client = openrouteservice.Client(key=ORS_API_KEY)  # removed - not used in approximate-only mode
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
                logger.info(f"Geocoded '{query}' -> {location.latitude}, {location.longitude}")
                return location
        except Exception as e:
            logger.warning(f"Geocode attempt {attempt+1} failed for '{query}': {e}")
            time.sleep(delay * (attempt + 1))  # exponential backoff
    logger.error(f"Geocoding failed for '{query}' after {retries} attempts")
    return None
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
    geolocator = Nominatim(user_agent="geoapi")
    # safer throttled geocoding
    location = safe_geocode(geolocator, user_address)
    if not location:
        logger.error(f"Address not found or geocoding service unavailable for '{user_address}'")
        return jsonify({"error": "Address not found or geocoding service unavailable"}), 400
    user_lat, user_lon = location.latitude, location.longitude
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
        logger.error(f"Autocomplete error: {e}")
        return jsonify([])
    if locations:
        suggestions = [loc.address for loc in locations]
        logger.info(f"Autocomplete suggestions: {suggestions}")
    return jsonify(suggestions)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)









