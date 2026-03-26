from flask import Flask, request, jsonify
import pandas as pd
import requests
from openrouteservice import Client
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
# North America bounding box: [min_lon, min_lat, max_lon, max_lat]
# Extended to include Caribbean territories like Puerto Rico
NA_BBOX = [-180.0, 5.0, -30.0, 85.0]

# Autocomplete rate limiting
autocomplete_lock = threading.Lock()
last_autocomplete_time = 0
AUTOCOMPLETE_MIN_INTERVAL = 2.0  # Minimum 2 seconds between autocomplete calls

def safe_geocode(query, retries=3, delay=1.0):
    """Safe geocoding prioritizing Nominatim, with ORS as backup."""
    # Try Nominatim first
    try:
        geolocator = Nominatim(user_agent="geoapi")
        location = geolocator.geocode(query, country_codes="us", timeout=10)
        if location:
            logger.info(f"Nominatim geocoded '{query}' -> {location.latitude}, {location.longitude}")
            return {'lat': location.latitude, 'lon': location.longitude, 'address': location.address}
        logger.warning(f"Nominatim could not geocode '{query}'")
    except Exception as e:
        logger.warning(f"Nominatim geocoding failed for '{query}': {e}")

    # Fallback to ORS
    if ORS_API_KEY:
        params = {
            "api_key": ORS_API_KEY,
            "text": query,
            "size": 1,
        }
        url = "https://api.openrouteservice.org/geocode/search"

        for attempt in range(retries):
            try:
                logger.info(f"Attempting ORS geocoding fallback for '{query}'")
                r = requests.get(url, params=params, timeout=10)
                r.raise_for_status()
                data = r.json()
                features = data.get("features", [])
                if features:
                    feature = features[0]
                    lon, lat = feature["geometry"]["coordinates"]
                    address = feature["properties"].get("label")
                    logger.info(f"ORS geocoded '{query}' -> {lat}, {lon}")
                    return {"lat": lat, "lon": lon, "address": address}
                logger.warning(f"No features returned for '{query}'")
                return None
            except Exception as e:
                logger.warning(f"ORS geocoding attempt {attempt+1} failed for '{query}': {e}")
                time.sleep(delay * (attempt + 1))

    logger.error(f"All geocoding services failed for '{query}'")
    return None


def ors_autocomplete(query, retries=3, delay=1.0, limit=5):
    """ORS autocomplete suggestions, results filtered to North America bbox."""
    if not ORS_API_KEY:
        logger.warning(f"ORS API key not available for autocomplete")
        return []

    params = {
        "api_key": ORS_API_KEY,
        "text": query,
        "size": limit * 2,  # Request more to account for filtering
    }
    url = "https://api.openrouteservice.org/geocode/autocomplete"

    for attempt in range(retries):
        try:
            logger.info(f"Autocomplete attempt {attempt+1} for '{query}'")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            suggestions = []
            for feature in data.get('features', []):
                lon, lat = feature['geometry']['coordinates']
                label = feature['properties'].get('label')
                # Filter to North America bounds
                if NA_BBOX[0] <= lon <= NA_BBOX[2] and NA_BBOX[1] <= lat <= NA_BBOX[3]:
                    suggestions.append(label)
                    logger.debug(f"  Included: {label} ({lat}, {lon})")
                else:
                    logger.debug(f"  Filtered out: {label} ({lat}, {lon}) - outside bounds")
                if len(suggestions) >= limit:
                    break
            logger.info(f"ORS autocomplete for '{query}': {len(suggestions)} suggestions (out of {len(data.get('features', []))} features)")
            return suggestions
        except Exception as e:
            logger.warning(f"Autocomplete attempt {attempt+1} failed for '{query}': {e}")
            time.sleep(delay * (attempt + 1))

    logger.error(f"Autocomplete failed for '{query}' after {retries} attempts with ORS")

    # Fallback to old client
    if client:
        try:
            ors_results = client.pelias_search(text=query, size=limit)
            return [feature['properties']['label'] for feature in ors_results['features']]
        except Exception as e:
            logger.warning(f"Fallback client autocomplete failed for '{query}': {e}")

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
    global last_autocomplete_time
    
    query = request.args.get("q", "")
    logger.info(f"autocomplete called q='{query}'")
    if not query:
        return jsonify([])

    # Rate limiting: enforce 2-second interval between requests
    with autocomplete_lock:
        elapsed = time.time() - last_autocomplete_time
        if elapsed < AUTOCOMPLETE_MIN_INTERVAL:
            wait_time = AUTOCOMPLETE_MIN_INTERVAL - elapsed
            logger.info(f"Autocomplete rate limit: waiting {wait_time:.2f}s")
            time.sleep(wait_time)
        last_autocomplete_time = time.time()

    suggestions = ors_autocomplete(query)
    return jsonify(suggestions)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
