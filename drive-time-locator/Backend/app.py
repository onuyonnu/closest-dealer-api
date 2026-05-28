from flask import Flask, request, jsonify
import json
import pandas as pd
import psycopg
import requests
from openrouteservice import Client
from geopy.geocoders import Nominatim
from flask_cors import CORS
from dotenv import load_dotenv
from slack_sdk.web import WebClient
from slack_sdk.signature import SignatureVerifier
import os
import math
import time
import threading
import logging
from slack_bolt import App as SlackBoltApp
from slack_bolt.adapter.flask import SlackRequestHandler


# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("closest-dealer-api")

# --- Load environment variables ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET")

slack_app = SlackBoltApp(
    token=os.getenv("SLACK_BOT_TOKEN"),
    signing_secret=os.getenv("SLACK_SIGNING_SECRET"),
)



slack_client = None
handler = SlackRequestHandler(slack_app)
signature_verifier = None
if SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET:
    slack_client = WebClient(token=SLACK_BOT_TOKEN)
    signature_verifier = SignatureVerifier(SLACK_SIGNING_SECRET)
    logger.info("Slack integration enabled")
else:
    logger.info("Slack integration disabled - set SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET")
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

# --- Load dealer data ---
EXCEL_FILE = "locations_with_coords.xlsx"

def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(DATABASE_URL, autocommit=True)


def load_dealer_data():
    if DATABASE_URL:
        logger.info("Loading dealer data from Supra SQL database")
        try:
            with get_db_connection() as conn:
                query = """
                    SELECT
                        name AS "Name",
                        phone AS "Phone",
                        address AS "Address",
                        latitude AS "Latitude",
                        longitude AS "Longitude",
                        notes AS "Notes"
                    FROM dealers
                """
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error("Failed to load dealer data from DATABASE_URL: %s", e)
            raise

    logger.warning("DATABASE_URL not set; falling back to Excel file %s", EXCEL_FILE)
    try:
        return pd.read_excel(EXCEL_FILE)
    except FileNotFoundError:
        raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")


df = load_dealer_data()

required_cols = ["Name", "Latitude", "Longitude"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"{col} column missing in dealer data source")

if "Phone" not in df.columns:
    df["Phone"] = ""
if "Notes" not in df.columns:
    df["Notes"] = ""

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

# Geocoding cache and rate limiting
geocode_cache = {}  # Session-level memory cache for performance
GEOCODE_CACHE_MAX_SIZE = 1000
geocode_lock = threading.Lock()
last_geocode_time = 0
GEOCODE_MIN_INTERVAL = 1.0  # Minimum 1 second between ORS geocoding calls


def get_cached_geocode_from_db(address):
    """Query the geocode_cache table in Supra for a cached address."""
    if not DATABASE_URL:
        return None
    try:
        with get_db_connection() as conn:
            result = conn.execute(
                "SELECT latitude, longitude FROM geocode_cache WHERE address = %s",
                (address,)
            ).fetchone()
        if result:
            lat, lon = result
            return {"lat": lat, "lon": lon, "address": address}
        return None
    except Exception as e:
        logger.warning(f"Failed to query geocode_cache from database: {e}")
        return None


def save_geocode_to_db(address, latitude, longitude):
    """Save a geocoded address to the geocode_cache table in Supra."""
    if not DATABASE_URL:
        return
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO geocode_cache (address, latitude, longitude) VALUES (%s, %s, %s) ON CONFLICT (address) DO UPDATE SET latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude",
                (address, latitude, longitude),
            )
        logger.info(f"Cached geocode for '{address}' in database")
    except Exception as e:
        logger.warning(f"Failed to save geocode to database: {e}")


def verify_slack_request(req):
    if not signature_verifier:
        return False
    body = req.get_data()
    return signature_verifier.is_valid_request(body, req.headers)


def save_dealer_to_db(name, phone, address, latitude, longitude, notes):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    try:
        with get_db_connection() as conn:
            conn.execute(
                "INSERT INTO dealers (name, phone, address, latitude, longitude, notes) VALUES (%s, %s, %s, %s, %s, %s)",
                (name, phone, address, latitude, longitude, notes),
            )
        logger.info(f"Saved dealer '{name}' to database")
    except Exception as e:
        logger.error(f"Failed to save dealer to database: {e}")
        raise


def safe_geocode(query, retries=3, delay=1.0):
    """Safe geocoding prioritizing Supra cache, then Nominatim, with ORS as backup."""
    global last_geocode_time
    
    # Check Supra database cache first
    cached_db = get_cached_geocode_from_db(query)
    if cached_db:
        logger.info(f"Using cached geocoding result from database for '{query}'")
        return cached_db
    
    # Check session memory cache
    cache_key = query.strip().lower()
    with geocode_lock:
        if cache_key in geocode_cache:
            cached_result = geocode_cache[cache_key]
            logger.info(f"Using cached geocoding result from memory for '{query}'")
            return cached_result

    # Try Nominatim first
    try:
        geolocator = Nominatim(user_agent="geoapi")
        # For zip codes, try different approaches
        if query.strip().isdigit() and len(query.strip()) == 5:
            # Looks like a US zip code, try with "US" appended
            location = geolocator.geocode(f"{query}, USA", timeout=10)
        else:
            location = geolocator.geocode(query, country_codes="us", timeout=10)

        if location:
            result = {'lat': location.latitude, 'lon': location.longitude, 'address': location.address}
            # Save to database and session cache
            save_geocode_to_db(query, location.latitude, location.longitude)
            with geocode_lock:
                geocode_cache[cache_key] = result
            logger.info(f"Nominatim geocoded '{query}' -> {location.latitude}, {location.longitude}")
            return result
        logger.warning(f"Nominatim could not geocode '{query}'")
    except Exception as e:
        logger.warning(f"Nominatim geocoding failed for '{query}': {e}")
        # If it's a 429 error from Nominatim, add a small delay before falling back
        if "429" in str(e):
            logger.info("Nominatim rate limited, adding delay before ORS fallback")
            time.sleep(2)

    # Fallback to ORS with rate limiting
    if ORS_API_KEY:
        # Rate limiting for ORS
        with geocode_lock:
            current_time = time.time()
            time_since_last = current_time - last_geocode_time
            if time_since_last < GEOCODE_MIN_INTERVAL:
                sleep_time = GEOCODE_MIN_INTERVAL - time_since_last
                logger.info(f"Rate limiting ORS geocoding, sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
            last_geocode_time = time.time()

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
                    result = {"lat": lat, "lon": lon, "address": address}
                    # Save to database and session cache
                    save_geocode_to_db(query, lat, lon)
                    with geocode_lock:
                        if len(geocode_cache) >= GEOCODE_CACHE_MAX_SIZE:
                            oldest_key = next(iter(geocode_cache))
                            del geocode_cache[oldest_key]
                        geocode_cache[cache_key] = result
                    logger.info(f"ORS geocoded '{query}' -> {lat}, {lon}")
                    return result
                logger.warning(f"No features returned for '{query}'")
                return None
            except requests.exceptions.HTTPError as e:
                if r.status_code == 429:
                    logger.warning(f"ORS rate limit hit (429) for '{query}', attempt {attempt+1}")
                    if attempt < retries - 1:
                        # Exponential backoff for rate limits
                        backoff_time = delay * (2 ** attempt)
                        logger.info(f"Backing off for {backoff_time}s before retry")
                        time.sleep(backoff_time)
                        continue
                    else:
                        logger.error(f"ORS rate limit persisted for '{query}' after {retries} attempts")
                        return None
                else:
                    logger.warning(f"ORS HTTP error {r.status_code} for '{query}': {e}")
                    time.sleep(delay * (attempt + 1))
            except Exception as e:
                logger.warning(f"ORS geocoding attempt {attempt+1} failed for '{query}': {e}")
                time.sleep(delay * (attempt + 1))

    logger.error(f"All geocoding services failed for '{query}'")
    return None


def ors_autocomplete(query, retries=3, delay=1.0, limit=5):
    """ORS autocomplete suggestions, results filtered to North America bbox."""
    global last_autocomplete_time
    
    if not ORS_API_KEY:
        logger.warning(f"ORS API key not available for autocomplete")
        return []

    # Simple cache for autocomplete results
    cache_key = f"autocomplete_{query.strip().lower()}_{limit}"
    with geocode_lock:  # Reuse the same lock
        if cache_key in geocode_cache:
            cached_result = geocode_cache[cache_key]
            logger.info(f"Using cached autocomplete result for '{query}'")
            return cached_result

    # Rate limiting
    with autocomplete_lock:
        current_time = time.time()
        time_since_last = current_time - last_autocomplete_time
        if time_since_last < AUTOCOMPLETE_MIN_INTERVAL:
            sleep_time = AUTOCOMPLETE_MIN_INTERVAL - time_since_last
            logger.info(f"Rate limiting autocomplete, sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        last_autocomplete_time = time.time()

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

            # Cache the result
            with geocode_lock:
                if len(geocode_cache) >= GEOCODE_CACHE_MAX_SIZE:
                    oldest_key = next(iter(geocode_cache))
                    del geocode_cache[oldest_key]
                geocode_cache[cache_key] = suggestions

            return suggestions
        except requests.exceptions.HTTPError as e:
            if r.status_code == 429:
                logger.warning(f"ORS autocomplete rate limit hit (429) for '{query}', attempt {attempt+1}")
                if attempt < retries - 1:
                    backoff_time = delay * (2 ** attempt)
                    logger.info(f"Backing off for {backoff_time}s before retry")
                    time.sleep(backoff_time)
                    continue
                else:
                    logger.error(f"ORS autocomplete rate limit persisted for '{query}' after {retries} attempts")
                    return []
            else:
                logger.warning(f"ORS autocomplete HTTP error {r.status_code} for '{query}': {e}")
                time.sleep(delay * (attempt + 1))
        except Exception as e:
            logger.warning(f"Autocomplete attempt {attempt+1} failed for '{query}': {e}")
            time.sleep(delay * (attempt + 1))

    logger.error(f"Autocomplete failed for '{query}' after {retries} attempts with ORS")

    # Fallback to old client
    if client:
        try:
            ors_results = client.pelias_search(text=query, size=limit)
            result = [feature['properties']['label'] for feature in ors_results['features']]
            # Cache the fallback result too
            with geocode_lock:
                if len(geocode_cache) >= GEOCODE_CACHE_MAX_SIZE:
                    oldest_key = next(iter(geocode_cache))
                    del geocode_cache[oldest_key]
                geocode_cache[cache_key] = result
            return result
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

    # Step 1: approximate distances (km) on a request-local copy to avoid mutating global data
    distance_df = df.assign(
        approx_distance=df.apply(
            lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
        )
    )

    # Step 2: choose nearest candidates by approx distance
    candidates = distance_df.sort_values("approx_distance").head(50)

    results = []
    accepted_summaries = []
    skipped_summaries = []

    for _, row in candidates.iterrows():
        dest_lat = row["Latitude"]
        dest_lon = row["Longitude"]

        if pd.isna(dest_lat) or pd.isna(dest_lon):
            skipped_summaries.append(f"{row['Name']}: invalid coordinates")
            continue

        approx_km = row["approx_distance"]
        if pd.isna(approx_km):
            skipped_summaries.append(f"{row['Name']}: invalid distance calculation")
            continue

        approx_km = float(approx_km)
        approx_miles = approx_km * 0.621371
        approx_time_min = approx_km / 80 * 60

        if approx_miles > 500:
            skipped_summaries.append(f"{row['Name']}: too far ({approx_miles:.1f} mi)")
            continue

        phone = str(row.get("Phone", "")) if not pd.isna(row.get("Phone")) else ""

        # Handle optional Notes field
        notes = str(row.get("Notes", "")) if not pd.isna(row.get("Notes")) else ""

        result = {
            "name": str(row["Name"]),
            "phone": phone,
            "drive_time": round(approx_time_min, 1),
            "distance_km": round(approx_km, 2)
        }
        if notes:
            result["notes"] = notes

        results.append(result)

        accepted_summaries.append(
            f"{row['Name']} ({approx_km:.2f} km, {approx_miles:.1f} mi, {approx_time_min:.1f} min)"
        )

    results.sort(key=lambda x: x["drive_time"])
    final_results = results[:5]

    if not final_results:
        accepted_text = "\n".join(f"  - {item}" for item in accepted_summaries) or "  - none"
        skipped_text = "\n".join(f"  - {item}" for item in skipped_summaries) or "  - none"

        logger.info(
            "find-closest candidate summary (no dealers met criteria):\naccepted:\n%s\nskipped:\n%s",
            accepted_text,
            skipped_text,
        )

    logger.info(f"Returning top {len(final_results)} results (approximate only)")
    return jsonify(final_results)


@app.route("/slack/commands", methods=["POST"])
def slack_commands():
    # Slack requires a 200 response within 3 seconds, so return immediately
    try:
        if not verify_slack_request(request):
            logger.warning("Slack request verification failed")
            return "", 200

        command = request.form.get("command")
        trigger_id = request.form.get("trigger_id")
        channel_id = request.form.get("channel_id")

        logger.info(f"Slack command received: {command}")

        if command != "/add_dealer":
            logger.warning(f"Unsupported Slack command: {command}")
            return "", 200
        
        if not slack_client:
            logger.error("Slack bot client is not configured")
            return "", 200

        if not trigger_id:
            logger.error("No trigger_id in Slack request")
            return "", 200

        view = {
            "type": "modal",
            "callback_id": "add_dealer_modal",
            "title": {"type": "plain_text", "text": "Add Dealer"},
            "submit": {"type": "plain_text", "text": "Save"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": channel_id or "",
            "blocks": [
                {
                    "type": "input",
                    "block_id": "name_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "name_input"
                    },
                    "label": {"type": "plain_text", "text": "Dealer name"}
                },
                {
                    "type": "input",
                    "block_id": "phone_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "phone_input"
                    },
                    "label": {"type": "plain_text", "text": "Phone"},
                    "optional": True
                },
                {
                    "type": "input",
                    "block_id": "address_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "address_input"
                    },
                    "label": {"type": "plain_text", "text": "Address"}
                },
                {
                    "type": "input",
                    "block_id": "latitude_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "latitude_input"
                    },
                    "label": {"type": "plain_text", "text": "Latitude"}
                },
                {
                    "type": "input",
                    "block_id": "longitude_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "longitude_input"
                    },
                    "label": {"type": "plain_text", "text": "Longitude"}
                },
                {
                    "type": "input",
                    "block_id": "notes_block",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "notes_input",
                        "multiline": True
                    },
                    "label": {"type": "plain_text", "text": "Notes"},
                    "optional": True
                }
            ]
        }

        slack_client.views_open(trigger_id=trigger_id, view=view)
        logger.info("Slack modal opened successfully")
    except Exception as e:
        logger.error(f"Error in slack_commands: {e}", exc_info=True)
    
    return "", 200

@app.route("/slack/events", methods=["POST"])
def slack_events():
    return handler.handle(request)


@app.route("/slack/interactions", methods=["POST"])
def slack_interactions():
    if not verify_slack_request(request):
        return jsonify({"error": "invalid request"}), 403

    payload = json.loads(request.form.get("payload", "{}"))
    if payload.get("type") == "view_submission" and payload.get("view", {}).get("callback_id") == "add_dealer_modal":
        values = payload["view"]["state"]["values"]
        name = values["name_block"]["name_input"]["value"].strip()
        phone = values["phone_block"]["phone_input"]["value"].strip()
        address = values["address_block"]["address_input"]["value"].strip()
        latitude = values["latitude_block"]["latitude_input"]["value"].strip()
        longitude = values["longitude_block"]["longitude_input"]["value"].strip()
        notes = values["notes_block"]["notes_input"]["value"].strip()

        errors = {}
        try:
            latitude_value = float(latitude)
        except Exception:
            errors["latitude_block"] = "Latitude must be a valid number."
        try:
            longitude_value = float(longitude)
        except Exception:
            errors["longitude_block"] = "Longitude must be a valid number."
        if not name:
            errors["name_block"] = "Dealer name is required."
        if not address:
            errors["address_block"] = "Address is required."

        if errors:
            return jsonify({"response_action": "errors", "errors": errors})

        try:
            save_dealer_to_db(name, phone, address, latitude_value, longitude_value, notes)
            channel_id = payload["view"].get("private_metadata")
            user_id = payload["user"]["id"]
            if slack_client and channel_id:
                slack_client.chat_postEphemeral(
                    channel=channel_id,
                    user=user_id,
                    text=f"Dealer *{name}* has been saved successfully."
                )
        except Exception as e:
            logger.error(f"Error saving dealer from Slack modal: {e}")
            return jsonify({"response_action": "errors", "errors": {"name_block": "Unable to save dealer. Please try again."}})

        return jsonify({"response_action": "clear"})

    return "", 200


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
