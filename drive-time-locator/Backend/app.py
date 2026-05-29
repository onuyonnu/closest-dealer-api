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

ALLOWED_CHANNELS = ["C0B6J59PPN1"]  # your channel ID



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


def post_slack_channel_message(client, channel_id, text):
    if not channel_id:
        return
    try:
        client.chat_postMessage(channel=channel_id, text=text)
    except Exception as e:
        logger.warning(f"Slack public channel message failed for channel {channel_id}: {e}")


def send_slack_feedback(client, channel_id, user_id, public_text, private_text=None):
    """Post a public channel message and send a private confirmation to the user."""
    if channel_id:
        post_slack_channel_message(client, channel_id, public_text)

    if private_text is None:
        private_text = "Your change was recorded successfully."

    if channel_id:
        try:
            client.chat_postEphemeral(channel=channel_id, user=user_id, text=private_text)
            return
        except Exception as e:
            logger.warning(f"Slack ephemeral feedback failed for channel {channel_id}: {e}")

    try:
        client.chat_postMessage(channel=user_id, text=private_text)
    except Exception as e:
        logger.error(f"Slack fallback DM failed for user {user_id}: {e}")


def save_dealer_to_db(name, phone, address, notes="", latitude=None, longitude=None):
    if latitude is None or longitude is None:
        location = safe_geocode(address)
        if not location:
            raise RuntimeError("Unable to geocode address. Please try again.")
        latitude, longitude = location["lat"], location["lon"]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dealers (name, phone, address, latitude, longitude, notes)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (name, phone, address, latitude, longitude, notes))

    refresh_dealer_data()
    logger.info(f"Saved dealer '{name}' to database")


def refresh_dealer_data():
    global df
    df = load_dealer_data()
    logger.info("Dealer data refreshed from database")


def get_all_dealers(limit=100):
    if not DATABASE_URL:
        return []
    query = """
        SELECT ctid::text AS dealer_id,
               name,
               phone,
               address,
               latitude,
               longitude,
               notes
        FROM dealers
        ORDER BY name
        LIMIT %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def get_dealer_by_id(dealer_id):
    if not DATABASE_URL:
        return None
    query = """
        SELECT ctid::text AS dealer_id,
               name,
               phone,
               address,
               latitude,
               longitude,
               notes
        FROM dealers
        WHERE ctid::text = %s
        LIMIT 1
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (dealer_id,))
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row))


def update_dealer(dealer_id, name, phone, address, notes="", latitude=None, longitude=None):
    if latitude is None or longitude is None:
        location = safe_geocode(address)
        if not location:
            raise RuntimeError("Unable to geocode address. Please try again.")
        latitude, longitude = location["lat"], location["lon"]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dealers
                SET name = %s,
                    phone = %s,
                    address = %s,
                    latitude = %s,
                    longitude = %s,
                    notes = %s
                WHERE ctid::text = %s
            """, (name, phone, address, latitude, longitude, notes, dealer_id))
    refresh_dealer_data()
    logger.info(f"Updated dealer '{name}' ({dealer_id}) in database")



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
        url = "https://api.heigit.org/geocode/search"

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
    url = "https://api.heigit.org/pelias/v1/autocomplete"

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

def slack_str(value):
    """
    Slack plain_text_input initial_value must always be a string.
    None -> ""
    numbers -> "123.45"
    """
    if value is None:
        return ""
    return str(value)


def safe_view_value(values, block_id, action_id):
    """
    Safely read a value from Slack modal submission state.
    Always returns a stripped string.
    """
    try:
        return (values.get(block_id, {}).get(action_id, {}).get("value") or "").strip()
    except Exception:
        return ""


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


@app.route("/slack/events", methods=["POST"])
@app.route("/slack/interactions", methods=["POST"])
@app.route("/slack/commands", methods=["POST"])
def slack_events():
    return handler.handle(request)



@slack_app.command("/add_dealer")
def open_add_modal(ack, body, client, logger):
    channel_id = body.get("channel_id")

    if channel_id not in ALLOWED_CHANNELS:
        ack("🚫 This command can only be used in #dealer-finder.")
        return

    ack()

    client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "add_dealer_modal",
            "title": {"type": "plain_text", "text": "Add Dealer"},
            "submit": {"type": "plain_text", "text": "Submit"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": channel_id or "",
            "blocks": [
                {
                    "type": "input",
                    "block_id": "name_block",
                    "label": {"type": "plain_text", "text": "Dealer Name"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "name_input",
                        "initial_value": ""
                    }
                },
                {
                    "type": "input",
                    "block_id": "address_block",
                    "label": {"type": "plain_text", "text": "Address"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "address_input",
                        "initial_value": ""
                    }
                },
                {
                    "type": "input",
                    "block_id": "phone_block",
                    "label": {"type": "plain_text", "text": "Phone"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "phone_input",
                        "initial_value": ""
                    }
                },
                {
                    "type": "input",
                    "block_id": "notes_block",
                    "optional": True,
                    "label": {"type": "plain_text", "text": "Notes"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "notes_input",
                        "multiline": True,
                        "initial_value": ""
                    }
                }
            ]
        }
    )



@slack_app.command("/dealer_edit")
def open_dealer_edit_modal(ack, body, client, logger):
    channel_id = body.get("channel_id")
    if channel_id not in ALLOWED_CHANNELS:
        ack("🚫 This command can only be used in #dealer-finder.")
        return

    ack()
    logger.info("Slash command /dealer_edit received")

    dealers = get_all_dealers(limit=100)
    if not dealers:
        client.chat_postEphemeral(
            channel=channel_id,
            user=body.get("user_id"),
            text="No dealers were found to edit."
        )
        return

    options = []
    for dealer in dealers:
        text = dealer["name"]
        if dealer.get("address"):
            text = f"{text} — {dealer['address']}"
        options.append({
            "text": {"type": "plain_text", "text": text[:75]},
            "value": dealer["dealer_id"]
        })

    client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "dealer_edit_select",
            "title": {"type": "plain_text", "text": "Edit Dealer"},
            "submit": {"type": "plain_text", "text": "Continue"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": json.dumps({"channel_id": channel_id}),
            "blocks": [
                {
                    "type": "input",
                    "block_id": "dealer_select_block",
                    "label": {"type": "plain_text", "text": "Select a dealer"},
                    "element": {
                        "type": "static_select",
                        "action_id": "dealer_select",
                        "options": options
                    }
                }
            ]
        }
    )


@slack_app.view("add_dealer_modal")
def handle_add_dealer_modal_submission(ack, body, client, logger):
    values = body["view"]["state"]["values"]

    name = safe_view_value(values, "name_block", "name_input")
    phone = safe_view_value(values, "phone_block", "phone_input")
    address = safe_view_value(values, "address_block", "address_input")
    notes = safe_view_value(values, "notes_block", "notes_input")

    errors = {}

    if not name:
        errors["name_block"] = "Dealer name is required."
    if not address:
        errors["address_block"] = "Address is required."

    latitude_value = None
    longitude_value = None

    if errors:
        ack(response_action="errors", errors=errors)
        return

    try:
        save_dealer_to_db(
            name=name,
            phone=phone,
            address=address,
            notes=notes,
            latitude=latitude_value,
            longitude=longitude_value
        )
        refresh_dealer_data()
        ack()
    except Exception as e:
        logger.exception("Error saving dealer from Slack modal")
        ack(
            response_action="update",
            view={
                "type": "modal",
                "title": {"type": "plain_text", "text": "Save Failed"},
                "close": {"type": "plain_text", "text": "Close"},
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"❌ Could not save dealer.\n`{str(e)}`"
                        }
                    }
                ]
            }
        )
@slack_app.view("dealer_edit_select")
def handle_dealer_select(ack, body, client, logger):
    values = body["view"]["state"]["values"]
    selected = values.get("dealer_select_block", {}).get("dealer_select", {}).get("selected_option")

    if not selected:
        ack(
            response_action="errors",
            errors={"dealer_select_block": "Please choose a dealer to edit."}
        )
        return

    dealer_id = selected["value"]
    dealer = get_dealer_by_id(dealer_id)

    if not dealer:
        ack(
            response_action="update",
            view={
                "type": "modal",
                "title": {"type": "plain_text", "text": "Dealer Not Found"},
                "close": {"type": "plain_text", "text": "Close"},
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "❌ The selected dealer could not be found."
                        }
                    }
                ]
            }
        )
        return

    # Acknowledge with update instead of opening a second modal call
    # This avoids extra timing issues and keeps the same modal flow
    ack(
        response_action="update",
        view={
            "type": "modal",
            "callback_id": "dealer_edit_modal",
            "title": {"type": "plain_text", "text": "Edit Dealer"},
            "submit": {"type": "plain_text", "text": "Save"},
            "close": {"type": "plain_text", "text": "Cancel"},
            "private_metadata": dealer_id,
            "blocks": [
                {
                    "type": "input",
                    "block_id": "name_block",
                    "label": {"type": "plain_text", "text": "Dealer Name"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "name_input",
                        "initial_value": slack_str(dealer.get("name"))
                    }
                },
                {
                    "type": "input",
                    "block_id": "address_block",
                    "label": {"type": "plain_text", "text": "Address"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "address_input",
                        "initial_value": slack_str(dealer.get("address"))
                    }
                },
                {
                    "type": "input",
                    "block_id": "phone_block",
                    "label": {"type": "plain_text", "text": "Phone"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "phone_input",
                        "initial_value": slack_str(dealer.get("phone"))
                    }
                },
                {
                    "type": "input",
                    "block_id": "notes_block",
                    "optional": True,
                    "label": {"type": "plain_text", "text": "Notes"},
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "notes_input",
                        "multiline": True,
                        "initial_value": slack_str(dealer.get("notes"))
                    }
                }
            ]
        }
    )

@slack_app.view("dealer_edit_modal")
def handle_dealer_edit_submission(ack, body, client, logger):
    values = body["view"]["state"]["values"]
    dealer_id = body["view"].get("private_metadata")

    name = safe_view_value(values, "name_block", "name_input")
    phone = safe_view_value(values, "phone_block", "phone_input")
    address = safe_view_value(values, "address_block", "address_input")
    notes = safe_view_value(values, "notes_block", "notes_input")

    errors = {}

    if not name:
        errors["name_block"] = "Dealer name is required."
    if not address:
        errors["address_block"] = "Address is required."

    latitude_value = None
    longitude_value = None


    if errors:
        ack(response_action="errors", errors=errors)
        return

    try:
        update_dealer(
            dealer_id=dealer_id,
            name=name,
            phone=phone,
            address=address,
            notes=notes,
            latitude=latitude_value,
            longitude=longitude_value
        )
        refresh_dealer_data()
        ack()
    except Exception as e:
        logger.exception("Error updating dealer from Slack modal")
        ack(
            response_action="update",
            view={
                "type": "modal",
                "title": {"type": "plain_text", "text": "Update Failed"},
                "close": {"type": "plain_text", "text": "Close"},
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"❌ Could not update dealer.\n`{str(e)}`"
                        }
                    }
                ]
            }
        )
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
