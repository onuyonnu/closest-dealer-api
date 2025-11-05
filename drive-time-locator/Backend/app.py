import os
import math
import time
import pandas as pd
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app)

# --- CONFIG ---
ORS_API_KEY = os.getenv("ORS_API_KEY")
EXCEL_FILE = "locations_with_coords.xlsx"

# --- Load Excel file ---
try:
    df = pd.read_excel(EXCEL_FILE)
    print(f"✅ Loaded {len(df)} locations from {EXCEL_FILE}")
except FileNotFoundError:
    raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")

# --- In-memory cache (autocomplete results) ---
autocomplete_cache = {}
CACHE_TTL = 3600  # 1 hour


# --- Utility: Haversine distance fallback ---
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))


# --- Helper: Geocode address ---
def get_coordinates(address):
    url = "https://api.openrouteservice.org/geocode/search"
    params = {"api_key": ORS_API_KEY, "text": address}
    res = requests.get(url, params=params)
    data = res.json()
    if "features" not in data or not data["features"]:
        return None
    coords = data["features"][0]["geometry"]["coordinates"]
    return coords[1], coords[0]


# --- Helper: Get driving route ---
def get_route(lat1, lon1, lat2, lon2):
    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {"Authorization": ORS_API_KEY, "Content-Type": "application/json"}
    body = {"coordinates": [[lon1, lat1], [lon2, lat2]]}
    res = requests.post(url, json=body, headers=headers)
    if res.status_code != 200:
        return None
    return res.json()


# --- AUTOCOMPLETE Endpoint (with caching) ---
@app.route("/autocomplete", methods=["GET"])
def autocomplete():
    query = request.args.get("q", "").strip()
    if len(query) < 3:
        return jsonify({"suggestions": []})

    # Check cache
    cached = autocomplete_cache.get(query.lower())
    now = time.time()
    if cached and now - cached["time"] < CACHE_TTL:
        return jsonify({"suggestions": cached["data"]})

    url = "https://api.openrouteservice.org/geocode/autocomplete"
    params = {"api_key": ORS_API_KEY, "text": query}

    try:
        res = requests.get(url, params=params)
        data = res.json()
        suggestions = [f["properties"]["label"] for f in data.get("features", [])]

        # Cache it
        autocomplete_cache[query.lower()] = {"data": suggestions, "time": now}

        return jsonify({"suggestions": suggestions})
    except Exception as e:
        print("Autocomplete error:", e)
        return jsonify({"suggestions": []}), 500


# --- FIND CLOSEST Endpoint ---
@app.route("/find-closest", methods=["POST", "GET"])
def find_closest():
    data = request.get_json() or request.args
    user_address = data.get("address")

    if not user_address:
        return jsonify({"error": "Address missing"}), 400

    coords = get_coordinates(user_address)
    if not coords:
        return jsonify({"error": "Could not geocode address"}), 400

    user_lat, user_lon = coords
    results = []

    for _, row in df.iterrows():
        name = row["Name"]
        phone = row.get("Phone", "N/A")
        dest_lat = row["Latitude"]
        dest_lon = row["Longitude"]

        if pd.isna(dest_lat) or pd.isna(dest_lon):
            continue

        try:
            route = get_route(user_lat, user_lon, dest_lat, dest_lon)
            if not route or "routes" not in route:
                print(f"Error getting route for {name}: missing route, using fallback")
                distance_km = haversine(user_lat, user_lon, dest_lat, dest_lon)
                duration_min = distance_km / 80 * 60  # average 80 km/h
            else:
                summary = route["routes"][0]["summary"]
                distance_km = summary["distance"] / 1000
                duration_min = summary["duration"] / 60

            if distance_km > 2000:
                print(f"Skipping {name} — too far ({distance_km:.1f} km)")
                continue

            results.append({
                "Name": name,
                "Phone": phone,
                "distance_km": distance_km,
                "duration_min": duration_min,
                "distance_text": f"{distance_km:.1f} km",
                "duration_text": f"{duration_min:.0f} min"
            })

        except Exception as e:
            print(f"Error getting route for {name}: {e}")
            continue

    results.sort(key=lambda x: x["duration_min"])
    return jsonify({"closest": results[:5]})


# --- Health check route ---
@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Backend is running"}), 200


if __name__ == "__main__":
    app.run(debug=True)

