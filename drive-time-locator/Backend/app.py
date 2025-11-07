from flask import Flask, request, jsonify
from flask_cors import CORS
from openrouteservice import Client
import pandas as pd
import time
import math
import os

app = Flask(__name__)
CORS(app)

# --- OpenRouteService API key ---
ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise EnvironmentError("ORS_API_KEY not found in environment variables or .env file")

client = Client(key=ORS_API_KEY)

# --- Load Excel file ---
EXCEL_FILE = "locations_with_coords.xlsx"
try:
    df = pd.read_excel(EXCEL_FILE)
except FileNotFoundError:
    raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")

required_cols = ["Name", "Latitude", "Longitude"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"Missing required column: {col}")

# Add Phone column if missing
if "Phone" not in df.columns:
    df["Phone"] = ""

# --- Haversine function (approx distance in km) ---
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


@app.route("/")
def home():
    return {"message": "Drive Time Locator API is running"}


@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json()
    address = data.get("address", "")

    if not address:
        return jsonify({"error": "Address is required"}), 400

    try:
        # --- Geocode address ---
        geocode = client.pelias_search(text=address, boundary_country=["US"])
        if not geocode or "features" not in geocode or len(geocode["features"]) == 0:
            return jsonify({"error": "Could not geocode address"}), 404

        coords = geocode["features"][0]["geometry"]["coordinates"]
        user_lon, user_lat = coords[0], coords[1]

        if not all(map(math.isfinite, [user_lat, user_lon])):
            return jsonify({"error": "Invalid coordinates"}), 400

        # --- Estimate distances ---
        df["approx_distance"] = df.apply(
            lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]),
            axis=1
        )

        candidates = df[df["approx_distance"] < 500].sort_values("approx_distance").head(10)

        if candidates.empty:
            return jsonify([])

        results = []
        for _, row in candidates.iterrows():
            dest_coords = (row["Longitude"], row["Latitude"])
            approx_km = row["approx_distance"]

            # Debug logs
            print(f"\n--- Route Debug ---")
            print(f"User address: {address}")
            print(f"User coords: ({user_lat}, {user_lon})")
            print(f"Destination: {row['Name']}")
            print(f"Dest coords: {dest_coords} (lat={row['Latitude']}, lon={row['Longitude']})")
            print(f"Approx distance (Haversine): {approx_km:.2f} km")

            try:
                time.sleep(0.75)  # throttle to reduce API rate hits

                route = client.directions(
                    coordinates=[(user_lon, user_lat), dest_coords],
                    profile="driving-car",
                    format="geojson"
                )

                summary = route["features"][0]["properties"]["summary"]
                duration = summary["duration"] / 60  # minutes
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

        return jsonify(sorted(results, key=lambda x: x["drive_time"]))

    except Exception as e:
        print(f"Error processing request: {e}")
        return jsonify({"error": "Internal server error"}), 500


@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("q", "")
    if not query:
        return jsonify([])

    try:
        time.sleep(0.2)  # throttle API requests slightly
        response = client.pelias_autocomplete(text=query, boundary_country=["US"])
        suggestions = [
            f["properties"]["label"]
            for f in response.get("features", [])
            if "label" in f["properties"]
        ]
        return jsonify(suggestions)
    except Exception as e:
        print(f"Autocomplete error: {e}")
        return jsonify([])


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
