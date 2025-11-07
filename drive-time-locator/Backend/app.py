from flask import Flask, request, jsonify
from flask_cors import CORS
from openrouteservice import Client
import pandas as pd
import time
import math
import os

app = Flask(__name__)
CORS(app)

# OpenRouteService API key
ORS_API_KEY = os.getenv("ORS_API_KEY")
client = Client(key=ORS_API_KEY)

# Load dealer data
DEALERS_CSV = "dealers.csv"
dealers = pd.read_csv(DEALERS_CSV)

# Haversine formula for approximate distance
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json()
    address = data.get("address", "")

    if not address:
        return jsonify({"error": "Address is required"}), 400

    try:
        # Geocode address using OpenRouteService
        geocode = client.pelias_search(text=address, boundary_country=["US"])
        if not geocode or "features" not in geocode or len(geocode["features"]) == 0:
            return jsonify({"error": "Could not geocode address"}), 404

        coords = geocode["features"][0]["geometry"]["coordinates"]
        user_lon, user_lat = coords[0], coords[1]

        # Check for invalid coordinates
        if not all(map(math.isfinite, [user_lat, user_lon])):
            return jsonify({"error": "Invalid coordinates"}), 400

        # Filter dealers within 500 km using haversine distance
        dealers["approx_distance"] = dealers.apply(
            lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]),
            axis=1
        )

        candidates = dealers[dealers["approx_distance"] < 500].sort_values("approx_distance").head(10)

        if candidates.empty:
            return jsonify([])

        results = []
        for _, row in candidates.iterrows():
            dest_coords = (row["Longitude"], row["Latitude"])
            approx_km = row["approx_distance"]

            # Debugging info
            print(f"\n--- Route Debug ---")
            print(f"User address: {address}")
            print(f"User coords: ({user_lat}, {user_lon})")
            print(f"Destination: {row['Name']}")
            print(f"Dest coords: {dest_coords} (lat={row['Latitude']}, lon={row['Longitude']})")
            print(f"Approx distance (Haversine): {approx_km:.2f} km")

            try:
                # Add a delay to prevent hitting API rate limits
                time.sleep(0.75)

                # Request driving route (no timeout param, handled via default retries)
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
        time.sleep(0.2)  # Slight delay to reduce spam to API
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
    app.run(host="0.0.0.0", port=5000)
