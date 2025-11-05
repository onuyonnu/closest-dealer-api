from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
from geopy.distance import geodesic
import os

app = Flask(__name__)
CORS(app)

# === Load API key and Excel data ===
ORS_API_KEY = os.getenv("ORS_API_KEY")

EXCEL_FILE = "locations_with_coords.xlsx"
if not os.path.exists(EXCEL_FILE):
    raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")

# Load once at startup
df = pd.read_excel(EXCEL_FILE)

# === Autocomplete endpoint (back-end handled) ===
@app.route("/autocomplete", methods=["GET"])
def autocomplete():
    query = request.args.get("q", "")
    if len(query) < 3:
        return jsonify({"suggestions": []})

    url = "https://api.openrouteservice.org/geocode/autocomplete"
    params = {
        "api_key": ORS_API_KEY,
        "text": query
    }

    try:
        res = requests.get(url, params=params)
        data = res.json()
        suggestions = [f["properties"]["label"] for f in data.get("features", [])]
        return jsonify({"suggestions": suggestions})
    except Exception as e:
        print("Autocomplete error:", e)
        return jsonify({"suggestions": []}), 500

# === Find Closest endpoint ===
@app.route("/find-closest", methods=["POST"])
def find_closest():
    try:
        address = request.json.get("address")
        if not address:
            return jsonify({"error": "Address is required"}), 400

        # Geocode the input address
        geo_url = "https://api.openrouteservice.org/geocode/search"
        geo_params = {"api_key": ORS_API_KEY, "text": address}
        geo_res = requests.get(geo_url, params=geo_params)
        geo_data = geo_res.json()

        features = geo_data.get("features")
        if not features:
            return jsonify({"error": "Address not found"}), 404

        lat = features[0]["geometry"]["coordinates"][1]
        lon = features[0]["geometry"]["coordinates"][0]

        # Calculate straight-line distance for all locations
        df["straight_dist_km"] = df.apply(
            lambda row: geodesic((lat, lon), (row["Latitude"], row["Longitude"])).km,
            axis=1
        )

        # Pre-filter top 20 nearest by straight-line distance
        nearby_df = df.nsmallest(20, "straight_dist_km")

        results = []
        for _, row in nearby_df.iterrows():
            if row["straight_dist_km"] > 5000:
                # Skip if more than 5000 km away (reduces invalid ORS calls)
                continue

            coords = [
                [lon, lat],
                [row["Longitude"], row["Latitude"]]
            ]

            route_url = "https://api.openrouteservice.org/v2/directions/driving-car"
            headers = {"Authorization": ORS_API_KEY, "Content-Type": "application/json"}
            body = {"coordinates": coords}

            try:
                r = requests.post(route_url, json=body, headers=headers)
                r_data = r.json()

                if "routes" not in r_data:
                    print(f"Skipping {row['Name']} — route not found.")
                    continue

                route = r_data["routes"][0]
                distance_km = route["summary"]["distance"] / 1000
                duration_min = route["summary"]["duration"] / 60

                results.append({
                    "name": row["Name"],
                    "phone": row["Phone"],
                    "distance": round(distance_km, 1),
                    "duration": round(duration_min, 1)
                })

            except Exception as e:
                print(f"Error getting route for {row['Name']}: {e}")

        # Sort by travel time and return top 5
        results = sorted(results, key=lambda x: x["duration"])[:5]

        return jsonify(results)

    except Exception as e:
        print("Server error:", e)
        return jsonify({"error": "Server error"}), 500


@app.route("/", methods=["GET"])
def root():
    return jsonify({"status": "OK", "message": "Drive Time Locator backend is running."})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

