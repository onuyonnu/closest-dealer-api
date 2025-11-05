import os
import pandas as pd
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables (for ORS API key)
load_dotenv()

app = Flask(__name__)
CORS(app)

# Get the absolute path of the backend folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_FILE = os.path.join(BASE_DIR, "locations_with_coords.xlsx")

# Load Excel data safely
try:
    df = pd.read_excel(EXCEL_FILE)
except FileNotFoundError:
    raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")

# Verify expected columns exist
expected_cols = {"Name", "Phone", "Address", "Latitude", "Longitude"}
if not expected_cols.issubset(df.columns):
    raise ValueError(f"Excel file missing one of these columns: {expected_cols}")

# OpenRouteService API key
ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise ValueError("Missing ORS_API_KEY environment variable.")

@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "Backend is running."}), 200


@app.route("/find-closest", methods=["POST", "GET"])
def find_closest():
    # Handle both GET (for testing) and POST (for frontend)
    if request.method == "POST":
        data = request.get_json()
        address = data.get("address")
    else:
        address = request.args.get("address")

    if not address:
        return jsonify({"error": "Address not provided"}), 400

    # Geocode user-provided address
    try:
        geo_url = "https://api.openrouteservice.org/geocode/search"
        geo_params = {"api_key": ORS_API_KEY, "text": address}
        geo_res = requests.get(geo_url, params=geo_params)
        geo_data = geo_res.json()

        coords = geo_data["features"][0]["geometry"]["coordinates"]
        user_lon, user_lat = coords
    except Exception as e:
        print("Geocoding error:", e)
        return jsonify({"error": "Could not find address or backend issue."}), 500

    # Compute driving distance to each location
    results = []
    for _, row in df.iterrows():
        try:
            route_url = "https://api.openrouteservice.org/v2/directions/driving-car"
            body = {
                "coordinates": [
                    [user_lon, user_lat],
                    [row["Longitude"], row["Latitude"]],
                ]
            }
            headers = {"Authorization": ORS_API_KEY, "Content-Type": "application/json"}
            route_res = requests.post(route_url, json=body, headers=headers)
            route_data = route_res.json()

            meters = route_data["routes"][0]["summary"]["distance"]
            seconds = route_data["routes"][0]["summary"]["duration"]

            results.append({
                "Name": row["Name"],
                "Phone": row["Phone"],
                "Address": row["Address"],
                "Distance_km": round(meters / 1000, 2),
                "Drive_time_min": round(seconds / 60, 1)
            })
        except Exception as e:
            print(f"Error computing route for {row['Name']}: {e}")
            continue

    if not results:
        return jsonify({"error": "No valid results found."}), 500

    # Sort by shortest drive time
    results.sort(key=lambda x: x["Drive_time_min"])
    top5 = results[:5]

    return jsonify(top5), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
