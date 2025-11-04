from flask import Flask, request, jsonify
import pandas as pd
import openrouteservice
from geopy.geocoders import Nominatim
from flask_cors import CORS
from dotenv import load_dotenv
import os

# --- Load environment variables ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")

if not ORS_API_KEY:
    raise EnvironmentError("ORS_API_KEY not found in environment variables or .env file")

# --- Flask setup ---
app = Flask(__name__)
CORS(app)  # allows frontend (e.g., GitHub Pages) to access this API

# --- Setup ORS client and data file ---
client = openrouteservice.Client(key=ORS_API_KEY)
EXCEL_FILE = "locations.xlsx"

try:
    df = pd.read_excel(EXCEL_FILE)
except FileNotFoundError:
    raise FileNotFoundError(f"{EXCEL_FILE} not found. Please add your Excel file to the backend folder.")

@app.route("/")
def home():
    return {"message": "Drive Time Locator API is running"}

@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json()
    user_address = data.get("address")

    if not user_address:
        return jsonify({"error": "No address provided"}), 400

    # Geocode the user's address
    geolocator = Nominatim(user_agent="geoapi")
    location = geolocator.geocode(user_address)
    if not location:
        return jsonify({"error": "Address not found"}), 400

    user_coords = (location.longitude, location.latitude)
    results = []

    for _, row in df.iterrows():
        dest_coords = (row["Longitude"], row["Latitude"])
        try:
            route = client.directions(
                coordinates=[user_coords, dest_coords],
                profile="driving-car",
                format="geojson"
            )
            duration = route["features"][0]["properties"]["summary"]["duration"] / 60  # minutes
            distance = route["features"][0]["properties"]["summary"]["distance"] / 1000  # km
            results.append({
                "name": row["Name"],
                "drive_time": round(duration, 1),
                "distance_km": round(distance, 2)
            })
        except Exception as e:
            print(f"Error getting route for {row['Name']}: {e}")

    results.sort(key=lambda x: x["drive_time"])
    return jsonify(results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
