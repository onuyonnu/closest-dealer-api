from flask import Flask, request, jsonify
import pandas as pd
import openrouteservice
from geopy.geocoders import Nominatim
from flask_cors import CORS
from dotenv import load_dotenv
import os
import math

# --- Load environment variables ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")
if not ORS_API_KEY:
    raise EnvironmentError("ORS_API_KEY not found in environment variables or .env file")

# --- Flask setup ---
app = Flask(__name__)
CORS(app)  # allow frontend calls

# --- ORS client ---
client = openrouteservice.Client(key=ORS_API_KEY)

# --- Load Excel file ---
EXCEL_FILE = "locations_with_coords.xlsx"
try:
    df = pd.read_excel(EXCEL_FILE)
except FileNotFoundError:
    raise FileNotFoundError(f"{EXCEL_FILE} not found in backend folder.")

# --- Ensure necessary columns exist ---
required_cols = ["Name", "Latitude", "Longitude"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"{col} column missing in {EXCEL_FILE}")

# Add Phone column if missing
if "Phone" not in df.columns:
    df["Phone"] = ""

# --- Haversine function for approximate distance ---
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# --- Routes ---
@app.route("/")
def home():
    return {"message": "Drive Time Locator API is running"}

@app.route("/find-closest", methods=["POST"])
def find_closest():
    data = request.get_json()
    user_address = data.get("address")

    if not user_address:
        return jsonify({"error": "No address provided"}), 400

    # Geocode user address
    geolocator = Nominatim(user_agent="geoapi")
    location = geolocator.geocode(user_address)
    if not location:
        return jsonify({"error": "Address not found"}), 400

    user_lat, user_lon = location.latitude, location.longitude

    # Step 1: approximate distances
    df["approx_distance"] = df.apply(
        lambda row: haversine(user_lat, user_lon, row["Latitude"], row["Longitude"]), axis=1
    )

    # Step 2: select top 10 nearest candidates
    candidates = df.sort_values("approx_distance").head(10)

    results = []
    for _, row in candidates.iterrows():
        dest_coords = (row["Longitude"], row["Latitude"])
        try:
            route = client.directions(
                coordinates=[(user_lon, user_lat), dest_coords],
                profile="driving-car",
                format="geojson"
            )
            duration = route["features"][0]["properties"]["summary"]["duration"] / 60  # min
            distance = route["features"][0]["properties"]["summary"]["distance"] / 1000  # km

            results.append({
                "name": row["Name"],
                "phone": row.get("Phone", ""),
                "drive_time": round(duration, 1),
                "distance_km": round(distance, 2)
            })
        except Exception as e:
            print(f"Error getting route for {row['Name']}: {e}")

    # Step 3: sort by actual drive time
    results.sort(key=lambda x: x["drive_time"])

    # Step 4: return top 5
    return jsonify(results[:5])

# --- Run ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

