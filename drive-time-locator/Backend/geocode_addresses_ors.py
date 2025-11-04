import pandas as pd
import openrouteservice
from dotenv import load_dotenv
import os
from time import sleep

# --- Load API key ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")

if not ORS_API_KEY:
    raise EnvironmentError("ORS_API_KEY not found in environment variables or .env file")

# --- Config ---
INPUT_FILE = "locations.xlsx"
OUTPUT_FILE = "locations_with_coords.xlsx"

client = openrouteservice.Client(key=ORS_API_KEY)

# --- Load Excel ---
df = pd.read_excel(INPUT_FILE)

if "Address" not in df.columns:
    raise ValueError("Your Excel file must have an 'Address' column.")

# Add missing columns
if "Latitude" not in df.columns:
    df["Latitude"] = None
if "Longitude" not in df.columns:
    df["Longitude"] = None

print(f"Starting geocoding for {len(df)} addresses...\n")

for i, row in df.iterrows():
    if pd.notnull(row["Latitude"]) and pd.notnull(row["Longitude"]):
        print(f"Skipping {row['Address']} (already has coordinates)")
        continue

    address = row["Address"]
    print(f"Geocoding: {address}")

    try:
        result = client.pelias_search(text=address)
        if result and result.get("features"):
            coords = result["features"][0]["geometry"]["coordinates"]
            lon, lat = coords
            df.at[i, "Latitude"] = lat
            df.at[i, "Longitude"] = lon
            print(f" → {lat}, {lon}")
        else:
            print(" → Address not found.")
    except Exception as e:
        print(f"Error geocoding {address}: {e}")
        sleep(1)

# --- Save results ---
df.to_excel(OUTPUT_FILE, index=False)
print(f"\n✅ Done! Saved geocoded file to {OUTPUT_FILE}")
