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

# --- Helpers to find columns robustly ---
def find_column(df, candidates):
    if df is None:
        return None
    for c in candidates:
        if c in df.columns:
            return c
    # case-insensitive match
    lc = {col.lower(): col for col in df.columns}
    for c in candidates:
        if c and c.lower() in lc:
            return lc[c.lower()]
    return None

# --- Load Excel ---
df = pd.read_excel(INPUT_FILE)

# Identify source columns
account_col = find_column(df, ["Account Name", "Account", "Name"])
phone_col = find_column(df, ["Phone", "Telephone", "Billing Phone", "Contact Phone"])
street_col = find_column(df, ["Billing Street", "Street", "Address", "Billing Address"])
city_col = find_column(df, ["Billing City", "City"])
state_col = find_column(df, ["Billing State/Province", "State", "Province"])
zip_col = find_column(df, ["Billing Zip/Postal Code", "Zip", "Postal Code", "Zip/Postal Code"])
existing_address_col = find_column(df, ["Address", "Billing Address", "Billing Street"])

# Build Address column if not present (combine billing parts)
def build_address(row):
    parts = []
    for col in (street_col, city_col, state_col, zip_col):
        if col and pd.notna(row.get(col)):
            part = str(row.get(col)).strip()
            if part:
                parts.append(part)
    return ", ".join(parts) if parts else pd.NA

if existing_address_col:
    # prefer existing Address column
    df["Address"] = df[existing_address_col].astype("string")
else:
    df["Address"] = df.apply(build_address, axis=1)

# Normalize Name and Phone columns into standard columns
if account_col:
    df["Name"] = df[account_col].astype("string")
else:
    # try existing 'Name' if present, else empty
    df["Name"] = df.get("Name", pd.Series([pd.NA]*len(df))).astype("string")

if phone_col:
    df["Phone"] = df[phone_col].astype("string")
else:
    df["Phone"] = df.get("Phone", pd.Series([pd.NA]*len(df))).astype("string")

# Ensure Latitude/Longitude columns exist
if "Latitude" not in df.columns:
    df["Latitude"] = pd.NA
if "Longitude" not in df.columns:
    df["Longitude"] = pd.NA

print(f"Starting geocoding for {len(df)} rows (will preserve Name, Phone, Address)...\n")

# --- Geocode loop with retries and throttling ---
for i, row in df.iterrows():
    address = row.get("Address")
    # Skip if no usable address
    if address is pd.NA or pd.isna(address) or str(address).strip() == "":
        print(f"Row {i}: no address, skipping")
        continue

    # If already have coords, skip
    if pd.notna(row.get("Latitude")) and pd.notna(row.get("Longitude")):
        print(f"Row {i}: '{row.get('Name')}' already has coordinates, skipping")
        continue

    print(f"Row {i}: Geocoding: {address}")
    coords_found = False
    for attempt in range(3):
        try:
            res = client.pelias_search(text=str(address))
            if res and res.get("features"):
                coords = res["features"][0]["geometry"]["coordinates"]
                lon, lat = coords[0], coords[1]
                df.at[i, "Latitude"] = lat
                df.at[i, "Longitude"] = lon
                print(f" → {lat}, {lon}")
                coords_found = True
                break
            else:
                print(" → Address not found (no features)")
                break
        except Exception as e:
            print(f"Error geocoding (attempt {attempt+1}) {address}: {e}")
            sleep(1 + attempt)
    if not coords_found:
        continue
    sleep(1)

# --- Fill empty Name/Phone/Address with "N/A" (after geocoding so address lookup isn't blocked) ---
df["Name"] = df["Name"].where(df["Name"].notna() & (df["Name"].astype(str).str.strip() != ""), "N/A")
df["Phone"] = df["Phone"].where(df["Phone"].notna() & (df["Phone"].astype(str).str.strip() != ""), "N/A")
df["Address"] = df["Address"].where(df["Address"].notna() & (df["Address"].astype(str).str.strip() != ""), "N/A")

# Prepare output with only required columns
out_cols = ["Name", "Phone", "Address", "Latitude", "Longitude"]
out_df = df.copy()
# Convert lat/lon to float or None
out_df["Latitude"] = out_df["Latitude"].apply(lambda v: None if pd.isna(v) else float(v))
out_df["Longitude"] = out_df["Longitude"].apply(lambda v: None if pd.isna(v) else float(v))
out_df = out_df[out_cols]

# --- Save results ---
out_df.to_excel(OUTPUT_FILE, index=False)
print(f"\n✅ Done! Saved cleaned & geocoded file to {OUTPUT_FILE}")
