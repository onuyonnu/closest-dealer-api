import pandas as pd
import openrouteservice
from dotenv import load_dotenv
import os
from time import sleep
from pathlib import Path

# --- Load API key ---
load_dotenv()
ORS_API_KEY = os.getenv("ORS_API_KEY")

if not ORS_API_KEY:
    raise EnvironmentError("ORS_API_KEY not found in environment variables or .env file")

# --- Config ---
INPUT_FILE = "locations.xlsx"
OUTPUT_FILE = "locations_with_coords.xlsx"

# ensure paths are resolved relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, INPUT_FILE)
# helper to find the first matching column name (case-insensitive, fuzzy contains)
def find_column(df, candidates):
    if df is None:
        return None
    # map lowercased column name -> original column name
    col_map = {str(c).strip().lower(): c for c in df.columns}
    # exact candidate match
    for cand in candidates:
        if cand is None:
            continue
        key = cand.strip().lower()
        if key in col_map:
            return col_map[key]
    # fallback: contains match
    for cand in candidates:
        key = cand.strip().lower()
        for col in df.columns:
            if key in str(col).strip().lower():
                return col
    return None
# --- Add: robust loader that handles .xlsx/.xls/.csv and HTML tables ---
def load_excel_with_engine(path):
    from pathlib import Path
    ext = Path(path).suffix.lower()
    # quick sniff for HTML content
    with open(path, "rb") as fh:
        start = fh.read(512)
    if start.lstrip().startswith(b"<"):
        try:
            tables = pd.read_html(path)
            if tables:
                print("Parsed HTML file; using first table as DataFrame.")
                return tables[0]
            raise RuntimeError("No tables found in HTML file.")
        except Exception as e:
            raise RuntimeError(f"Failed reading HTML file: {e}") from e

    if ext == ".xlsx":
        engine = "openpyxl"
    elif ext == ".xls":
        engine = "xlrd"
    elif ext == ".csv":
        return pd.read_csv(path)
    else:
        engine = None

    try:
        if engine:
            return pd.read_excel(path, engine=engine)
        return pd.read_excel(path)
    except Exception as e:
        # last-resort try read_html
        try:
            tables = pd.read_html(path)
            if tables:
                return tables[0]
        except Exception:
            pass
        raise RuntimeError(f"Failed reading '{path}': {e}") from e

# --- Add: initialize OpenRouteService client ---
client = openrouteservice.Client(key=ORS_API_KEY)

# --- Load Excel ---
if not os.path.exists(INPUT_PATH):
    raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")
df = load_excel_with_engine(INPUT_PATH)

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
