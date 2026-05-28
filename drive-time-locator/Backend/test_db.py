#!/usr/bin/env python3
"""Test script to verify Supa database connectivity and table structure."""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print("❌ DATABASE_URL not set in .env file")
    sys.exit(1)

try:
    import psycopg
except ImportError:
    print("❌ psycopg not installed. Run: pip install psycopg[binary]")
    sys.exit(1)

print(f"Testing connection to: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'database'}")
print()

try:
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    print("✓ Connected to database")
    
    # Test dealers table
    print("\n--- Dealers Table ---")
    try:
        result = conn.execute("SELECT COUNT(*) FROM dealers;").fetchone()
        count = result[0]
        print(f"✓ dealers table exists ({count} records)")
        
        cols = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'dealers'
            ORDER BY ordinal_position;
        """).fetchall()
        for col_name, col_type in cols:
            print(f"  - {col_name}: {col_type}")
    except Exception as e:
        print(f"❌ dealers table error: {e}")
    
    # Test geocode_cache table
    print("\n--- Geocode Cache Table ---")
    try:
        result = conn.execute("SELECT COUNT(*) FROM geocode_cache;").fetchone()
        count = result[0]
        print(f"✓ geocode_cache table exists ({count} cached entries)")
        
        cols = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'geocode_cache'
            ORDER BY ordinal_position;
        """).fetchall()
        for col_name, col_type in cols:
            print(f"  - {col_name}: {col_type}")
    except Exception as e:
        print(f"❌ geocode_cache table error: {e}")
    
    conn.close()
    print("\n✓ All tests passed!")
    
except Exception as e:
    print(f"❌ Connection failed: {type(e).__name__}: {e}")
    sys.exit(1)
