#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Microsoft Certifications data for all countries
Runs fetch_ms_country.py (or fetch_large_ms_country.py) in parallel
"""

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from generate_ms_rankings import CONTINENT_MAP

METADATA_FILE = 'ms_csv_metadata.json'
DATASOURCE_DIR = 'datasource_ms'

# Use venv python if available, otherwise fall back to system python3
PYTHON_BIN = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'venv', 'bin', 'python3')
if not os.path.exists(PYTHON_BIN):
    PYTHON_BIN = sys.executable

def get_all_countries():
    countries = set()
    for country in CONTINENT_MAP.keys():
        countries.add(country.title())
    return sorted(countries)

def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_metadata(metadata):
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)

def fetch_country_data(country, metadata):
    file_suffix = country.lower().replace(' ', '-')
    csv_file = f'{DATASOURCE_DIR}/ms-certs-{file_suffix}.csv'
    
    # Large countries list (can be adjusted)
    large_countries = ['India', 'United States', 'Brazil', 'United Kingdom', 'Germany', 'Canada', 'Australia']
    
    script = 'fetch_large_ms_country.py' if country in large_countries else 'fetch_ms_country.py'
    timeout = 1800 if country in large_countries else 600
    
    try:
        result = subprocess.run(
            [PYTHON_BIN, script, country],
            timeout=timeout,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            metadata[country] = {
                'csv_file': csv_file,
                'last_updated': datetime.now().isoformat(),
                'status': 'success'
            }
            return (country, 'success', None)
        else:
            return (country, 'failed', f"Exit code: {result.returncode}")
    except Exception as e:
        return (country, 'failed', str(e))

def main():
    os.makedirs(DATASOURCE_DIR, exist_ok=True)
    metadata = load_metadata()
    countries = get_all_countries()
    
    # Limit for testing/demonstration if needed
    # countries = countries[:10] 
    
    print(f"📋 Processing {len(countries)} countries for Microsoft certs...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_country_data, c, metadata): c for c in countries}
        for future in as_completed(futures):
            c, status, err = future.result()
            if status == 'success':
                print(f"✓ Success: {c}")
            else:
                print(f"✗ Failed: {c} ({err})")
    
    save_metadata(metadata)
    print("\n✅ All fetching completed. Proceeding to generation...")
    subprocess.run([PYTHON_BIN, 'generate_ms_rankings.py'])

if __name__ == "__main__":
    main()
