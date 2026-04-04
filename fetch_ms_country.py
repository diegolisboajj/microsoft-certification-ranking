#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch Microsoft Certifications for a single country
Using GitHub Org ID as users pool (Microsoft's is closed) 
and filtering for Microsoft certs (all Microsoft Certified & Applied Skills)
"""

import csv
import json
import os
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Microsoft issuers on Credly
MICROSOFT_ISSUER_IDS = {
    '1392f199-abe0-4698-92b5-834610af6baf', # Microsoft (official - only count this)
}

EXCLUDED_BADGES = {
    'Example Excluded Badge', 
}

def is_badge_expired(expires_at_date):
    if not expires_at_date: return False
    try:
        expiration_date = datetime.strptime(expires_at_date, "%Y-%m-%d").date()
        return expiration_date < datetime.now().date()
    except: return False

def fetch_user_badges(user_id):
    """Fetch all unique Microsoft badge names for a user, including the 'Other' tab"""
    unique_badge_names = set()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        # 1. Fetch Profile for synthetic_id (UUID)
        profile_url = f"https://www.credly.com/users/{user_id}"
        profile_res = requests.get(profile_url, headers={**headers, 'Accept': 'application/json'}, timeout=10)
        profile_data = profile_res.json().get('data', {})
        synthetic_id = profile_data.get('synthetic_id')

        # 2. Fetch Regular Badges
        page = 1
        while True:
            url = f"https://www.credly.com/users/{user_id}/badges.json?page={page}&per_page=100"
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            badges = data.get('data', [])
            if not badges: break
            
            for badge in badges:
                issuer = badge.get('issuer', {})
                entities = issuer.get('entities', [])

                is_ms = any(e.get('entity', {}).get('id') in MICROSOFT_ISSUER_IDS for e in entities)
                if not is_ms:
                    summary = issuer.get('summary', '')
                    issuer_msg = ''
                    if isinstance(summary, dict):
                        issuer_msg = summary.get('name', '')
                    else:
                        issuer_msg = str(summary)
                    if 'Microsoft' in issuer_msg or 'GitHub' in issuer_msg: is_ms = True
                
                if is_ms:
                    badge_template = badge.get('badge_template', {})
                    name = badge_template.get('name', '')
                    if name and name not in EXCLUDED_BADGES:
                        unique_badge_names.add(name)
            
            page += 1
            if page > 5: break # Cap pages for performance
            
        # 3. Fetch External Badges (Other tab)
        if synthetic_id:
            ext_url = f"https://www.credly.com/api/v1/users/{synthetic_id}/external_badges/open_badges/public?page=1&page_size=100"
            ext_res = requests.get(ext_url, headers=headers, timeout=10)
            if ext_res.status_code == 200:
                ext_data = ext_res.json().get('data', [])
                for item in ext_data:
                    ext_badge = item.get('external_badge', {})
                    issuer_name = ext_badge.get('issuer_name', '')
                    if 'Microsoft' in issuer_name or 'GitHub' in issuer_name:
                        badge_name = ext_badge.get('badge_name', '')
                        if badge_name and badge_name not in EXCLUDED_BADGES:
                            unique_badge_names.add(badge_name)

        return unique_badge_names
    except Exception as e:
        return set()

def fetch_country_data(country):
    """Fetch data for a country using GitHub Org directory as user pool"""
    # GitHub Org ID: 63074953-290b-4dce-86ce-ea04b4187219
    base_url = f"https://www.credly.com/api/v1/directory?organization_id=63074953-290b-4dce-86ce-ea04b4187219&sort=alphabetical&filter%5Blocation_name%5D={country.replace(' ', '%20')}&page="
    
    MAX_DIRECTORY_PAGES = 40 # Cap to ~320 users for performance
    all_users = []
    page = 1
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    print(f"Fetching Microsoft certification data (via GitHub user pool) for {country}...")
    
    while page <= MAX_DIRECTORY_PAGES:
        url = f"{base_url}{page}&format=json"
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            users = data.get('data', [])
            if not users: break
            all_users.extend(users)
            print(f"  Page {page}/{MAX_DIRECTORY_PAGES}: {len(users)} users")
            page += 1
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break
    
    # Inject manually known missing users
    known_missing_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'known_missing_users.json')
    if os.path.exists(known_missing_file):
        try:
            with open(known_missing_file, 'r') as f:
                known_users = json.load(f)
                missing_for_country = known_users.get(country, [])
                for user_obj in missing_for_country:
                    username = user_obj.get('id')
                    print(f"  Injecting known missing user: {username}")
                    # Force a very high badge count to ensure they get tested in the top 100
                    all_users.append({
                        'id': username, 
                        'badge_count': 9999,
                        'first_name': user_obj.get('first_name', ''),
                        'last_name': user_obj.get('last_name', ''),
                        'url': f"/users/{username}/badges"
                    })
        except: pass

    if all_users:
        # Sort by directory badge_count (includes expired) to get top candidates
        all_users_sorted = sorted(all_users, key=lambda x: x.get('badge_count', 0), reverse=True)
        # Check top 100 candidates (plenty for a Top 10)
        top_candidates = all_users_sorted[:min(100, len(all_users_sorted))]
        
        print(f"  Fetching detailed MS badges for top {len(top_candidates)} candidates...")
        user_badge_data = {} # Will store set of names
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_user = {
                executor.submit(fetch_user_badges, user.get('id')): user.get('id')
                for user in top_candidates if user.get('id')
            }
            for future in as_completed(future_to_user):
                user_id = future_to_user[future]
                try: 
                    badge_names_set = future.result()
                except: 
                    badge_names_set = set()
                user_badge_data[user_id] = badge_names_set
        
        # Update badge counts and names
        for user in all_users:
            user_id = user.get('id')
            badge_names_set = user_badge_data.get(user_id, set())
            user['badge_count'] = len(badge_names_set)
            user['badge_names'] = "|".join(sorted(list(badge_names_set)))
    
    return all_users

def save_to_csv(country, users, output_dir='datasource_ms'):
    os.makedirs(output_dir, exist_ok=True)
    file_suffix = country.lower().replace(' ', '-')
    output_file = f"{output_dir}/ms-certs-{file_suffix}.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'middle_name', 'last_name', 'badge_count', 'profile_url', 'badge_names'])
        for user in users:
            if user.get('badge_count', 0) > 0:
                writer.writerow([
                    user.get('first_name', ''),
                    user.get('middle_name', ''),
                    user.get('last_name', ''),
                    user.get('badge_count', 0),
                    user.get('url', ''),
                    user.get('badge_names', '')
                ])
    print(f"\nSaved to {output_file}")
    return output_file

def main():
    if len(sys.argv) < 2:
        print("Usage: ./fetch_ms_country.py <country_name>")
        sys.exit(1)
    country = sys.argv[1]
    print("=" * 80)
    print(f"Fetching Microsoft certifications for: {country}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    users = fetch_country_data(country)
    save_to_csv(country, users)
    print()
    print("=" * 80)
    print(f"✅ Success! Processed {len(users)} users")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    main()
