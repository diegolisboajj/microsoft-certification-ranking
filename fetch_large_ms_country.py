#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Specialized script for fetching large countries with parallel page requests
Using GitHub Org ID as users pool (Microsoft's is closed)
"""

import csv
import json
import os
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

MICROSOFT_ISSUER_IDS = {
    '1392f199-abe0-4698-92b5-834610af6baf', # Microsoft
    '244bddb0-ca01-406b-a07d-084fb1c3cc68', # Microsoft Power Up Program
    'e3cf6b4e-7d68-45e5-a9ae-b95d20f7cefd', # Certiport (Pearson VUE)
}

def is_badge_expired(expires_at_date):
    if not expires_at_date: return False
    try:
        expiration_date = datetime.strptime(expires_at_date, "%Y-%m-%d").date()
        return expiration_date < datetime.now().date()
    except: return False

def fetch_user_badges(user_id):
    unique_badge_names = set()
    page = 1
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
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
                
                is_ms = False
                if any(e.get('entity', {}).get('id') == '1392f199-abe0-4698-92b5-834610af6baf' for e in entities):
                    is_ms = True
                    
                if not is_ms and entities and len(entities) > 0:
                    issuer_name = entities[0].get('entity', {}).get('name', '')
                    if issuer_name == 'Microsoft':
                        is_ms = True

                if is_ms:
                    expires_at_date = badge.get('expires_at_date')
                    if not is_badge_expired(expires_at_date):
                        badge_template = badge.get('badge_template', {})
                        name = badge_template.get('name', '')
                        if name: unique_badge_names.add(name)
            
            page += 1
            if page > 10: break
            
        return len(unique_badge_names)
    except:
        return 0

def fetch_page(country, page):
    """Fetch a single page for a country (GitHub directory)"""
    url = f"https://www.credly.com/api/v1/directory?organization_id=63074953-290b-4dce-86ce-ea04b4187219&sort=alphabetical&filter%5Blocation_name%5D={country.replace(' ', '%20')}&page={page}&format=json"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        metadata = data.get('metadata', {})
        total_pages = metadata.get('total_pages', 0)
        users = data.get('data', [])
        return (page, users, total_pages)
    except Exception as e:
        print(f"  Error on page {page}: {e}")
        return (page, [], 0)

def fetch_country_parallel(country, max_workers=20):
    print(f"Fetching Microsoft data for {country} (large country parallel)...")
    _, _, total_pages = fetch_page(country, 1)
    
    # Cap directory pages for performance
    MAX_DIRECTORY_PAGES = 100
    total_pages = min(total_pages, MAX_DIRECTORY_PAGES)
    
    if total_pages == 0:
        print(f"No data found for {country}")
        return []
    
    print(f"Total pages (capped): {total_pages}")
    all_users = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_page, country, page): page for page in range(1, total_pages + 1)}
        completed = 0
        for future in as_completed(futures):
            page, users, _ = future.result()
            all_users.extend(users)
            completed += 1
            if completed % 50 == 0:
                print(f"  Progress: {completed}/{total_pages} pages ({len(all_users)} users)")
    
    print(f"✓ Completed: {len(all_users)} users from {total_pages} pages")
    
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
        all_users_sorted = sorted(all_users, key=lambda x: x.get('badge_count', 0), reverse=True)
        top_candidates = all_users_sorted[:min(100, len(all_users_sorted))]
        
        print(f"  Fetching detailed MS badges for top {len(top_candidates)} candidates...")
        user_badge_counts = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_user = {executor.submit(fetch_user_badges, user.get('id')): user.get('id') for user in top_candidates if user.get('id')}
            for future in as_completed(future_to_user):
                user_id = future_to_user[future]
                try: badge_count = future.result()
                except: badge_count = 0
                user_badge_counts[user_id] = badge_count
        
        for user in all_users:
            user_id = user.get('id')
            user['badge_count'] = user_badge_counts.get(user_id, 0)
    
    return all_users

def save_to_csv(country, users, output_dir='datasource_ms'):
    os.makedirs(output_dir, exist_ok=True)
    file_suffix = country.lower().replace(' ', '-')
    output_file = f"{output_dir}/ms-certs-{file_suffix}.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'middle_name', 'last_name', 'badge_count', 'profile_url'])
        for user in users:
            if user['badge_count'] > 0:
                writer.writerow([
                    user.get('first_name', ''),
                    user.get('middle_name', ''),
                    user.get('last_name', ''),
                    user['badge_count'],
                    user.get('url', '')
                ])
    print(f"Saved to {output_file}")
    return output_file

def main():
    if len(sys.argv) < 2:
        print("Usage: ./fetch_large_ms_country.py <country_name>")
        sys.exit(1)
    country = sys.argv[1]
    print("=" * 80)
    print(f"Fetching Microsoft certifications (large): {country}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    users = fetch_country_parallel(country, max_workers=25)
    if users:
        save_to_csv(country, users)
        print()
        print("=" * 80)
        print(f"✅ Success! Processed {len(users)} users")
        print("=" * 80)
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
