#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate community-only ranking for Microsoft Certifications in Brazil
Using GitHub directory as a user pool
Focuses on "Microsoft Certified" and "Microsoft Applied Skills"
"""

import csv
import json
import os
import requests
from collections import defaultdict
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

def fetch_user_badges_and_company(user_id, profile_url):
    unique_badge_names = set()
    company = ''
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        # Fetch badges
        page = 1
        while True:
            url = f"https://www.credly.com/users/{user_id}/badges.json?page={page}&per_page=100"
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            data = res.json()
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
                    if 'Microsoft' in issuer_msg: is_ms = True
                
                if is_ms:
                    name = badge.get('badge_template', {}).get('name', '')
                    if name: unique_badge_names.add(name)
            
            page += 1
            if page > 10: break
            
        # Fetch company
        username = profile_url.split('/')[2] if '/users/' in profile_url else ''
        if username:
            url = f"https://www.credly.com/users/{username}"
            headers['Accept'] = 'application/json'
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                company = r.json().get('data', {}).get('current_organization_name', '').replace('|', '/')
                
        return len(unique_badge_names), company
    except:
        return 0, ''

def fetch_brazil_users_pool():
    """Read users from the primary Brazil CSV datasource to ensure consistency"""
    csv_file = 'datasource_ms/ms-certs-brazil.csv'
    if not os.path.exists(csv_file):
        print(f"⚠️  {csv_file} not found! Falling back to directory fetch...")
        return []
        
    print(f"📂 Loading users from {csv_file} for community ranking...")
    users = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                users.append({
                    'id': row.get('profile_url', '').split('/')[-2] if '/users/' in row.get('profile_url', '') else '',
                    'first_name': row.get('first_name', ''),
                    'middle_name': row.get('middle_name', ''),
                    'last_name': row.get('last_name', ''),
                    'url': row.get('profile_url', '')
                })
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
    return users

def main():
    csv_file = 'datasource_ms/ms-certs-brazil.csv'
    if not os.path.exists(csv_file):
        print(f"❌ {csv_file} not found! Run the fetch script first.")
        return

    print(f"📂 Loading Brazil users from {csv_file}...")
    final_users = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                badge_count = int(row.get('badge_count', 0))
                if badge_count > 0:
                    name = ' '.join(filter(None, [row.get('first_name'), row.get('middle_name'), row.get('last_name')]))
                    final_users.append({
                        'name': name,
                        'badges': badge_count,
                        'profile_url': row.get('profile_url'),
                        'company': '' # To be fetched
                    })
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    # Name-based grouping in case there are duplicates in CSV (e.g. from manual injections)
    grouped_users = {}
    for u in final_users:
        name = u['name']
        if name not in grouped_users:
            grouped_users[name] = u
        else:
            grouped_users[name]['badges'] += u['badges']
            if not grouped_users[name]['profile_url'] and u['profile_url']:
                grouped_users[name]['profile_url'] = u['profile_url']

    sorted_users = sorted(grouped_users.values(), key=lambda x: (-x['badges'], x['name'].lower()))
    
    # We only need company info for those appearing in or near the Top 10
    top_candidates = sorted_users[:20] 
    print(f"📂 Fetching company info for top {len(top_candidates)} users...")
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_user_company, u['profile_url']): u for u in top_candidates}
        for future in as_completed(futures):
            u_data = futures[future]
            u_data['company'] = future.result()

    # Generate MS_TOP10_BRAZIL_COMMUNITY.md
    content = f"# 🇧🇷 TOP 10 Microsoft Certifications - Brazil (Community)\n\n> Last updated: {datetime.now().strftime('%B %d, %Y at %H:%M UTC')}\n\n| Rank | Name | Badges | Company |\n|------|------|--------|---------|\n"
    
    pos = 0
    prev_badges = None
    for u in sorted_users:
        if u['badges'] != prev_badges:
            pos += 1
            prev_badges = u['badges']
            if pos > 10: break
        
        medal = {1: '🥇', 2: '🥈', 3: '🥉'}.get(pos, '')
        r_disp = f"{medal} #{pos}" if medal else f"#{pos}"
        p_url = f"https://www.credly.com{u['profile_url']}" if u['profile_url'] else '#'
        content += f"| {r_disp} | [{u['name']}]({p_url}) | {u['badges']} | {u['company']} |\n"

    with open('MS_TOP10_BRAZIL_COMMUNITY.md', 'w', encoding='utf-8') as f:
        f.write(content)
    print("✅ Generated: MS_TOP10_BRAZIL_COMMUNITY.md")

def fetch_user_company(profile_url):
    """Fetch company info from Credly profile JSON"""
    if not profile_url: return ''
    username = profile_url.split('/')[-2] if '/users/' in profile_url else ''
    if not username: return ''
    
    try:
        url = f"https://www.credly.com/users/{username}"
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        }
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            return r.json().get('data', {}).get('current_organization_name', '').replace('|', '/')
    except:
        pass
    return ''

if __name__ == "__main__":
    main()
