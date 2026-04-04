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
    """Using GitHub directory (public) as a starting pool for Brazil"""
    print("📂 Fetching Brazil users pool (via GitHub directory)...")
    # GitHub Org ID: 63074953-290b-4dce-86ce-ea04b4187219
    all_users = []
    page = 1
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    while True:
        url = f"https://www.credly.com/api/v1/directory?organization_id=63074953-290b-4dce-86ce-ea04b4187219&sort=alphabetical&filter%5Blocation_name%5D=Brazil&page={page}&format=json"
        res = requests.get(url, headers=headers, timeout=30)
        data = res.json()
        users = data.get('data', [])
        if not users: break
        all_users.extend(users)
        print(f"  Page {page}: {len(users)} users")
        page += 1
        if page > 20: break # Process top candidates for performance in community script
    return all_users

def main():
    directory_users = fetch_brazil_users_pool()
    if not directory_users:
        print("❌ No users found!")
        return

    # Inject manually known missing users
    known_missing_file = 'known_missing_users.json'
    if os.path.exists(known_missing_file):
        try:
            with open(known_missing_file, 'r') as f:
                known_users = json.load(f)
                missing_for_country = known_users.get('Brazil', [])
                for user_obj in missing_for_country:
                    username = user_obj.get('id')
                    print(f"  Injecting known missing user: {username}")
                    directory_users.append({
                        'id': username, 
                        'first_name': user_obj.get('first_name', ''),
                        'last_name': user_obj.get('last_name', ''),
                        'url': f"/users/{username}/badges"
                    })
        except: pass

    print(f"\n📂 Fetching MS badges for {len(directory_users)} users...")
    grouped_results = {}
    
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_user_badges_and_company, u['id'], u.get('url', '')): u for u in directory_users}
        for i, future in enumerate(as_completed(futures), 1):
            u_data = futures[future]
            try:
                cnt, comp = future.result()
            except:
                cnt, comp = 0, ''
                
            if cnt > 0:
                name = ' '.join(filter(None, [u_data.get('first_name'), u_data.get('middle_name'), u_data.get('last_name')]))
                if name not in grouped_results:
                    grouped_results[name] = {
                        'name': name, 
                        'badges': cnt, 
                        'company': comp, 
                        'profile_url': u_data.get('url')
                    }
                else:
                    grouped_results[name]['badges'] += cnt
                    if not grouped_results[name]['company'] and comp:
                        grouped_results[name]['company'] = comp
            
            if i % 10 == 0: print(f"  Progress: {i}/{len(directory_users)}")

    final_users = list(grouped_results.values())

    sorted_users = sorted(final_users, key=lambda x: (-x['badges'], x['name'].lower()))
    
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

if __name__ == "__main__":
    main()
