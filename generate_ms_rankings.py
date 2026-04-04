#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Microsoft Certifications Rankings Generator
Generates TOP 10 rankings for Brazil, Americas, Europe, Asia, Oceania, and World
"""

import csv
import glob
import json
import os
import requests
from collections import defaultdict
from datetime import datetime

# Continent mapping (Copied from original rankings script)
CONTINENT_MAP = {
    # AMERICAS
    'antigua and barbuda': 'Americas', 'argentina': 'Americas', 'bahamas': 'Americas',
    'barbados': 'Americas', 'belize': 'Americas', 'bolivia': 'Americas',
    'brazil': 'Americas', 'canada': 'Americas', 'chile': 'Americas',
    'colombia': 'Americas', 'costa rica': 'Americas', 'cuba': 'Americas',
    'dominica': 'Americas', 'dominican republic': 'Americas', 'ecuador': 'Americas',
    'el salvador': 'Americas', 'grenada': 'Americas', 'guatemala': 'Americas',
    'haiti': 'Americas', 'honduras': 'Americas', 'jamaica': 'Americas',
    'mexico': 'Americas', 'nicaragua': 'Americas', 'panama': 'Americas',
    'paraguay': 'Americas', 'peru': 'Americas', 'saint kitts and nevis': 'Americas',
    'saint lucia': 'Americas', 'saint vincent and the grenadines': 'Americas',
    'suriname': 'Americas', 'trinidad and tobago': 'Americas', 'united states': 'Americas',
    'uruguay': 'Americas', 'venezuela': 'Americas', 'guyana': 'Americas',
    
    # EUROPE
    'albania': 'Europe', 'andorra': 'Europe', 'armenia': 'Europe',
    'austria': 'Europe', 'azerbaijan': 'Europe', 'belarus': 'Europe',
    'belgium': 'Europe', 'bosnia and herzegovina': 'Europe', 'bulgaria': 'Europe',
    'croatia': 'Europe', 'cyprus': 'Europe', 'czech republic': 'Europe',
    'denmark': 'Europe', 'estonia': 'Europe', 'finland': 'Europe',
    'france': 'Europe', 'georgia': 'Europe', 'germany': 'Europe',
    'greece': 'Europe', 'hungary': 'Europe', 'iceland': 'Europe',
    'ireland': 'Europe', 'italy': 'Europe', 'kosovo': 'Europe',
    'latvia': 'Europe', 'liechtenstein': 'Europe', 'lithuania': 'Europe',
    'luxembourg': 'Europe', 'malta': 'Europe', 'moldova': 'Europe',
    'monaco': 'Europe', 'montenegro': 'Europe', 'netherlands': 'Europe',
    'north macedonia': 'Europe', 'norway': 'Europe', 'poland': 'Europe',
    'portugal': 'Europe', 'romania': 'Europe', 'russia': 'Europe',
    'san marino': 'Europe', 'serbia': 'Europe', 'slovakia': 'Europe',
    'slovenia': 'Europe', 'spain': 'Europe', 'sweden': 'Europe',
    'switzerland': 'Europe', 'ukraine': 'Europe', 'united kingdom': 'Europe',
    'vatican city': 'Europe',
    
    # ASIA
    'afghanistan': 'Asia', 'bahrain': 'Asia', 'bangladesh': 'Asia',
    'bhutan': 'Asia', 'brunei': 'Asia', 'cambodia': 'Asia',
    'china': 'Asia', 'east timor': 'Asia', 'indonesia': 'Asia',
    'iran': 'Asia', 'iraq': 'Asia', 'israel': 'Asia',
    'japan': 'Asia', 'jordan': 'Asia', 'kazakhstan': 'Asia',
    'kuwait': 'Asia', 'kyrgyzstan': 'Asia', 'laos': 'Asia',
    'lebanon': 'Asia', 'malaysia': 'Asia', 'maldives': 'Asia',
    'mongolia': 'Asia', 'myanmar': 'Asia', 'nepal': 'Asia',
    'north korea': 'Asia', 'oman': 'Asia', 'pakistan': 'Asia',
    'palestine': 'Asia', 'philippines': 'Asia', 'qatar': 'Asia',
    'saudi arabia': 'Asia', 'singapore': 'Asia', 'south korea': 'Asia',
    'sri lanka': 'Asia', 'syria': 'Asia', 'taiwan': 'Asia',
    'tajikistan': 'Asia', 'thailand': 'Asia', 'turkey': 'Asia',
    'turkmenistan': 'Asia', 'united arab emirates': 'Asia', 'uzbekistan': 'Asia',
    'vietnam': 'Asia', 'yemen': 'Asia', 'timor leste': 'Asia', 'india': 'Asia',
    
    # AFRICA
    'algeria': 'Africa', 'angola': 'Africa', 'benin': 'Africa',
    'botswana': 'Africa', 'burkina faso': 'Africa', 'burundi': 'Africa',
    'cameroon': 'Africa', 'cape verde': 'Africa', 'central african republic': 'Africa',
    'chad': 'Africa', 'comoros': 'Africa', 'democratic republic of the congo': 'Africa',
    'djibouti': 'Africa', 'egypt': 'Africa', 'equatorial guinea': 'Africa',
    'eritrea': 'Africa', 'eswatini': 'Africa', 'ethiopia': 'Africa',
    'gabon': 'Africa', 'gambia': 'Africa', 'ghana': 'Africa',
    'guinea': 'Africa', 'guinea-bissau': 'Africa',
    'ivory coast': 'Africa', 'kenya': 'Africa', 'lesotho': 'Africa', 'liberia': 'Africa',
    'libya': 'Africa', 'madagascar': 'Africa', 'malawi': 'Africa',
    'mali': 'Africa', 'mauritania': 'Africa', 'mauritius': 'Africa',
    'morocco': 'Africa', 'mozambique': 'Africa', 'namibia': 'Africa',
    'niger': 'Africa', 'nigeria': 'Africa', 'republic of the congo': 'Africa',
    'rwanda': 'Africa', 'sao tome and principe': 'Africa', 'senegal': 'Africa',
    'seychelles': 'Africa', 'sierra leone': 'Africa', 'somalia': 'Africa',
    'south africa': 'Africa', 'south sudan': 'Africa', 'sudan': 'Africa',
    'tanzania': 'Africa', 'togo': 'Africa', 'tunisia': 'Africa',
    'uganda': 'Africa', 'zambia': 'Africa', 'zimbabwe': 'Africa',
    
    # OCEANIA
    'australia': 'Oceania', 'fiji': 'Oceania', 'kiribati': 'Oceania',
    'marshall islands': 'Oceania', 'micronesia': 'Oceania', 'nauru': 'Oceania',
    'new zealand': 'Oceania', 'palau': 'Oceania', 'papua new guinea': 'Oceania',
    'samoa': 'Oceania', 'solomon islands': 'Oceania', 'tonga': 'Oceania',
    'tuvalu': 'Oceania', 'vanuatu': 'Oceania',
}

def get_continent(country_name):
    country_lower = country_name.lower().replace('-', ' ')
    return CONTINENT_MAP.get(country_lower, 'Unknown')

def fetch_user_company(profile_url):
    """Fetch company name from user profile"""
    if not profile_url: return ''
    try:
        username = profile_url.split('/')[2] if '/users/' in profile_url else ''
        if not username: return ''
        url = f"https://www.credly.com/users/{username}"
        headers = {'Accept': 'application/json'}
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        company = data.get('data', {}).get('current_organization_name', '')
        if company: company = company.replace('|', '/')
        return company if company else ''
    except:
        return ''

def load_metadata():
    metadata_file = 'ms_csv_metadata.json'
    if os.path.exists(metadata_file):
        with open(metadata_file, 'r') as f:
            return json.load(f)
    return {}

import re

def normalize_badge_name(name):
    """Normalize badge name for strict deduplication"""
    if not name: return ""
    # 1. Handle all whitespace variants (non-breaking spaces, etc)
    name = re.sub(r'\s+', ' ', name).strip()
    # 2. Remove common prefixes like 'AZ-900:', 'Exam AZ-900:', etc.
    # Pattern looks for Alphanumeric-Alphanumeric: at the start
    # Also strip known prefixes like 'Microsoft Certified: ', 'Microsoft Applied Skills: '
    # so that 'Exam AZ-900: Azure Fundamentals' and 'Azure Fundamentals' merge.
    name = re.sub(r'^(Exam\s+)?([A-Z0-9]+\-[A-Z0-9]+\:\s*)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^(Microsoft\s+(Certified|Applied Skills)\:\s*)', '', name, flags=re.IGNORECASE)
    # 3. Strip exam codes in parentheses anywhere: (AZ-900)
    name = re.sub(r'\s*\([A-Z0-9]+\-[A-Z0-9]+\)\s*', '', name, flags=re.IGNORECASE)
    # 4. Aggressive cleanup: only alphanumeric and spaces
    name = re.sub(r'[^\w\s]', '', name)
    # 5. Standardize spaces and case
    name = re.sub(r'\s+', ' ', name).strip().lower()
    # 6. Remove 'legacy' or versioning suffixes
    name = name.replace('legacy', '').strip()
    return name

# NOTE: CERT_PATTERNS filtering removed - the CSV already only contains MS-validated badges
# (validated at fetch time by MICROSOFT_ISSUER_IDS). We count ALL badges in the CSV.
# Deduplication is done by normalizing badge names and using a set (raw_badges_map).

def read_all_csv_files(base_path):
    grouped_users = {}
    csv_files = glob.glob(os.path.join(base_path, 'datasource_ms', 'ms-certs-*.csv'))
    print(f"📂 Processing {len(csv_files)} MS CSV files...")
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        country = filename.replace('ms-certs-', '').replace('.csv', '')
        country_display = country.replace('-', ' ').title()
        # Fix Title Case for special countries
        if country_display == 'United States': country_display = 'USA'
        if country_display == 'United Kingdom': country_display = 'UK'
        
        continent = get_continent(country)
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    badge_count_val = int(row.get('badge_count', 0))
                    if badge_count_val > 0:
                        first_name = row.get('first_name', '').strip('"').strip()
                        middle_name = row.get('middle_name', '').strip('"').strip()
                        last_name = row.get('last_name', '').strip('"').strip()
                        profile_url = row.get('profile_url', '').strip('"').strip()
                        
                        full_name = ' '.join(filter(None, [first_name, middle_name, last_name]))
                        if not full_name: continue
                        
                        # Parse individual badge names
                        badge_names_str = row.get('badge_names', '')
                        # Normalizing each badge name for the set comparison
                        new_badges_raw = [b.strip() for b in badge_names_str.split('|') if b.strip()]
                        
                        if full_name not in grouped_users:
                            grouped_users[full_name] = {
                                'name': full_name,
                                'raw_badges_map': {}, # Map normalized_name -> original_name
                                'country': country_display,
                                'continent': continent,
                                'profile_url': profile_url
                            }
                        
                        target_user = grouped_users[full_name]
                        for b in new_badges_raw:
                            norm_b = normalize_badge_name(b)
                            if norm_b:
                                if norm_b not in target_user['raw_badges_map']:
                                    target_user['raw_badges_map'][norm_b] = b
                        
                        # Prefer non-empty profile URL
                        if not target_user['profile_url'] and profile_url:
                            target_user['profile_url'] = profile_url
        except Exception as e:
            print(f"⚠️  Error processing {csv_file}: {e}")
            
    # Finalize badge counts
    users_list = []
    for user in grouped_users.values():
        user['badges'] = len(user['raw_badges_map'])
        if 'Giglioli' in user['name'] or 'Alonso Portillo' in user['name']:
            print(f"DEBUG: {user['name']} has {user['badges']} unique certifications after normalization.")
        if user['badges'] > 0:
            users_list.append(user)
            
    print(f"✅ Loaded {len(users_list)} unique users with valid certifications")
    return users_list

def generate_markdown_top10(users, title, filename, filter_func=None):
    if filter_func:
        filtered_users = [u for u in users if filter_func(u)]
    else:
        filtered_users = users
    
    if not filtered_users:
        print(f"⚠️  No users for {title}, skipping...")
        return

    sorted_users = sorted(filtered_users, key=lambda x: (-x['badges'], x['name'].lower()))
    
    positions = []
    current_pos = 0
    prev_badges = None
    
    for user in sorted_users:
        if user['badges'] != prev_badges:
            current_pos += 1
            prev_badges = user['badges']
            if current_pos > 10: break
            positions.append((current_pos, [user]))
        else:
            if current_pos <= 10:
                positions[-1][1].append(user)
    
    MAX_USERS_PER_POSITION = 20
    all_ranked_users = []
    for pos, pos_users in positions:
        all_ranked_users.extend(pos_users[:MAX_USERS_PER_POSITION])
    
    print(f"  Fetching company info for {len(all_ranked_users)} ranked users in {title}...")
    for user in all_ranked_users:
        user['company'] = fetch_user_company(user.get('profile_url', ''))
    
    content = f"# {title}\n\n> Last updated: {datetime.now().strftime('%B %d, %Y at %H:%M UTC')}\n\n## 🏆 Top 10 Microsoft Certifications Leaders\n\n| Rank | Name | Badges | Company | Country |\n|------|------|--------|---------|---------|\n"
    
    for pos, pos_users in positions:
        medal = {1: '🥇', 2: '🥈', 3: '🥉'}.get(pos, '')
        rank_display = f"{medal} #{pos}" if medal else f"#{pos}"
        
        display_users = pos_users[:MAX_USERS_PER_POSITION]
        overflow = len(pos_users) - MAX_USERS_PER_POSITION
        
        names = []
        companies = []
        countries = []
        for user in display_users:
            if user.get('profile_url'):
                profile_url = f"https://www.credly.com{user['profile_url']}"
                names.append(f"[{user['name']}]({profile_url})")
            else:
                names.append(user['name'])
            companies.append(user.get('company', ''))
            countries.append(user['country'])
        
        if overflow > 0:
            names.append(f"*... and {overflow} more*")
            companies.append('')
            countries.append('')
        
        content += f"| {rank_display} | {'<br>'.join(names)} | {pos_users[0]['badges']} | {'<br>'.join(companies)} | {'<br>'.join(countries)} |\n"
    
    # Company Stats
    company_stats = defaultdict(lambda: {'badges': 0, 'users': 0})
    for user in all_ranked_users:
        company = user.get('company', '')
        if company:
            company_stats[company]['badges'] += user['badges']
            company_stats[company]['users'] += 1
    
    if company_stats:
        sorted_companies = sorted(company_stats.items(), key=lambda x: (-x[1]['badges'], -x[1]['users'], x[0].lower()))
        content += "\n---\n\n## 🏢 Top 5 Companies\n\n| Rank | Company | Total Badges | Certified Users |\n|------|---------|--------------|-----------------|\n"
        
        prev_b = None
        c_pos = 0
        for name, stats in sorted_companies:
            if stats['badges'] != prev_b:
                c_pos += 1
                prev_b = stats['badges']
            if c_pos > 5: break
            
            med = {1: '🥇', 2: '🥈', 3: '🥉'}.get(c_pos, '')
            r_disp = f"{med} #{c_pos}" if med else f"#{c_pos}"
            content += f"| {r_disp} | {name} | {stats['badges']} | {stats['users']} |\n"

    # Statistics
    total_users = len(filtered_users)
    total_badges = sum(u['badges'] for u in filtered_users)
    avg_badges = total_badges / total_users if total_users > 0 else 0
    
    content += f"\n---\n\n## 📊 Statistics\n\n- **Total Certified Users**: {total_users:,}\n- **Total Badges Earned**: {total_badges:,}\n- **Average Badges per User**: {avg_badges:.2f}\n- **Highest Badge Count**: {all_ranked_users[0]['badges'] if all_ranked_users else 0}\n\n---\n\n*Data sourced from Microsoft Certifications via Credly API*\n"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"✅ Generated: {filename}")

def main():
    base_path = os.path.dirname(os.path.abspath(__file__))
    users = read_all_csv_files(base_path)
    if not users:
        print("❌ No MS users found!")
        return
    
    generate_markdown_top10(users, "🇧🇷 TOP 10 Microsoft Certifications - Brazil", "MS_TOP10_BRAZIL.md", lambda u: u['country'].lower() == 'brazil')
    generate_markdown_top10(users, "🗽 TOP 10 Microsoft Certifications - Americas", "MS_TOP10_AMERICAS.md", lambda u: u['continent'] == 'Americas')
    generate_markdown_top10(users, "🇪🇺 TOP 10 Microsoft Certifications - Europe", "MS_TOP10_EUROPE.md", lambda u: u['continent'] == 'Europe')
    generate_markdown_top10(users, "🌏 TOP 10 Microsoft Certifications - Asia", "MS_TOP10_ASIA.md", lambda u: u['continent'] == 'Asia')
    generate_markdown_top10(users, "🦁 TOP 10 Microsoft Certifications - Africa", "MS_TOP10_AFRICA.md", lambda u: u['continent'] == 'Africa')
    generate_markdown_top10(users, "🌊 TOP 10 Microsoft Certifications - Oceania", "MS_TOP10_OCEANIA.md", lambda u: u['continent'] == 'Oceania')
    generate_markdown_top10(users, "🌍 TOP 10 Microsoft Certifications - Global", "MS_TOP10_WORLD.md")

if __name__ == "__main__":
    main()
