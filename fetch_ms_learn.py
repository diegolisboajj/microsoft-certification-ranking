#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Microsoft Learn Certification Fetcher
Fetches certifications from Microsoft Learn public API.

This supplements Credly data for users who do not have a Credly account
or as an enrichment source for existing Credly users.

How it works:
  1. Calls /api/profiles/{username} to get the internal userId (GUID)
  2. Calls /api/achievements/user/{userId} to get all user achievements
  3. Filters learning paths whose title starts with an official certification
     code (AZ-XXX, SC-XXX, AI-XXX, etc.) and deduplicates by code.

NOTE: Official exam-pass certifications (linkedMcId) require authentication
and are NOT accessible publicly. This approach counts Learning Paths that
map to exam preparation paths as a public-accessible proxy.
"""

import re
import requests
from datetime import datetime

# Pattern matching official Microsoft certification codes in Learning Path titles
# e.g.: "AZ-305: Design infrastructure solutions", "SC-100: ...", "AI-900: ..."
CERT_CODE_PATTERN = re.compile(
    r'^(AZ|SC|AI|DP|PL|MS|MB|MD|MO|DA|IO|PD|SI|NU|WS)-(\d{3})\b',
    re.IGNORECASE
)

MS_LEARN_BASE = 'https://learn.microsoft.com'

_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'application/json',
}


def fetch_learn_user_id(ms_learn_username: str) -> str | None:
    """
    Resolve a Microsoft Learn username to its internal userId (GUID).
    Returns None if the profile is not found or is private.
    """
    url = f'{MS_LEARN_BASE}/api/profiles/{ms_learn_username}'
    try:
        r = requests.get(url, headers=_HEADERS, timeout=10)
        if r.status_code != 200:
            return None
        data = r.json()
        if data.get('isPrivate', True):
            return None
        return data.get('userId')
    except Exception:
        return None


def fetch_learn_achievements(user_id: str) -> list[dict]:
    """
    Fetch all public achievements for a user by their GUID userId.
    Returns the raw list of achievement objects.
    """
    url = f'{MS_LEARN_BASE}/api/achievements/user/{user_id}?locale=en-us'
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200:
            return []
        return r.json().get('achievements', [])
    except Exception:
        return []


def extract_cert_code(title: str) -> str | None:
    """
    Extract the base certification code from a Learning Path title.
    e.g. "AZ-305: Design infrastructure solutions" -> "AZ-305"
         "AZ-305 Microsoft Azure..." -> "AZ-305"
    Returns None if the title does not start with a certification code.
    """
    m = CERT_CODE_PATTERN.match(title.strip())
    if m:
        return f'{m.group(1).upper()}-{m.group(2)}'
    return None


def fetch_learn_cert_names(ms_learn_username: str) -> set[str]:
    """
    Main entry point. Given a Microsoft Learn username, returns a set of
    unique certification badge names sourced from Learning Path achievements.

    Each unique certification code (e.g. AZ-305, SC-100) is represented
    by the title of its FIRST matching learning path (alphabetically earliest
    grant date), so we deduplicate multiple AZ-305 paths into one entry.

    Returns an empty set if the user is not found, profile is private,
    or no certification learning paths are found.
    """
    user_id = fetch_learn_user_id(ms_learn_username)
    if not user_id:
        return set()

    achievements = fetch_learn_achievements(user_id)
    if not achievements:
        return set()

    # Collect learning paths that match a cert code, deduplicating by code.
    # For each cert code, keep the entry with the earliest grantedOn date
    # (i.e., the first time they engaged with that cert track).
    cert_by_code: dict[str, dict] = {}
    for achievement in achievements:
        if achievement.get('category') != 'learningpaths':
            continue
        title = achievement.get('title', '')
        code = extract_cert_code(title)
        if not code:
            continue

        granted_on = achievement.get('grantedOn', '')
        if code not in cert_by_code:
            cert_by_code[code] = achievement
        else:
            # Keep earliest entry
            existing_grant = cert_by_code[code].get('grantedOn', '')
            if granted_on and granted_on < existing_grant:
                cert_by_code[code] = achievement

    # Return the certification code itself as the badge name
    # (e.g. "AZ-305", "SC-100") so it's consistent across sources
    return set(cert_by_code.keys())


def fetch_learn_profile_url(ms_learn_username: str) -> str:
    """Returns the public Microsoft Learn profile URL for a username."""
    return f'https://learn.microsoft.com/en-us/users/{ms_learn_username}/'


# ──────────────────────────────────────────────────────────────────────────────
# CLI: test a username directly
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import sys

    username = sys.argv[1] if len(sys.argv) > 1 else 'dlisboagiglioli-4456'
    print(f'Testing Microsoft Learn fetch for: {username}')
    print('=' * 60)

    user_id = fetch_learn_user_id(username)
    if not user_id:
        print(f'❌ Profile not found or private: {username}')
        sys.exit(1)

    print(f'✅ userId: {user_id}')

    achievements = fetch_learn_achievements(user_id)
    total_lps = sum(1 for a in achievements if a.get('category') == 'learningpaths')
    total_mods = sum(1 for a in achievements if a.get('category') == 'modules')
    print(f'   Total achievements: {len(achievements)} ({total_lps} learning paths, {total_mods} modules)')

    cert_names = fetch_learn_cert_names(username)
    print(f'\n📋 Detected certification codes ({len(cert_names)}):')
    for name in sorted(cert_names):
        print(f'   - {name}')

    print(f'\n🔗 Profile URL: {fetch_learn_profile_url(username)}')
