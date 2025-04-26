import requests
import re
import json

# Proxy Configuration
username = 'guhan_U1Lt4'
password = 'Ishita_23456'
proxy_url = f"http://customer-{username}:{password}@pr.oxylabs.io:7777"
proxies = {
    'http': proxy_url,
    'https': proxy_url
}

# Load existing cookies
with open('cookies.json', 'r') as f:
    cookie_data = json.load(f)
    cookies = {cookie['name']: cookie['value'] for cookie in cookie_data}

# Initial request to get session
search_url = "https://www.publicnoticecolorado.com/Search.aspx"
response = requests.get(
    search_url,
    proxies=proxies,
    headers={'User-Agent': 'Mozilla/5.0'},
    cookies=cookies,
    allow_redirects=True
)

# Extract session ID
session_match = re.search(r'\(S\((.*?)\)\)', response.url)
if session_match:
    session_id = session_match.group(1)
    print(f"New session ID: {session_id}")
else:
    print("Failed to get session ID") 