# pip3 install anticaptchaofficial boto3 requests

from anticaptchaofficial.recaptchav2proxyless import *
import boto3
import requests
import urllib.request
import json
import re
from bs4 import BeautifulSoup
import subprocess
import sys

# Check if ID is provided as command line argument
if len(sys.argv) > 1:
    target_id = sys.argv[1]
    output_file = f"{target_id}.html"
else:
    # Default ID if none provided
    target_id = "91588"
    output_file = "page_content.html"

# Function to get session ID from JSON file
def get_session_from_json():
    try:
        with open('session.json', 'r') as f:
            session_data = json.load(f)
            return session_data.get('session_id')
    except Exception as e:
        print(f"Error reading session ID from JSON: {e}")
        return None

# Get initial session ID
try:
    SID = get_session_from_json()
    if not SID:
        raise Exception("No session ID found in JSON file")
except Exception as e:
    print(f"Error getting session ID: {e}")
    SID = "msqg0rrsbn4ao13u3lsputip"  # Fallback to hardcoded value

def get_anticaptcha_key():
    session = boto3.session.Session()
    client = session.client('secretsmanager')
    response = client.get_secret_value(SecretId='anticaptcha/api-key')
    return response['SecretString']

def load_cookies():
    with open('cookies.json', 'r') as f:
        cookie_data = json.load(f)
        return {cookie['name']: cookie['value'] for cookie in cookie_data}

def extract_form_values(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    return {
        'viewstate': soup.find('input', {'name': '__VIEWSTATE'})['value'],
        'viewstategenerator': soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value'],
        'eventvalidation': soup.find('input', {'name': '__EVENTVALIDATION'})['value'],
        'recaptcha_site_key': soup.find('div', {'class': 'recaptcha'})['data-sitekey']
    }

# Set up proxy
username = 'guhan_U1Lt4'
password = 'Ishita_23456'
proxy_auth = f'http://customer-{username}:{password}@pr.oxylabs.io:7777'
proxies = {
    'http': proxy_auth,
    'https': proxy_auth
}

# Test proxy connection
print("Testing proxy connection...")
proxy_handler = urllib.request.ProxyHandler({
    'http': proxy_auth,
    'https': proxy_auth
})
opener = urllib.request.build_opener(proxy_handler)
try:
    response = opener.open('https://ip.oxylabs.io/location').read()
    print(f"Proxy connection successful: {response.decode('utf-8')}")
except Exception as e:
    print(f"Proxy connection failed: {e}")

def process_page(session_id):
    # First request to get the form values
    print(f"Using session ID: {session_id}")
    initial_url = f'https://www.publicnoticecolorado.com/(S({session_id}))/Details.aspx?SID={session_id}&ID={target_id}'

    initial_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'dnt': '1',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 CrKey/1.54.248666'
    }

    cookies = load_cookies()
    initial_response = requests.get(initial_url, headers=initial_headers, cookies=cookies, proxies=proxies)
    print(f"Initial page fetch status: {initial_response.status_code}")
    
    # Extract form values
    form_values = extract_form_values(initial_response.text)
    print("\nExtracted form values:")
    print(f"Recaptcha site key: {form_values['recaptcha_site_key']}")

    # Solve captcha using AntiCaptcha
    solver = recaptchaV2Proxyless()
    solver.set_verbose(1)
    solver.set_key(get_anticaptcha_key())
    solver.set_website_url(initial_url)
    solver.set_website_key(form_values['recaptcha_site_key'])
    solver.set_soft_id(0)

    g_response = solver.solve_and_return_solution()

    if g_response and g_response != 0:
        print("g-response:", g_response)
        
        # Make final form submission with captcha
        print("Submitting form with solved captcha...")
        final_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'content-type': 'application/x-www-form-urlencoded',
            'dnt': '1',
            'origin': 'https://www.publicnoticecolorado.com',
            'priority': 'u=0, i',
            'referer': initial_url,
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Linux; Android) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 CrKey/1.54.248666'
        }

        form_data = {
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$PublicNoticeDetailsBody1$btnViewNotice',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': form_values['viewstate'],
            '__VIEWSTATEGENERATOR': form_values['viewstategenerator'],
            '__EVENTVALIDATION': form_values['eventvalidation'],
            'g-recaptcha-response': g_response
        }

        final_response = requests.post(
            initial_url,
            headers=final_headers,
            cookies=cookies,
            data=form_data,
            proxies=proxies
        )

        print("Final response status code:", final_response.status_code)
            
        print("\nPage HTML content:")
        print(final_response.text)

        # Save the HTML content to a file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(final_response.text)
        print(f"\nHTML content saved to {output_file}")
        
        return final_response

    else:
        print("Captcha solving failed:", solver.error_code)
        return None

# Main execution
response = process_page(SID)
