import requests
import time
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Proxy credentials and entry
username = 'customer-guhan_U1Lt4'
password = 'Ishita_23456'
proxy_host = 'pr.oxylabs.io:7777'

# Configure proxy
proxies = {
    'http': f'http://{username}:{password}@{proxy_host}',
    'https': f'http://{username}:{password}@{proxy_host}'
}

# Test with requests first
print("Testing proxy with requests...")
try:
    response = requests.get('https://www.publicnoticecolorado.com', proxies=proxies, verify=False)
    print(f"Status Code: {response.status_code}")
    print("Cookies received:")
    for cookie in response.cookies:
        print(f"{cookie.name} = {cookie.value}")
    print("\nResponse Headers:")
    for header, value in response.headers.items():
        print(f"{header}: {value}")
except Exception as e:
    print(f"Error with requests: {e}")

time.sleep(5)  # Wait to see the output
