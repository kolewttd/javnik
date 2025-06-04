import pandas as pd
import ipaddress
from collections import Counter
import re
import gzip
import os
import pycountry

def get_country_name(code):
    try:
        return pycountry.countries.get(alpha_2=code).name
    except:
        return 'Unknown'


# --- Config ---
LOG_FILE = 'access.log'  # Can also be 'access.log'
DBIP_FILE = 'dbip-country-lite-2025-06.csv'  # Update filename if needed

# --- Helper Functions ---

def is_ipv4(addr):
    try:
        ipaddress.IPv4Address(addr)
        return True
    except ipaddress.AddressValueError:
        return False

def get_country(ip_int, dbip_df):
    """Binary search for IP range match"""
    row = dbip_df[
        (dbip_df['ip_start_int'] <= ip_int) & (dbip_df['ip_end_int'] >= ip_int)
    ]
    if not row.empty:
        return row.iloc[0]['country_code']
    return 'Unknown'

def extract_ip(line):
    match = re.match(r'^(\d{1,3}(?:\.\d{1,3}){3})', line)
    return match.group(1) if match else None

def open_log_file(filename):
    if filename.endswith('.gz'):
        return gzip.open(filename, 'rt')
    return open(filename, 'r')

# --- Load DB-IP CSV and prepare IP ranges ---

print(f"Loading DB-IP data from: {DBIP_FILE}")
dbip_df = pd.read_csv(DBIP_FILE, header=None, names=['ip_start', 'ip_end', 'country_code'])
dbip_df = dbip_df[dbip_df['ip_start'].apply(is_ipv4) & dbip_df['ip_end'].apply(is_ipv4)]
dbip_df['ip_start_int'] = dbip_df['ip_start'].apply(lambda x: int(ipaddress.IPv4Address(x)))
dbip_df['ip_end_int'] = dbip_df['ip_end'].apply(lambda x: int(ipaddress.IPv4Address(x)))

# --- Parse log file and count IPs ---

print(f"Parsing log file: {LOG_FILE}")
ip_counter = Counter()

with open_log_file(LOG_FILE) as f:
    for line in f:
        ip = extract_ip(line)
        if ip:
            ip_counter[ip] += 1

# --- Map IPs to countries and count per country ---

print("Mapping IPs to countries...")
country_counter = Counter()
ip_country_map = {}

for ip, count in ip_counter.items():
    try:
        ip_int = int(ipaddress.IPv4Address(ip))
        country = get_country(ip_int, dbip_df)
    except ValueError:
        country = 'Unknown'
    ip_country_map[ip] = (country, count)
    country_counter[country] += count

# --- Output ---

print("\nTop IPs with country:")
for ip, (country, count) in sorted(ip_country_map.items(), key=lambda x: x[1][1], reverse=True)[:1000]:
    print(f"{ip:15} {country:5} {count} times")

print("\nRequests per country:")
for code, count in country_counter.most_common():
    country_name = get_country_name(code)
    print(f"{code:3} ({country_name:25}: {count}) requests")


