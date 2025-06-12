import pandas as pd
import numpy as np
import re
import random
import ipaddress

# === CONFIG ===
INPUT_CSV = "/home/mpaul/projects/tw/tw-dns/ddos_data/abnormal.csv"
LIST_COLUMNS = [
    'pl_fwd_count', 'pl_bwd_count',
    'pl_len_fwd_total', 'pl_len_bwd_total',
    'flow_fwd_count', 'flow_bwd_count'
]

# === HELPERS ===
def parse_list_column(s):
    s = re.sub(r'\s+', ' ', s.strip())
    s = s.replace('[', '').replace(']', '')
    return np.fromstring(s, sep=' ')

def random_local_ip():
    return f"192.168.10.{random.randint(150, 153)}"

def random_public_ip():
    while True:
        ip = ipaddress.IPv4Address(random.randint(0, 2**32 - 1))
        if not ip.is_private and not ip.is_multicast and not ip.is_reserved and not ip.is_loopback:
            return str(ip)

def generate_unique_public_ips(count):
    ips = set()
    while len(ips) < count:
        ips.add(random_public_ip())
    return list(ips)

def random_ephemeral_port():
    return random.randint(1024, 65535)

# === LOAD & CLEAN ===
df = pd.read_csv(INPUT_CSV)
for col in LIST_COLUMNS:
    df[col] = df[col].apply(parse_list_column)

# === FLATTENING ===
flattened_rows = []
for _, row in df.iterrows():
    length = len(row['pl_fwd_count'])
    for i in range(length):
        new_row = {
            'window_id': row['window_id'],
            'timestamp': row['timestamp'],
            'label': row['label'],
        }
        for col in LIST_COLUMNS:
            new_row[col] = row[col][i]
        flattened_rows.append(new_row)

df_flat = pd.DataFrame(flattened_rows)
df_flat['timestamp'] = pd.to_datetime(df_flat['timestamp'])

# === GENERATE LABEL-SPECIFIC PUBLIC IP POOLS ===
labels = df_flat['label'].unique()
label_ip_map = {
    label: generate_unique_public_ips(20)
    for label in labels
}

# === ADD NETWORK FIELDS ===
df_flat['sip'] = [random_local_ip() for _ in range(len(df_flat))]
df_flat['sport'] = [random_ephemeral_port() for _ in range(len(df_flat))]
df_flat['dport'] = 53
df_flat['proto'] = 17

# Assign DIP from the label-specific IP pool
df_flat['dip'] = df_flat.apply(lambda row: random.choice(label_ip_map[row['label']]), axis=1)

# === REORDER COLUMNS ===
fixed_cols = ['window_id', 'timestamp', 'label', 'sip', 'sport', 'dip', 'dport', 'proto']
other_cols = [col for col in df_flat.columns if col not in fixed_cols]
df_flat = df_flat[fixed_cols + other_cols]

# === DONE ===
print(df_flat.head())

# Optional: print the IP pools
for label, ips in label_ip_map.items():
    print(f"\nPublic IPs for label '{label}':")
    for ip in ips:
        print(ip)

# Uncomment to save:
df_flat.to_csv(f"/home/mpaul/projects/tw/tw-dns/ddos_data/abnormal_flat.csv", index=False)
