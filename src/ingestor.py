import os
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

processed_marker = ".processed"

def csv_to_es(es, index, csv_file):
    col = ['sip', 'sport', 'dip', 'dport', 'proto', 'first_timestamp',
       'total_time', 'sni', 'vpn', 'dd', 'default_vpn',
       'dn', 'dns', 'ds', 'application', 'dl', 'traffic_type']
    df = pd.read_csv(csv_file, usecols=col)
    df["source_file"] = os.path.basename(csv_file)
    df["ingested_at"] = datetime.utcnow().isoformat()
    # Convert epoch timestamp to calendar date
    # df['first_timestamp'] = pd.to_datetime(df['first_timestamp'], unit='ns')
    df['first_timestamp'] = df['first_timestamp'] / 1_000_000
    df['first_timestamp'] = df['first_timestamp'].apply(lambda x: datetime.fromtimestamp(x).isoformat() if pd.notnull(x) else None)
    actions = [
        {"_index": index, "_source": row.dropna().to_dict()}
        for _, row in df.iterrows()
    ]

    if actions:
        helpers.bulk(es, actions)
        print(f"[✓] Ingested {len(actions)} from {csv_file}")

def ingest_output_folder(folder, es_url="http://localhost:9200", index="twc-streaming"):
    es = Elasticsearch(es_url,
        api_key=None,  # if you're not using API key authentication
        verify_certs=False,
        request_timeout=30
    )
    for file in os.listdir(folder):
        if file.endswith(".csv"):
            filepath = os.path.join(folder, file)
            done_flag = filepath + processed_marker
            if not os.path.exists(done_flag):
                try:
                    csv_to_es(es, index, filepath)
                    open(done_flag, "w").close()
                except Exception as e:
                    print(f"[!] Failed to ingest {file}: {e}")
