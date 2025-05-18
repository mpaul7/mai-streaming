import os
import pandas as pd
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

processed_marker = ".processed"

def csv_to_es(es, index, csv_file):
    df = pd.read_csv(csv_file)
    df["source_file"] = os.path.basename(csv_file)
    df["ingested_at"] = datetime.utcnow().isoformat()

    actions = [
        {"_index": index, "_source": row.dropna().to_dict()}
        for _, row in df.iterrows()
    ]

    if actions:
        helpers.bulk(es, actions)
        print(f"[✓] Ingested {len(actions)} from {csv_file}")

def ingest_output_folder(folder, es_url="http://localhost:9200", index="network_flows"):
    es = Elasticsearch(es_url)
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
