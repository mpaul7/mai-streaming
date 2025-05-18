import subprocess
import os
import time
import glob
from ingestor import ingest_output_folder

def process_pcap_folder(pcap_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    pcap_files = glob.glob(os.path.join(pcap_dir, '**/*.pcap'), recursive=True)

    for idx, pcap in enumerate(pcap_files, 1):
        print(f"[{idx}/{len(pcap_files)}] Extracting {pcap}")
        cmd = [
            "twc", "extract", "-f", "csv",
            "--notimestamp", "--min-flow-packets", "1",
            "-out", output_dir, pcap
        ]
        subprocess.run(cmd)
        time.sleep(1)

def process_live_interface(interface, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    cmd = [
        "twc", "live", "--interface", interface, "-f", "csv",
        "--notimestamp", "--min-flow-packets", "1",
        "-out", output_dir
    ]
    print(f"[•] Starting live capture on {interface}...")
    process = subprocess.Popen(cmd)

    try:
        while True:
            ingest_output_folder(output_dir)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n[✋] Terminating live capture...")
        process.terminate()
