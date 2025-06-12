import glob
import os
import subprocess
import time
import logging
from pathlib import Path
from typing import Optional
from mai_streaming.ingestor import ingest_output_folder
from mai_streaming.config import TWCConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def process_pcap_folder(pcap_dir: str, output_dir: str) -> None:
    """Process all PCAP files in a directory."""
    logger.info(f"Processing PCAP files from {pcap_dir}")
    os.makedirs(output_dir, exist_ok=True)
    pcap_files = glob.glob(os.path.join(pcap_dir, "**/*.pcap"), recursive=True)

    for idx, pcap in enumerate(pcap_files, 1):
        print(f"[{idx}/{len(pcap_files)}] Extracting {pcap}")
        cmd = TWCConfig.get_pcap_extract_cmd(output_dir, pcap)
        subprocess.run(cmd)
        # time.sleep(1)


def process_live_interface(interface: str, output_dir: str, es_url: str = "http://localhost:9200", index: str = "twc_streaming") -> None:
    """Process live network traffic from an interface."""
    logger.info(f"Capturing traffic from interface {interface}")
    os.makedirs(output_dir, exist_ok=True)
    cmd = TWCConfig.get_live_capture_cmd(output_dir, interface)
    print(f"Starting live capture on {interface}...")
    process = subprocess.Popen(cmd)

    try:
        while True:
            ingest_output_folder(output_dir, es_url=es_url, index=index)
            # time.sleep(5)
    except KeyboardInterrupt:
        print("\nTerminating live capture...")
        process.terminate()
