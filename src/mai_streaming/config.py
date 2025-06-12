"""
Configuration classes and constants for the application.
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class ESConfig:
    """Elasticsearch configuration settings."""

    url: str = "http://localhost:9200"
    bulk_chunk_size: int = 5000
    index: str = "streaming"


@dataclass
class CLIConfig:
    """Configuration for CLI operations."""

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = "streaming"
    default_output_dir: str = "./output"

    def to_es_config(self) -> ESConfig:
        """Convert CLI config to Elasticsearch config."""
        return ESConfig(url=self.elasticsearch_url, index=self.elasticsearch_index)


@dataclass
class TWCConfig:
    """Configuration for TWC command execution."""

    # Column definitions for TWC output
    COLUMNS: List[str] = field(default_factory=lambda: [
        "sip",
        "sport",
        "dip",
        "dport",
        "proto",
        "first_timestamp",
        "total_time",
        "sni",
        "vpn",
        "dd",
        "default_vpn",
        "dn",
        "dns",
        "ds",
        "application",
        "traffic_type",
    ])

    @staticmethod
    def get_pcap_extract_cmd(output_dir: str, pcap_file: str) -> List[str]:
        """Get TWC command for extracting PCAP files."""
        return [
            "twc",
            "extract",
            "-f",
            "csv",
            "--notimestamp",
            "--max-flow-packets",
            "500",
            "--min-flow-packets",
            "1",
            "-o",
            output_dir,
            pcap_file,
        ]

    @staticmethod
    def get_live_capture_cmd(output_dir: str, interface: str) -> List[str]:
        """Get TWC command for live capture."""
        return [
            "twc",
            "extract",
            "--mode",
            "live",
            "-f",
            "csv",
            "--notimestamp",
            "--min-flow-packets",
            "1",
            "--export-duration",
            "1",
            "-o",
            output_dir,
            interface,
        ]


# Constants
PROCESSED_MARKER = ".processed"
CHUNK_SIZE = 10000  # Number of records to process at once
