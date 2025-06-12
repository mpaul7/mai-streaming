import os
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd
import pyarrow.orc as orc
from elasticsearch import Elasticsearch, helpers
import glob
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from elasticsearch.helpers import BulkIndexError
from mai_streaming.config import ESConfig, PROCESSED_MARKER, CHUNK_SIZE, TWCConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ESIngestor:
    def __init__(self, config: ESConfig):
        self.config = config
        self.es = Elasticsearch(
            self.config.url,
            verify_certs=False,  # Default to False for development
            request_timeout=30,  # Default timeout
        )

    def _create_actions(self, df: pd.DataFrame, index: str) -> List[Dict[str, Any]]:
        """Create Elasticsearch bulk actions from DataFrame."""
        return [
            {"_index": index, "_source": row.dropna().to_dict()}
            for _, row in df.iterrows()
        ]

    def bulk_ingest(self, actions: List[Dict[str, Any]]) -> None:
        """Perform bulk ingestion with error handling."""
        try:
            success, failed = helpers.bulk(
                self.es,
                actions,
                chunk_size=self.config.bulk_chunk_size,
                raise_on_error=False,
            )
            if failed:
                logger.warning(f"Failed to ingest {len(failed)} documents")
            logger.info(f"Successfully ingested {success} documents")
        except BulkIndexError as e:
            logger.error(f"Bulk index error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error during bulk ingestion: {str(e)}")


def read_data_file(
    file_path: str, columns: List[str], chunk_size: Optional[int] = None
) -> Union[pd.DataFrame, Any]:
    """Read data from either CSV or ORC file."""
    file_path = Path(file_path)
    if file_path.suffix.lower() == ".csv":
        return pd.read_csv(file_path, usecols=columns, chunksize=chunk_size)
    elif file_path.suffix.lower() == ".orc":
        # Read ORC file using pyarrow
        orc_data = orc.ORCFile(file_path)
        df = orc_data.read().to_pandas()
        if columns:
            df = df[columns]
        # If chunk_size is specified, return an iterator
        if chunk_size:
            return np.array_split(df, max(len(df) // chunk_size, 1))
        return df
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


def process_data_file(file_path: str, es_ingestor: ESIngestor, index: str) -> None:
    """Process a single data file (CSV or ORC) and ingest to Elasticsearch."""
    try:
        # Read data in chunks to handle large files
        chunks = read_data_file(file_path, TWCConfig.COLUMNS, CHUNK_SIZE)

        for chunk in chunks:
            chunk["source_file"] = os.path.basename(file_path)
            chunk["ingested_at"] = datetime.utcnow().isoformat()

            # Optimize timestamp conversion
            chunk["first_timestamp"] = chunk["first_timestamp"] / 1_000_000
            chunk.loc[chunk["first_timestamp"].notna(), "first_timestamp"] = (
                pd.to_datetime(
                    chunk.loc[chunk["first_timestamp"].notna(), "first_timestamp"],
                    unit="s",
                ).dt.isoformat()
            )

            actions = es_ingestor._create_actions(chunk, index)
            if actions:
                es_ingestor.bulk_ingest(actions)

        logger.info(f"Completed processing file: {file_path}")
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {str(e)}")
        raise


def process_ddos_data(file_path: str, es_ingestor: ESIngestor, index: str) -> None:
    """Process a single DDoS data file (CSV or ORC) and ingest to Elasticsearch."""
    try:
        chunks = read_data_file(file_path, None, CHUNK_SIZE)

        for chunk in chunks:
            chunk["timestamp"] = chunk["window_id"] * 30
            chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], unit="s")

            actions = es_ingestor._create_actions(chunk, index)
            if actions:
                es_ingestor.bulk_ingest(actions)

        logger.info(f"Completed processing DDoS file: {file_path}")
    except Exception as e:
        logger.error(f"Error processing DDoS file {file_path}: {str(e)}")
        raise


def ddos_ingest_output_folder(
    folder: str, es_url: str = "http://localhost:9200", index: str = "streaming") -> None:
    """Ingest DDoS data files from a folder into Elasticsearch.

    Args:
        folder: Directory containing DDoS data files
        es_url: Elasticsearch URL (default: http://localhost:9200)
        index: Elasticsearch index name (default: streaming)
    """
    es_config = ESConfig(url=es_url)
    es_ingestor = ESIngestor(es_config)

    try:
        files = []
        for ext in ["csv", "orc"]:
            files.extend(glob.glob(os.path.join(folder, f"**/*.{ext}"), recursive=True))

        with ThreadPoolExecutor() as executor:
            for file in files:
                executor.submit(process_ddos_data, file, es_ingestor, index)
    except Exception as e:
        logger.error(f"Error processing folder {folder}: {str(e)}")
        raise


def ingest_output_folder(
    folder: str, es_url: str = "http://localhost:9200", index: str = "streaming"
) -> None:
    """Ingest data files from a folder into Elasticsearch.

    Args:
        folder: Directory containing data files
        es_url: Elasticsearch URL (default: http://localhost:9200)
        index: Elasticsearch index name (default: streaming)
    """
    es_config = ESConfig(url=es_url)
    es_ingestor = ESIngestor(es_config)
    folder_path = Path(folder)

    try:
        for ext in ["csv", "orc"]:
            for file_path in folder_path.glob(f"**/*.{ext}"):
                done_flag = file_path.with_suffix(file_path.suffix + PROCESSED_MARKER)

                if not done_flag.exists():
                    try:
                        process_data_file(str(file_path), es_ingestor, index)
                        done_flag.touch()
                    except Exception as e:
                        logger.error(f"Failed to process {file_path}: {str(e)}")
                        continue
    except Exception as e:
        logger.error(f"Error processing folder {folder}: {str(e)}")
        raise
