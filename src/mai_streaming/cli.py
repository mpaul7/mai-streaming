import click
import sys
import logging
from pathlib import Path
from typing import Optional
from mai_streaming.extractor import process_pcap_folder, process_live_interface
from mai_streaming.ingestor import (
    ddos_ingest_output_folder,
    ingest_output_folder,
)
from mai_streaming.config import CLIConfig
from mai_streaming.utils import create_output_dir, get_data_files

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

pass_config = click.make_pass_decorator(CLIConfig, ensure=True)


@click.group()
@click.option(
    "--es-url",
    envvar="ES_URL",
    default="http://localhost:9200",
    help="Elasticsearch URL (can also be set via ES_URL env var)",
)
@click.option(
    "--index",
    envvar="ES_INDEX",
    default="streaming",
    help="Elasticsearch index name (can also be set via ES_INDEX env var)",
)
@click.pass_context
def cli(ctx: click.Context, es_url: str, index: str) -> None:
    """Network traffic analysis tool.

    This tool provides functionality for:
    1. DDoS Attack Data Analysis: Ingest and analyze DDoS attack data
    2. Encrypted Traffic Classification: Process and analyze encrypted network traffic
       in both offline (PCAP files) and live (network interface) modes
    """
    ctx.obj = CLIConfig(elasticsearch_url=es_url, elasticsearch_index=index)


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--format",
    "file_format",
    type=click.Choice(["csv", "orc"]),
    default="csv",
    help="File format to process (csv or orc)",
)
@pass_config
def ddos(config: CLIConfig, input_dir: Path, file_format: str) -> None:
    """Process and ingest DDoS attack data to Elasticsearch.

    This command processes DDoS attack data files and ingests them into Elasticsearch
    for analysis. The data should contain DDoS attack metrics and patterns.

    INPUT_DIR: Directory containing DDoS data files (CSV/ORC format)
    """
    try:
        formats = []
        if file_format in ["csv"]:
            formats.append("csv")
        if file_format in ["orc"]:
            formats.append("orc")

        files = get_data_files(input_dir, formats)
        if not files:
            raise click.ClickException(
                f"No {', '.join(formats)} files found in {input_dir}"
            )

        logger.info(
            f"Starting DDoS data ingestion from directory: {input_dir} "
            f"(formats: {', '.join(formats)})"
        )

        ddos_ingest_output_folder(
            str(input_dir),
            es_url=config.elasticsearch_url,
            index=config.elasticsearch_index,
        )
        logger.info("DDoS data ingestion completed successfully")
    except Exception as e:
        logger.error(f"Error ingesting DDoS files: {e}", exc_info=True)
        raise click.ClickException(f"Error ingesting DDoS files: {str(e)}")


@cli.command()
@click.argument("pcap_dir", type=click.Path(exists=True, path_type=Path))
@click.argument("output_dir", type=click.Path(path_type=Path), default=None)
@pass_config
def offline(config: CLIConfig, pcap_dir: Path, output_dir: Optional[Path]) -> None:
    """Process PCAP files for encrypted traffic classification.

    This command analyzes PCAP files to classify encrypted network traffic,
    extracting features for traffic type identification and analysis.

    PCAP_DIR: Directory containing PCAP files to process
    OUTPUT_DIR: Directory for processed output (default: ./output)
    """
    try:
        output_dir = output_dir or Path(config.default_output_dir)
        create_output_dir(output_dir)

        logger.info(f"Processing PCAP files for traffic classification from {pcap_dir} to {output_dir}")
        process_pcap_folder(str(pcap_dir), str(output_dir))

        logger.info("Starting Elasticsearch ingestion of classified traffic data")
        ingest_output_folder(
            str(output_dir),
            es_url=config.elasticsearch_url,
            index=config.elasticsearch_index,
        )
        logger.info("Traffic classification and ingestion completed successfully")
    except Exception as e:
        logger.error(f"Error processing PCAP files: {e}", exc_info=True)
        raise click.ClickException(f"Error processing PCAP files: {str(e)}")


@cli.command()
@click.argument("interface", type=str)
@click.argument("output_dir", type=click.Path(path_type=Path), default=None)
@pass_config
def live(config: CLIConfig, interface: str, output_dir: Optional[Path]) -> None:
    """Run live encrypted traffic classification.

    This command performs real-time analysis of network traffic from a specified interface,
    classifying encrypted traffic types and patterns as they occur.

    INTERFACE: Network interface to capture from (e.g., eth0)
    OUTPUT_DIR: Directory for captured output (default: ./output)
    """
    try:
        output_dir = output_dir or Path(config.default_output_dir)
        create_output_dir(output_dir)

        logger.info(f"Starting live traffic classification on interface {interface}")
        logger.info(f"Output directory: {output_dir}")

        process_live_interface(
            interface,
            str(output_dir),
            es_url=config.elasticsearch_url,
            index=config.elasticsearch_index,
        )
    except Exception as e:
        logger.error(f"Error processing live interface: {e}", exc_info=True)
        raise click.ClickException(f"Error processing live interface: {str(e)}")


def main() -> None:
    """Main entry point for the CLI application."""
    try:
        cli()
    except click.ClickException as e:
        logger.error(str(e))
        click.echo(str(e), err=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
