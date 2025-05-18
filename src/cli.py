import click
from extractor import process_pcap_folder, process_live_interface
from ingestor import ingest_output_folder

@click.group()
def cli():
    pass

@cli.command()
@click.option('--pcap-dir', required=True, type=click.Path(exists=True), help="Path to folder containing PCAPs")
@click.option('--output-dir', default="./output", type=click.Path(), help="Where to store extracted CSVs")
def offline(pcap_dir, output_dir):
    """Process PCAP files and ingest to Elasticsearch"""
    process_pcap_folder(pcap_dir, output_dir)
    ingest_output_folder(output_dir)

@cli.command()
@click.option('--interface', required=True, help="Network interface name (e.g., eth0)")
@click.option('--output-dir', default="./output", type=click.Path(), help="Where to store live CSVs")
def live(interface, output_dir):
    """Run live capture and real-time ingestion"""
    process_live_interface(interface, output_dir)

if __name__ == '__main__':
    cli()
