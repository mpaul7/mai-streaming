import click
import sys
from extractor import process_pcap_folder, process_live_interface
from ingestor import ingest_output_folder

@click.group()
def cli():
    """Network traffic analysis tool"""
    pass

@cli.command()
@click.argument('pcap_dir', type=click.Path(exists=True))
@click.argument('output_dir', type=click.Path(), default="./output")
def offline(pcap_dir, output_dir):
    """Process PCAP files and ingest to Elasticsearch"""
    try:
        process_pcap_folder(pcap_dir, output_dir)
        ingest_output_folder(output_dir)
    except Exception as e:
        click.echo(f"Error processing PCAP files: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('interface', type=str)
@click.argument('output_dir', type=click.Path(), default="./output")
def live(interface, output_dir):
    """Run live capture and real-time ingestion"""
    try:
        process_live_interface(interface, output_dir)
    except Exception as e:
        click.echo(f"Error processing live interface: {str(e)}", err=True)
        sys.exit(1)

if __name__ == '__main__':
    try:
        cli()
    except click.ClickException as e:
        click.echo(str(e), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)
