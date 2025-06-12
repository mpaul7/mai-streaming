# MAI Streaming

A network traffic analysis and ingestion tool that processes network traffic data and ingests it into Elasticsearch.

## Features

- Process CSV and ORC files
- Live network traffic capture
- PCAP file processing
- Elasticsearch ingestion
- Multi-format support

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/yourusername/mai-streaming.git
cd mai-streaming

# Install in development mode
pip install -e .

# Install with development dependencies
pip install -e .[dev]
```

### Using pip

```bash
pip install mai-streaming
```

## Usage

### Basic Commands

1. Ingest CSV or ORC files:
```bash
mai-streaming ingest /path/to/data/dir --format csv
mai-streaming ingest /path/to/data/dir --format orc
```

2. Process PCAP files:
```bash
mai-streaming offline /path/to/pcap/dir /path/to/output/dir
```

3. Live capture:
```bash
mai-streaming live eth0 /path/to/output/dir
```

### Configuration

You can configure the Elasticsearch connection using:
- Command line: `--es-url http://localhost:9200`
- Environment variable: `export ES_URL=http://localhost:9200`

## Development

1. Install development dependencies:
```bash
pip install -e .[dev]
```

2. Run tests:
```bash
pytest
```

3. Format code:
```bash
black src/
isort src/
```

4. Type checking:
```bash
mypy src/
```

5. Lint:
```bash
flake8 src/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
