# mai-streaming# MAI CLI Pipeline

Extract and ingest network traffic using `mai`, Elasticsearch, and Click.

## Requirements

- Python 3.8+
- TWC installed in PATH
- Elasticsearch running locally or remote

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python cli.py offline /path/to/pcaps
python cli.py live eth0 /path/to/output

```
