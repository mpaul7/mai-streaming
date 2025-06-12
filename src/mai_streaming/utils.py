"""
Utility functions for the application.
"""

import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


def create_output_dir(output_dir: Path) -> None:
    """Create output directory if it doesn't exist.

    Args:
        output_dir: Path object representing the directory to create

    Raises:
        Exception: If directory creation fails
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        raise


def get_data_files(directory: Path, formats: List[str]) -> List[Path]:
    """Get all files with specified formats from directory recursively.

    Args:
        directory: Base directory to search in
        formats: List of file extensions to look for (without the dot)

    Returns:
        List of Path objects for matching files
    """
    files = []
    for fmt in formats:
        files.extend(directory.glob(f"**/*.{fmt}"))
    return files
