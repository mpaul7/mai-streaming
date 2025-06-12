"""
Setup configuration for mai-streaming package.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read the contents of README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

# Read requirements from requirements.txt
def read_requirements(filename: str) -> list:
    """Read requirements from file."""
    return [line.strip() 
            for line in (this_directory / filename).read_text().splitlines()
            if line.strip() and not line.startswith('#')]

setup(
    name="mai-streaming",
    version="0.1.0",
    description="Network traffic analysis and ingestion tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/yourusername/mai-streaming",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.7",
    install_requires=[
        "click>=8.0.0",
        "pandas>=1.3.0",
        "elasticsearch>=8.0.0",
        "numpy>=1.20.0",
        "pyarrow>=6.0.0",
    ],
    extras_require={
        'dev': [
            'pytest>=6.0.0',
            'pytest-cov>=2.0.0',
            'black>=22.0.0',
            'isort>=5.0.0',
            'mypy>=0.900',
            'flake8>=3.9.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'mai-streaming=mai_streaming.cli:main',
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: System :: Networking :: Monitoring",
    ],
    keywords="network traffic analysis elasticsearch monitoring",
)
