from setuptools import setup, find_packages

setup(
    name='mai',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'click',
        'pandas',
        'elasticsearch',
    ],
    entry_points={
        'console_scripts': [
            'twc-pipeline=twc_pipeline.cli:cli',
        ],
    },
    author='Your Name',
    description='CLI tool for extracting and visualizing network traffic using TWC and Elasticsearch',
    url='https://github.com/your-username/twc-pipeline',
    classifiers=[
        'Programming Language :: Python :: 3'
    ],
    python_requires='>=3.7',
)
