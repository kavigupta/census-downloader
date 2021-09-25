import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="census-downloader",
    version="1.1.0",
    author="Kavi Gupta",
    author_email="censusdownloader@kavigupta.org",
    description="Clean way to download census data.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kavigupta/census-downloader",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
        "us",
        "pandas",
        "openpyxl",
        "tqdm",
        "permacache",
    ],
    entry_points={
        "console_scripts": ["census-downloader=census_downloader.cli:cli"],
    },
)
