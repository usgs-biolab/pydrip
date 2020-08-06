from setuptools import setup
from setuptools import find_packages

setup(
    author="Daniel Wieferich",
    author_email="dwieferich@usgs.gov",
    python_requires=">=3.6",
    name="pydrip",
    version="0.0.1",
    description="Processing codes for building DRIP data",
    url="http://github.com/usgs-bcb/pydrip",
    license="unlicense",
    packages=find_packages(include=['pydrip','pydrip.*']),
    test_suite='tests',
    install_requires=[
        "pandas==1.0.3",
        "requests==2.22.0",
        "numpy==1.18.2",
        "sciencebasepy==1.6.9",
        "Shapely==1.7.0",
    ],
    zip_safe=False,
)
