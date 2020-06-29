from setuptools import setup

setup(
    name="pydrip",
    version="0.0.1",
    description="Processing codes for building DRIP data",
    url="http://github.com/usgs-bcb/pydrip",
    author="Daniel Wieferich",
    author_email="bcb@usgs.gov",
    license="unlicense",
    packages=["pydrip"],
    install_requires=[
        "pandas==1.0.3",
        "requests==2.22.0",
        "numpy==1.18.2",
        "sciencebasepy==1.6.9",
        "Shapely==1.7.0",
    ],
    zip_safe=False,
)
