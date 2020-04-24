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
    install_requires=["sciencebasepy", "requests"],
    zip_safe=False,
)
