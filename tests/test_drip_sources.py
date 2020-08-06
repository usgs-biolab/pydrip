"""Tests of drip_sources module."""

from pydrip import drip_sources
import validators

science_url = drip_sources.get_science_data_url()
ar_url = drip_sources.get_american_rivers_data_url()


def test_get_science_data_url():
    """Validate a url is returned."""
    assert validators.url(science_url)


def test_get_american_rivers_data_url():
    """Validate a url is returned."""
    assert validators.url(ar_url)


def test_read_american_rivers():
    """Validate schema and df shape.

    This test needs to be expanded to validate schema.
    Also this test is only intended to throw a flag if
    for some reason the number of records returned is less
    than that of version 7 to allow us to look into why.
    """
    ar_df = drip_sources.read_american_rivers(ar_url)
    # v7 has 1699 records
    assert ar_df.shape[0] >= 1699


def test_read_science_data():
    """Validate schema and df shape.

    This test needs to be expanded to validate schema.
    Also this test is only intended to throw a flag if
    for some reason the number of records returned is less
    than that of version 3 to allow us to look into why.
    """
    science_df = drip_sources.read_science_data(science_url)
    # v3 had 483 records
    assert science_df.shape[0] >= 483
