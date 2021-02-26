"""Tests of drip_sources module."""

from pydrip import drip_dam

test_dam = drip_dam.Dam(dam_id=1)


def test_clean_name():
    """Test a few common name issues."""
    test_data = [{'in_name': 'Upper Dam (Lost Man Dam)',
                  'out_name': 'upper dam',
                  'out_list': ['lost man dam']
                  },
                 {'in_name': 'Russell (Hinkley) Dam',
                  'out_name': 'russell dam',
                  'out_list': ['hinkley']
                  },
                 {'in_name': 'Glenbrook/ Anadromous Fish Habitat Restoration',
                  'out_name': 'glenbrook',
                  'out_list': []
                  }]
    for t in test_data:
        dam_name, alt_dam_name = drip_dam.clean_name(
            t['in_name']
        )
        assert dam_name == t['out_name']
        assert alt_dam_name == t['out_list']


def test_get_unique_names():
    """Test adding of new names."""
    # this is strategy in update missing data for building current names
    # list from name and alt names, figured I would test that too
    current_name = 'murphy creek'
    current_alt_names = ['sparrowk dam']
    current_names = current_alt_names + [current_name]
    new_names = ['Murphy Creek Dam', 'Sparrow Dam', '', 'Sparrowk dam']

    unique_names = set(drip_dam.get_unique_names(current_names, new_names))
    expected = set(['sparrow dam', 'murphy creek dam'])
    assert unique_names == expected


def test_add_geometry():
    """Test to make sure no nan or no values."""
    test_dam.latitude = 40.25
    test_dam.longitude = -90.25
    test_dam.add_geometry()
    assert test_dam.geometry == 'POINT (-90.25 40.25)'
