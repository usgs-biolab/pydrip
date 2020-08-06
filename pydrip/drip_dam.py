"""Combines dam removal information from multiple sources.

This module combines dam information from two sources into a
common dataset that manages dam removals for the Dam Removal
Information Portal.  This module has a Dam class that allows
for assigning of dam characteristics.

Author
----------
Name: Daniel Wieferich
Contact: dwieferich@usgs.gov

Notes
----------
In notes and docstrings the following abbreviations are used
AR = American Rivers
AR Data = American Rivers Dam Removal Database
DRSD = USGS Dam Removal Science Database
science database = USGS Dam Removal Science Database

"""
# Import packages
import numpy as np
import sys
from shapely.geometry import Point


class Dam:
    """Builds known information about a dam removal based on sources."""

    def __init__(self, dam_id, dam_source="Dam Removal Science"):
        """Initiate dam removal object.

        Parameters
        ----------
        dam_id: str
            identifier of dam
        dam_source: str
            source of dam removal information
            options: 'American Rivers', 'Dam Removal Science'

        """
        self._id = str(dam_id)
        self.dam_source = str(dam_source)
        self.ar_id = None
        self.latitude = None
        self.longitude = None
        self.dam_built_year = None
        self.dam_removed_year = None
        self.dam_height_ft = None
        self.dam_name = None
        self.stream_name = None
        self.dam_alt_name = []
        self.stream_alt_name = []
        self.from_ar = []
        self.science_citation_ids = []
        self.science_result_ids = []

        if dam_source == "Dam Removal Science":
            self.in_drd = 1

        elif dam_source == "American Rivers":
            self.in_drd = 0

        else:
            print(
                f"Unknown Source for id: {dam_id}. "
                f"Only accepts 'American Rivers' and 'Dam Removal Science'"
            )
            sys.exit()

    def science_data(self, science_data):
        """Get data about dam from dam removal science data.

        Parameters
        ----------
        science_data: tuple
            Information about dam from dam removal science data

        """
        self.dam_science_id = str(science_data.DamAccessionNumber)

        if ~np.isnan(float(science_data.DamLatitude)) and ~np.isnan(
            float(science_data.DamLongitude)
        ):
            self.latitude = float(science_data.DamLatitude)
            self.longitude = float(science_data.DamLongitude)

        # if height value in science database is not null then set value
        if ~np.isnan(float(science_data.DamHeight_m)):
            # convert meters to feet
            self.dam_height_ft = (
                float(science_data.DamHeight_m) * 3.28084
            )
        # if removal year is not null in the science database then set value
        if ~np.isnan(science_data.DamYearRemovalFinished):
            self.dam_removed_year = int(science_data.DamYearRemovalFinished)

        # if year dam built is not null in the science database then set value
        if ~np.isnan(science_data.DamYearBuiltOriginalStructure):
            self.dam_built_year = int(
                science_data.DamYearBuiltOriginalStructure
            )
        elif ~np.isnan(science_data.DamYearBuiltRemovedStructure):
            self.dam_built_year = int(
                science_data.DamYearBuiltRemovedStructure
            )

        # if dam name is not null in the science database then set value
        if str(science_data.DamName) != "nan":
            self.dam_name = str(science_data.DamName).lower()

        # if stream name is not null in the science database then set value
        if str(science_data.DamRiverName) != "nan":
            self.stream_name = str(science_data.DamRiverName).lower()

        # if dam alt name is not null in the science database then set value
        if str(science_data.DamNameAlternate) != "nan":
            self.dam_alt_name = str(
                science_data.DamNameAlternate
            ).lower().split(",")

        # if stream alt name is not null in the science database then set value
        if str(science_data.DamRiverNameAlternate) != "nan":
            self.stream_alt_name = (
                str(science_data.DamRiverNameAlternate).lower().split(",")
            )

        # if american rivers id is not null in science database then set value
        if str(science_data.AR_ID) != "nan":
            self.ar_id = str(science_data.AR_ID)

    def update_missing_data(self, american_rivers_df):
        """Update missing data using american rivers data.

        For a dam in science database if data are missing try
        filling it in using AR Dam Removal Database (AR Data).

        Parameters
        ----------
        american_rivers_df: df
            pandas dataframe of AR Data

        """
        # If science database has AR data id and
        # AR data has information for that id
        if (
            self.ar_id is not None
            and len(american_rivers_df[american_rivers_df["AR_ID"] == self.ar_id]) > 0
        ):
            # Get AR data ID for record with matching id
            ar_id_data = american_rivers_df[american_rivers_df["AR_ID"] == self.ar_id]
            ar_id_data = ar_id_data.reset_index()

            # If lat or lon is none populate from AR data
            if self.latitude is None and ~np.isnan(ar_id_data["Latitude"][0]):
                self.latitude = float(ar_id_data["Latitude"][0])
                self.from_ar.append("latitude")
            if self.longitude is None and ~np.isnan(ar_id_data["Longitude"][0]):
                self.longitude = float(ar_id_data["Longitude"][0])
                self.from_ar.append("longitude")
            # Update dam build year from AR data if currently none
            if self.dam_built_year is None and ~np.isnan(ar_id_data["Year_Built"][0]):
                self.dam_built_year = int(ar_id_data["Year_Built"][0])
                self.from_ar.append("dam_built_year")
            # Update dam remove year from AR data if currently none
            if self.dam_removed_year is None and ~np.isnan(
                ar_id_data["Year_Removed"][0]
            ):
                self.dam_removed_year = int(ar_id_data["Year_Removed"][0])
                self.from_ar.append("dam_removed_year")
            # Update dam height from AR data if currently none
            if self.dam_height_ft is None and ~np.isnan(ar_id_data["Dam_Height_ft"][0]):
                self.dam_height_ft = float(ar_id_data["Dam_Height_ft"][0])
                self.from_ar.append("dam_height_ft")
            # Update stream name from AR data if currently none
            if self.stream_name is None and ar_id_data["River"][0]:
                self.stream_name = ar_id_data["River"][0]
            # If AR data has dam name add if new
            if ar_id_data["Dam_Name"][0]:
                name = ar_id_data["Dam_Name"][0]
                ar_dam_name, ar_alt_dam_name = clean_name(name)

                # If name is none replace with AR name
                if self.dam_name is None:
                    self.dam_name = str(ar_dam_name)
                    self.from_ar.append("dam_name")

                # All AR dam names
                ar_alt_dam_name.append(ar_dam_name)
                # All dam names currently tracked in py object
                current_dam_names = self.dam_alt_name + [self.dam_name]
                # get list of dam names that are unique to AR data
                unique = get_unique_names(current_dam_names, ar_alt_dam_name)
                unique_list = list(set(unique))
                if len(unique_list) > 0:
                    self.dam_alt_name.extend(unique_list)
                    self.from_ar.append("dam_alt_name")

    def ar_dam_data(self, dam_data):
        """Add properties to dam from the American Rivers Database.

        Parameters
        ----------
        dam_data: tuple
            Information about dam
            including attributes from the American Rivers Database

        """
        self.ar_id = dam_data.AR_ID
        self.latitude = dam_data.Latitude
        self.longitude = dam_data.Longitude
        self.dam_built_year = dam_data.Year_Built
        self.dam_removed_year = dam_data.Year_Removed
        self.dam_height_ft = dam_data.Dam_Height_ft
        self.stream_name = dam_data.River

        ar_dam_name, ar_alt_dam_name = clean_name(dam_data.Dam_Name)
        self.dam_name = ar_dam_name
        self.dam_alt_name = ar_alt_dam_name

    def add_science_summaries(self, science_accession):
        """Add science summaries to dam object.

        Use science data accession information to add information
        about citations and results related to the dam object.

        Parameters
        ----------
        science_accession: df
            Information about dam
            including attributes from the American Rivers Database

        """
        dam_accession = science_accession[science_accession['DamAccessionNumber'] == int(self.dam_science_id)]
        # if int(dam_accession.shape[0]) > 0:
        # Group by Dam ID get unique set of associated citation ids
        citations = dam_accession.groupby('DamAccessionNumber')['CitationAccessionNumber'].apply(set).reset_index(name='science_citation_ids')
        self.science_citation_ids.extend(citations['science_citation_ids'][0])
        # Group by Dam ID get unique set of associated result ids
        results = dam_accession.groupby('DamAccessionNumber')['ResultsID'].apply(set).reset_index(name='science_result_ids')
        self.science_result_ids.extend(results['science_result_ids'][0])

    def add_geometry(self):
        """Convert shapely point to wkt."""
        if self.longitude is not None and self.latitude is not None:
            geo = Point(self.longitude, self.latitude).wkt
            if geo != 'POINT (nan nan)':
                self.geometry = geo


def clean_name(name):
    """Clean common issues in name fields.

    Removes alternative name from a feature name
    Name fields in American Rivers sometimes have multiple names
    where alternative names are seperated by parentheses.
    This extracts the alt name from parenthesis and moves it to
    an alternative name field.

    Parameters
    ----------
    name: str
       initial name of feature

    Returns
    ----------
    main_name: str
        name with (alt_name) removed
    alt_name: list
        comman separated strings representing alt names without parentheses

    """
    # Separate alternative dam names from main dam name in
    # American Rivers Dam Name field
    # Also deal with a few cases where / was used instead of ()
    name = name.replace("/ Anadromous Fish Habitat Restoration", "")
    name = name.replace("/Arnold", "(Arnold)")
    name = name.replace("/Horseshoe Pond Dam", "(Horseshoe Pond Dam)")

    if (
        "(" in name and ")" in name
    ):
        main_name = name.split("(")[0] + name.split(")")[-1]
        main_name = main_name.replace("  ", " ")
        main_name = main_name.strip()

        extraction = name.split("(")[1]
        alt_name = extraction.split(")")[0]
        alt_name = [alt_name.strip()]
    else:
        main_name = name
        alt_name = []

    return main_name, alt_name


def get_unique_names(current_names, new_names):
    """Get unique names between two lists.

    Parameters
    ----------
    new_names: list
        list of strings representing new names
    current_names: list
        list of strings representing currently documented names
    """
    new_names = set(new_names)
    unique_names = [
        i.lower()
        for i in new_names
        if i.lower() not in [x.lower() for x in current_names]
        and i != ""
    ]

    return unique_names
