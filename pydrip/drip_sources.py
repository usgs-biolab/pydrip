"""Access dam removal data from sources.

This module uses ScienceBase and Figshare APIs to retrieve most current
versions of source data for the Dam Removal Information Portal (DRIP).
Source datasets include the USGS Dam Removal Science Database and the
American Rivers Dam Removal Database.  Functions in this module also
allow for subsetting of source data.

Author
----------
Name: Daniel Wieferich
Contact: dwieferich@usgs.gov
"""

# Import packages
import requests
import re
from sciencebasepy import SbSession
import pandas as pd
import io
import numpy as np

sb = SbSession()

######################################################################
######################################################################


def get_science_data_url(
    doi_meta="https://api.datacite.org/works/10.5066/P9IGEC9G"
):
    """Get url for newest version of Dam Removal Science Database.

    Checks DOI for newest version of the Dam Removal Science Database.
    Returns download url for most recent version of databases as CSV.

    Parameters
    ----------
    doi_meta: str
        datacite api call to specific DOI for dam removal science database

    Returns
    ----------
    file_url: str
        url to access dam removal database
    """
    # this gets metadata about the Dam Removal Science Database
    header = {"content-type": "application/json"}
    r = requests.get(doi_meta, header).json()
    url = r["data"]["attributes"]["url"]

    # strip url to return sbid
    sbid = url.replace("https://www.sciencebase.gov/catalog/item/", "")

    # get sciencebase information
    item = sb.get_item(sbid)

    files = sb.get_item_file_info(item)
    for file in files:
        if re.search("^USGS_Dam_Removal_Database_v.*csv$", file["name"]):
            file_url = file["url"]
            break

    return file_url


def get_american_rivers_data_url(
    url_public_api="https://api.figshare.com/v2/articles/5234068"
):
    """Get url for newest version of American Rivers dam removal data.

    Checks for newest version of the American Rivers dam removal database.
    Returns download url for most recent version of database CSV.

    Parameters
    ----------
    url_public_api: str
        Url for connecting to American Rivers database via figshare APIta

    Returns
    ----------
    file_url: str
        url to access (download) American Rivers dam removal database

    """
    # get figshare item information
    header = {"content-type": "application/json"}
    r = requests.get(url_public_api, header).json()

    # the public api only displays file information for most recent version
    for file in r["files"]:
        suffix = ".csv"

        if file["name"].endswith(suffix):
            file_url = file["download_url"]
            break

    return file_url


def read_american_rivers(file_url):
    """Read in American Rivers Dam Removal Database into pandas dataframe.

    Parameters
    ----------
    file_url: str
        Url to access American Rivers database
        Get from get_american_rivers_data_url()

    Returns
    ----------
    df: pandas dataframe
        Pandas dataframe with American Rivers Dam Removal Database

    """
    raw_data = requests.get(file_url).content
    df = pd.read_csv(io.StringIO(raw_data.decode("utf-8")))
    # remove unnamed columns
    df = df[df.columns[~df.columns.str.contains("Unnamed:")]]
    df["Year_Removed"] = pd.to_numeric(df["Year_Removed"], errors="coerce")
    df["Year_Built"] = pd.to_numeric(df["Year_Built"], errors="coerce")
    df["Dam_Height_ft"] = pd.to_numeric(df["Dam_Height_ft"], errors="coerce")
    return df


def read_science_data(file_url):
    """Read in USGS Dam Removal Science Database in pandas dataframe.

    Reads in the flattened version (CSV) of the USGS Dam Removal
    Science Database into pandas dataframe.

    Parameters
    ----------
    file_url: str
        Url to access dam removal science database
        Get from get_science_data_url()

    Returns
    ----------
    df: pandas dataframe
        Pandas dataframe with Dam Removal Science Database
    """
    df = pd.read_csv(file_url, encoding="ISO-8859-1")
    return df


def get_science_subset(science_df, target="Dam"):
    """Return subsets of USGS Dam Removal Science Database.

    From USGS Dam Removal Science Database return subset of the
    full dataframe specific to the target.

    Parameters
    ----------
    df: pandas dataframe
        Return dataframe from read_science_data.
        This is the full dam removal science dataset.
    target: str
        options include 'Citation', 'Dam', 'Design', 'Results', 'Accession'
    """
    if target == "Dam":
        # Select fields that contain dam information or american rivers id
        dam_data_all = science_df[
            science_df.columns[
                science_df.columns.str.contains("Dam")
                | science_df.columns.str.contains("AR_ID")
            ]
        ]
        dam_data_all = dam_data_all.drop(
            ["DesignNumOfDamsRemoved"], axis=1
        )  # get rid of unwanted field
        dam_data = dam_data_all.drop_duplicates()
        return dam_data

    elif target == "Accession":
        # Select fields that contain accession keys
        accession_data_all = science_df[
            science_df.columns[
                science_df.columns.str.contains("Accession")
                | science_df.columns.str.contains("ResultsID")
                | science_df.columns.str.contains("DesignID")
            ]
        ]
        accession_data = accession_data_all.drop_duplicates()
        return accession_data

    elif target == "Results":
        # Select fields that contain accession keys
        results_data_all = science_df[
            science_df.columns[
                science_df.columns.str.contains("Results")
                | science_df.columns.str.contains("CitationAccessionNumber")
            ]
        ]
        results_data = results_data_all.drop_duplicates()
        return results_data

    elif target == "Citation":
        # need to include damaccessionnumber so we can create citations per dam
        citation_data_all = science_df[
            science_df.columns[science_df.columns.str.contains("Citation")]
        ]
        citation_data = citation_data_all.drop_duplicates()
        return citation_data

    elif target == "DamCitations":
        # need to include damaccessionnumber so we can create citations per dam
        dam_citation_data_all = science_df[
            science_df.columns[
                science_df.columns.str.contains("Citation")
                | science_df.columns.str.contains("DamAccessionNumber")
            ]
        ]
        dam_citation_data = dam_citation_data_all.drop_duplicates()
        relevant = []
        for citation in dam_citation_data.itertuples():
            dam = citation.DamAccessionNumber
            doi = f"https://doi.org/{citation.CitationDOI}"
            if ~np.isnan(citation.CitationYear):
                citation = f"{citation.CitationAuthor}, {str(int(citation.CitationYear))}, {citation.CitationTitle}"
            else:
                citation = f"{citation.CitationAuthor}, {citation.CitationTitle}"
            relevant.append(
                {"DamAccessionNumber": dam,
                 "CitationDOI": doi,
                 "Citation": citation}
            )
            relevant_dam_citation_data = pd.DataFrame(relevant)
        return relevant_dam_citation_data

    elif target in ["Design"]:
        design_data_all = science_df[
            science_df.columns[science_df.columns.str.contains(target)]
        ]
        design_data = design_data_all.drop_duplicates()
        return design_data


def get_ar_only_dams(american_rivers_df, dam_science_df):
    """Find dam removal record in AR database not in Science Database.

    Finds dam removal records in American Rivers database that are 
    not documented in the USGS Dam Removal Science Database.

    Parameters
    ------------
    american_rivers_df: pandas dataframe of full American Rivers Database
    dam_science_df: pandas dataframe of full USGS Dam Removal Science Database
    """
    ar_in_science = dam_science_df["AR_ID"].dropna().to_list()
    ar_only_dams = american_rivers_df[~american_rivers_df["AR_ID"].isin(ar_in_science)]
    return ar_only_dams
