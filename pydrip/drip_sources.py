import requests
import re
from sciencebasepy import SbSession
import pandas as pd
import io

sb = SbSession()


"""
This module uses ScienceBase and Figshare APIs to retrieve most current versions of source data.
"""

def get_science_data_url(doi_meta = 'https://api.datacite.org/works/10.5066/P9IGEC9G'):
    """
    Checks DOI for new versions of the Dam Removal Science Database.  If data has changed, extract most recent version.
 
    :param version: Which numbered version of the USNVC source to retrieve
    :param force: Set to True to force cache of ScienceBase source info regardless of pre-existence
    """
    
    #get doi metadata
    header = {'content-type':'application/json'}
    r=requests.get(doi_meta, header).json()
    url = r['data']['attributes']['url']

    #strip url to return sbid
    sbid = url.replace('https://www.sciencebase.gov/catalog/item/', '')

    #get sciencebase information
    item =sb.get_item(sbid)

    files = sb.get_item_file_info(item)
    for file in files:
        if re.search("^USGS_Dam_Removal_Science_Database_v.*csv$", file['name']):
            file_url = file['url']
            break
    
    return file_url


def get_american_rivers_data_url(url_public_api = 'https://api.figshare.com/v2/articles/5234068'):
    #get figshare item information
    header = {'content-type':'application/json'}
    r=requests.get(url_public_api, header).json()
    
    #the public api only displays file information for most recent version
    for file in r['files']:
        suffix = '.csv'
    
        if file['name'].endswith(suffix):
            file_url = file['download_url']
            break
    return file_url

def read_american_rivers(file_url):
    raw_data = requests.get(file_url).content
    df = pd.read_csv(io.StringIO(raw_data.decode('utf-8')))
    #remove unnamed columns
    df=df[df.columns[~df.columns.str.contains('Unnamed:')]]
    df['Year_Removed'] = pd.to_numeric(df['Year_Removed'],errors='coerce')
    df['Year_Built'] = pd.to_numeric(df['Year_Built'],errors='coerce')
    df['Dam_Height_ft'] = pd.to_numeric(df['Dam_Height_ft'],errors='coerce')
    return df

def read_science_data(file_url):
    df = pd.read_csv(file_url, encoding='ISO-8859-1')
    return df

def get_science_subset(science_df, target='Dam'):
    '''
    Description
    ------------
    From USGS Dam Removal Science Database return subset of the full dataframe specific to the target.
    
    Parameters
    ------------
    df: df, returned dataframe from read_science_data.  This is the full dam removal science dataset.
    target: str, options include 'Citation', 'Dam', 'Design', 'Results'  
    '''

    if target == 'Dam':
        #Select fields that contain dam information or american rivers id
        dam_data_all = science_df[science_df.columns[science_df.columns.str.contains('Dam')|science_df.columns.str.contains('AR_ID')]]
        dam_data_all = dam_data_all.drop(['DesignNumOfDamsRemoved'], axis=1) #get rid of unwanted field
        dam_data = dam_data_all.drop_duplicates()
        return dam_data

    elif target in ['Citation', 'Design', 'Results']:
        target_data_all = science_df[science_df.columns[science_df.columns.str.contains(target)]]
        target_data = target_data_all.drop_duplicates()
        return target_data

def get_ar_only_dams(american_rivers_df, dam_science_df):
    ar_in_science = dam_science_df['AR_ID'].dropna().to_list()
    ar_only_dams = american_rivers_df[~american_rivers_df['AR_ID'].isin(ar_in_science)]
    return ar_only_dams

def get_dam_data():
    pass