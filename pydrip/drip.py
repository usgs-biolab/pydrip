import requests
import re
from sciencebasepy import SbSession


sb = SbSession


"""
This script is the core drip package. This process uses ScienceBase and Figshare APIs to retrieve most current versions of source data.
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
