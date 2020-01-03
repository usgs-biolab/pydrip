import pandas as pd 
#import geopandas as gpd
from pydrip import drip_dam
from pydrip import drip_sources

def get_data():
    #get latest American Rivers Data
    ar_url = drip_sources.get_american_rivers_data_url()
    american_rivers_df = drip_sources.read_american_rivers(ar_url)

    #get latest Dam Removal Science Data
    drd_url = drip_sources.get_science_data_url()
    dam_removal_science_df = drip_sources.read_science_data(drd_url)

    return american_rivers_df, dam_removal_science_df 

def build_drip_dams_table(dam_removal_science_df, american_rivers_df):
    #Select fields that contain dam information or american rivers id
    dam_science_df = drip_sources.get_science_subset(dam_removal_science_df, target='Dam')

    #For each dam in science database find best available data for the dam, first looking in science database and if null look in American Rivers
    all_dam_info = []
    for dam in dam_science_df.itertuples():
        removal_data = drip_dam.Dam(dam_id=dam.DamAccessionNumber)
        removal_data.science_data(dam)
        removal_data.update_missing_data(american_rivers_df)
        removal_data.add_geometry()
        all_dam_info.append(removal_data.__dict__)

    #For each dam only in American Rivers database, get AR data
    ar_only_dams = drip_sources.get_ar_only_dams(american_rivers_df, dam_science_df)
    for dam in ar_only_dams.itertuples():
        removal_data = drip_dam.Dam(dam_id=dam.AR_ID)
        removal_data.ar_dam_data(dam)
        removal_data.add_geometry()
        all_dam_info.append(removal_data.__dict__)

    all_dam_df = pd.DataFrame(all_dam_info)
    
    #select only records with geometery
    all_spatial_dam_df = all_dam_df[all_dam_df['geometry'].notna()]
    
    #Create GeoDataFrame, set crs
    #dams_gdf = gpd.GeoDataFrame(df, geometry=df['geometry'])
    #dams_gdf.crs = {'init':'epsg:4326'}     

    #export as csv
    all_spatial_dam_df.to_csv('drip_dams.csv', sep=',', index=False)

    return all_dam_info


def main():
    #Get american rivers and dam removal science data into dataframes
    american_rivers_df, dam_removal_science_df = get_data()

    #Build JSON Representation of Drip Dams
    drip_dams = build_drip_dams_table(dam_removal_science_df, american_rivers_df)


if __name__ == "__main__":
    main()