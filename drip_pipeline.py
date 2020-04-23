# Import needed packages
import json
import pandas as pd

# import geopandas as gpd
from pydrip import drip_dam
from pydrip import drip_sources


def get_data():
    """
    Description
    ------------
    Retrieves source data from American Rivers Dam Removal Database and USGS Dam Removal Science Database

    Output
    -----------
    american_rivers_df: American Rivers database in pandas dataframe
    dam_removal_science_df: USGS Dam Removal Science database in pandas dataframe
    """

    # get latest American Rivers Data
    ar_url = drip_sources.get_american_rivers_data_url()
    american_rivers_df = drip_sources.read_american_rivers(ar_url)

    # get latest Dam Removal Science Data
    drd_url = drip_sources.get_science_data_url()
    dam_removal_science_df = drip_sources.read_science_data(drd_url)

    return american_rivers_df, dam_removal_science_df


def build_drip_dams_table(dam_removal_science_df, american_rivers_df):
    """
    Description
    ------------
    Builds table of all dam removals from both USGS and American Rivers sources.
    This dataset represents dams shown in the Dam Removal Science Database.
    """

    # Select fields that contain dam information or american rivers id
    dam_science_df = drip_sources.get_science_subset(
        dam_removal_science_df, target="Dam"
    )

    # For each dam in science database find best available data for the dam, first looking in science database and if null look in American Rivers
    all_dam_info = []
    for dam in dam_science_df.itertuples():
        removal_data = drip_dam.Dam(dam_id=dam.DamAccessionNumber)
        removal_data.science_data(dam)
        removal_data.update_missing_data(american_rivers_df)
        removal_data.add_geometry()
        all_dam_info.append(removal_data.__dict__)

    # For each dam only in American Rivers database, get AR data
    ar_only_dams = drip_sources.get_ar_only_dams(american_rivers_df, dam_science_df)
    for dam in ar_only_dams.itertuples():
        removal_data = drip_dam.Dam(dam_id=dam.AR_ID, dam_source="American Rivers")
        removal_data.ar_dam_data(dam)
        removal_data.add_geometry()
        all_dam_info.append(removal_data.__dict__)

    all_dam_df = pd.DataFrame(all_dam_info)

    # select only records with geometery
    all_spatial_dam_df = all_dam_df[all_dam_df["geometry"].notna()]

    # Create GeoDataFrame, set crs
    # dams_gdf = gpd.GeoDataFrame(df, geometry=df['geometry'])
    # dams_gdf.crs = {'init':'epsg:4326'}

    # NOTE: we now need to do this in the process_1 function to conform to the
    # pipeline architecture requirements
    # export as csv
    # all_spatial_dam_df.to_csv("drip_dams.csv", sep=",", index=False)

    return all_spatial_dam_df


# @Matt TODO: #current find all to_csv and mark for change to process
def export_science_tables(
    dam_removal_science_df, tables=["DamCitations", "Results", "Accession"]
):
    """
    Description
    ------------
    takes flattened USGS Dam Removal Science Database and subsets/normalizes the data extracting
    attributes specific to attributes of interest.  See drip_sources.get_science_subset for options

    currently this function exports tables in CSV format
    """
    for table in tables:
        df = drip_sources.get_science_subset(dam_removal_science_df, table)
        table_name = f"{table}.csv"
        # @Matt TODO: #current call process_1 on each df, they should each be imported as a table
        # export as csv
        df.to_csv(table_name, sep=",", index=False)


def process_1(
    path, ch_ledger, send_final_result, send_to_stage, previous_stage_result,
):
    """
    Description
    -----------
    architecture and process is based on the pipeline documentation here:
    https://code.chs.usgs.gov/fort/bcb/pipeline/docs
    """
    # @Matt TODO: #current setup process_1
    # Get american rivers and dam removal science data into dataframes
    american_rivers_df, dam_removal_science_df = get_data()

    # Build JSON Representation of Drip Dams
    all_spatial_dam_df = build_drip_dams_table(
        dam_removal_science_df, american_rivers_df
    )
    # @Matt TODO: #current if we are in mock mode, put in csv
    # all_spatial_dam_df.to_csv("drip_dams.csv", sep=",", index=False)
    record_count = 0
    for _index, dam in all_spatial_dam_df.iterrows():
        row_id = 'all_spatial_dams_' + dam['_id']
        data = {"row_id": row_id, "data": dam.to_json()}
        send_final_result(json.dumps(data))
        record_count += 1

    # Export Dam Removal Science Tables needed for DRIP
    export_science_tables(
        dam_removal_science_df, tables=["DamCitations", "Results", "Accession"]
    )

    return record_count


# @Matt TODO: document a mock run
# @Matt TODO: #current this is basically our process_1, except we need to do csv_exporting here only
def main():
    """
    Description
    ------------
    Main components needed to retrieve and manage source data for the Dam Removal Information Portal
    """

    collected_all_spatial_dam = []

    # @Matt TODO: #current the format of this needs to be row_id, data I think
    def send_final_result(record):
        json_record = json.loads(record)
        data = json.loads(json_record['data'])
        data['dataset'] = 'all_spatial_dams'
        collected_all_spatial_dam.append(data)
        # @Matt TODO: #current add to a df

    # @Matt TODO: #current if local, apply local mock pipeline
    records_processed = process_1("mock", None, send_final_result, None, None)

    collected_all_spatial_dam_df = pd.DataFrame(collected_all_spatial_dam)
    collected_all_spatial_dam_df.to_csv("drip_dams.csv", sep=",", index=False)

    print('Records processed for Spatial Dams: ', records_processed)

    # @Matt TODO: #current csv out the df

    # @Matt TODO: do we need json?
    # @Matt TODO: this saves a csv internally, so we probably do need it


if __name__ == "__main__":
    main()
