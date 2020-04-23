# Import needed packages
import json
import pandas as pd

# import geopandas as gpd
from pydrip import drip_dam
from pydrip import drip_sources

# Export Dam Removal Science Tables needed for DRIP
tables = ["DamCitations", "Results", "Accession"]


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

    return all_spatial_dam_df


# NOTE: This function is not currently used, we do the exporting in main
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
        df.to_csv(table_name, sep=",", index=False)


# This architecture and process is based on the pipeline documentation here: https://code.chs.usgs.gov/fort/bcb/pipeline/docs
def process_1(
    path, ch_ledger, send_final_result, send_to_stage, previous_stage_result,
):
    """
    Description
    -----------
    architecture and process is based on the pipeline documentation here:
    https://code.chs.usgs.gov/fort/bcb/pipeline/docs
    """
    # Get american rivers and dam removal science data into dataframes
    american_rivers_df, dam_removal_science_df = get_data()

    # Build JSON Representation of Drip Dams
    all_spatial_dam_df = build_drip_dams_table(
        dam_removal_science_df, american_rivers_df
    )

    record_count = 0
    for _index, dam in all_spatial_dam_df.iterrows():
        dam.loc["dataset"] = "drip_dams"
        row_id = "drip_dams_" + dam["_id"]
        data = {"row_id": row_id, "data": dam.to_json()}
        send_final_result(json.dumps(data))
        record_count += 1

    for table in tables:
        df = drip_sources.get_science_subset(dam_removal_science_df, table)

        for index, record in df.iterrows():
            record.loc["dataset"] = table
            row_id = f"{table}_{index}"
            data = {"row_id": row_id, "data": record.to_json()}
            send_final_result(json.dumps(data))
            record_count += 1

    return record_count


def main():
    """
    Description
    ------------
    Main components needed to retrieve and manage source data for the Dam Removal Information Portal
    This is used to run locally, all pipeline elements are mocked.
    """

    collected_data = {}

    # Is in format {'row_id': <row_id>, 'data', <json_data>}
    def send_final_result(record):
        json_record = json.loads(record)
        data = json.loads(json_record["data"])

        dataset = data["dataset"]

        if dataset not in collected_data:
            collected_data[dataset] = []
        collected_data[dataset].append(data)

    records_processed = process_1("mock", None, send_final_result, None, None)

    # since this is a mock, let's output all the datasets as csv tables
    for table in collected_data:
        df = pd.DataFrame(collected_data[table])
        df.to_csv(f"{table}.csv", sep=",", index=False)

    print("Records processed: ", records_processed)


if __name__ == "__main__":
    main()
