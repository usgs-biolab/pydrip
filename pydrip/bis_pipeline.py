"""Methods to get dam removal data into bis pipeline."""

# Import needed packages
import pandas as pd

from . import drip_dam
from . import drip_sources

from datetime import datetime

# Export Dam Removal Science Tables needed for DRIP
tables = ["DamCitations",
          "Results",
          "Design",
          "Dam",
          "Accession",
          "Citation",
          "dam removal science"]

json_schema = None


def get_data():
    """Retrieve source data.

    Retrieves source data from American Rivers Dam Removal Database
    and USGS Dam Removal Science Database.

    Returns
    ----------
    american_rivers_df: pandas dataframe
        American Rivers database in pandas dataframe
    dam_removal_science_df: pandas dataframe
        USGS Dam Removal Science database in pandas dataframe

    """
    # get latest American Rivers Data
    ar_url = drip_sources.get_american_rivers_data_url()
    american_rivers_df = drip_sources.read_american_rivers(ar_url)

    # get latest Dam Removal Science Data
    drd_url = drip_sources.get_science_data_url()
    dam_removal_science_df = drip_sources.read_science_data(drd_url)

    # source data
    today = datetime.today().strftime('%Y-%m-%d')
    source_datasets = [{"source": "american rivers dam removal database",
                        "data_download_url": ar_url,
                        "data_accessed": today},
                       {"source": "usgs dam removal science database",
                        "data_download_url": drd_url,
                        "data_accessed": today}
                       ]

    return american_rivers_df, dam_removal_science_df, source_datasets


def build_drip_dams_table(dam_removal_science_df, american_rivers_df):
    """Build all needed tables of information.

    Builds table of all dam removals from both USGS and
    American Rivers sources. This dataset represents dams
    shown in the Dam Removal Science Database.

    """
    # Select fields that contain dam information or american rivers id
    dam_science_df = drip_sources.get_science_subset(
        dam_removal_science_df, target="Dam"
    )

    # Select fields that contain relationship keys in science database
    science_accession_df = drip_sources.get_science_subset(
        dam_removal_science_df, target="Accession"
    )

    # For each dam in science database find best available data for the dam
    # First looking in science database and if null look in American Rivers
    all_dam_info = []
    for dam in dam_science_df.itertuples():
        removal_data = drip_dam.Dam(dam_id=dam.DamAccessionNumber)
        removal_data.science_data(dam)
        removal_data.update_missing_data(american_rivers_df)
        removal_data.add_geometry()
        removal_data.add_science_summaries(science_accession_df)
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


def process_1(
    path, ch_ledger, send_final_result, send_to_stage, previous_stage_result,
):
    """Pipeline process.

    Architecture and process is based on the pipeline documentation here:
    https://code.chs.usgs.gov/fort/bcb/pipeline/docs

    """
    # Get american rivers and dam removal science data into dataframes
    american_rivers_df, dam_removal_science_df, source_datasets = get_data()

    # Build JSON Representation of Drip Dams
    all_spatial_dam_df = build_drip_dams_table(
        dam_removal_science_df, american_rivers_df
    )

    record_count = 0
    for _index, dam in all_spatial_dam_df.iterrows():
        dam.loc["dataset"] = "dam_removals"
        row_id = "dam_removals_" + dam["_id"]
        data = {"row_id": row_id, "data": dam.to_dict()}
        send_final_result(data)
        record_count += 1

    for table in tables:
        df = drip_sources.get_science_subset(dam_removal_science_df, table)

        for index, record in df.iterrows():
            record.loc["dataset"] = table
            row_id = f"{table}_{index}"
            data = {"row_id": row_id, "data": record.to_dict()}
            send_final_result(data)
            record_count += 1

    df = pd.DataFrame(source_datasets)
    table = "source_datasets"
    for index, record in df.iterrows():
        record.loc["dataset"] = table
        row_id = f"{table}_{index}"
        data = {"row_id": row_id, "data": record.to_dict()}
        send_final_result(data)
        record_count += 1

    return record_count
