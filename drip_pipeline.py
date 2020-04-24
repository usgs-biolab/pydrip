import json
import pandas as pd
import pydrip

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
        df = pydrip.drip_sources.get_science_subset(dam_removal_science_df, table)
        table_name = f"{table}.csv"
        df.to_csv(table_name, sep=",", index=False)


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
        data = json_record['data']
        dataset = data['dataset']

        if dataset not in collected_data:
            collected_data[dataset] = []
        collected_data[dataset].append(data)

    records_processed = pydrip.bis_pipeline.process_1("mock", None, send_final_result, None, None)

    # since this is a mock, let's output all the datasets as csv tables
    for table in collected_data:
        df = pd.DataFrame(collected_data[table])
        df.to_csv(f"{table}.csv", sep=",", index=False)

    print("Records processed: ", records_processed)


if __name__ == "__main__":
    main()
