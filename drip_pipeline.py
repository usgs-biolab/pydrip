import pandas as pd
from pydrip import bis_pipeline


def main():
    """

    Description
    ------------
    Main components needed to retrieve and manage source data for the
    Dam Removal Information Portal.
    This is used to run locally, all pipeline elements are mocked.
    """
    collected_data = {}

    # Is in format {'row_id': <row_id>, 'data', <json_data>}
    def send_final_result(record):
        data = record['data']
        dataset = data['dataset']

        if dataset not in collected_data:
            collected_data[dataset] = []
        collected_data[dataset].append(data)

    records_processed = bis_pipeline.process_1("mock", None, send_final_result, None, None)

    # since this is a mock, let's output all the datasets as csv tables
    for table in collected_data:
        df = pd.DataFrame(collected_data[table])
        df.to_csv(f"{table}.csv", sep=",", index=False)

    print("Records processed: ", records_processed)


if __name__ == "__main__":
    main()
