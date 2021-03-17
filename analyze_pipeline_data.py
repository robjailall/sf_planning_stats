import csv
import json
import os
from argparse import ArgumentParser

import requests


def download_pipeline_data(url):
    result = requests.get(url)
    return result.json()


def main():
    parser = ArgumentParser()
    parser.add_argument("--re-download-pipeline-test_data", action="store_true", default=False)
    args = parser.parse_args()
    quarter_data = get_pipline_data_by_quarter(force_pipeline_data_download=args.re_download_pipeline_data)


def get_pipline_data_by_quarter(force_pipeline_data_download):
    pipeline_data_path = "data/pipeline_data_by_quarter.json"
    if not os.path.exists(pipeline_data_path) or force_pipeline_data_download:
        with open("data/dataset_links.txt") as f:
            reader = csv.DictReader(f, delimiter="\t")
            rows = {(row["year"], row["quarter"]): row for row in reader}

        quarter_data = {"_".join(key): download_pipeline_data(rows[key]["url"]) for key in rows}

        with open(pipeline_data_path, "w") as f:
            json.dump(quarter_data, fp=f)
    else:
        with open(pipeline_data_path) as f:
            quarter_data = json.load(fp=f)
    return quarter_data


if __name__ == "__main__":
    main()
