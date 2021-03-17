import csv
import json
import os
from argparse import ArgumentParser
from collections import defaultdict
import requests


def download_pipeline_data(url):
    result = requests.get(url)
    return result.json()


def main():
    parser = ArgumentParser()
    parser.add_argument("--re-download-pipeline-data", action="store_true", default=False)
    args = parser.parse_args()
    dataset_urls_by_quarter = get_dataset_urls()
    quarter_data = get_pipline_data_by_quarter(dataset_urls_by_quarter=dataset_urls_by_quarter,
                                               force_pipeline_data_download=args.re_download_pipeline_data)

    quarter_keys = sorted(["_".join(row) for row in dataset_urls_by_quarter.keys()])
    project_stats = get_stats_by_project(quarters=quarter_keys, quarter_data=quarter_data)


def get_pipline_data_by_quarter(dataset_urls_by_quarter, force_pipeline_data_download):
    pipeline_data_path = "data/pipeline_data_by_quarter.json"
    if not os.path.exists(pipeline_data_path) or force_pipeline_data_download:

        quarter_data = {"_".join(key): download_pipeline_data(dataset_urls_by_quarter[key]["url"]) for key in
                        dataset_urls_by_quarter}

        with open(pipeline_data_path, "w") as f:
            json.dump(quarter_data, fp=f)
    else:
        with open(pipeline_data_path) as f:
            quarter_data = json.load(fp=f)
    return quarter_data


def get_dataset_urls():
    with open("data/dataset_links.txt") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = {(row["year"], row["quarter"]): row for row in reader}
    return rows


def get_stats_by_project(quarters, quarter_data):
    project_status_data = defaultdict(dict)
    for quarter in quarters:
        projects = quarter_data[quarter]
        for project in projects:
            try:
                desc = project.get("nameaddr") or project["location_1"]["human_address"]
                status = project.get("beststat") or project.get("project_status")
                unitsnet = project.get("unitsnet") or 0
                project_status_data[desc] = (status, unitsnet)
            except Exception as e:
                print(project)
    return project_status_data


if __name__ == "__main__":
    main()
