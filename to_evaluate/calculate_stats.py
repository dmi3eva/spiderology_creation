import csv
import json
import os
import shutil
# import pandas as pd
from configure import *
from copy import deepcopy
from typing import *

TESTSET_TEMPLATE = {
    "examples": {"testset": 0, "simplyset": 0},
    "execution": {"testset": 0, "simplyset": 0},
    "exact_matching": {"testset": 0, "simplyset": 0},
    "human": {"testset": 0, "simplyset": 0}
}


def calculate_stats(result_path: str, model_name: str, output_csv: str, output_json: str) -> NoReturn:
    stats = dict()
    with open(result_path, "r") as in_f:
        data_df = json.load(in_f)
    for row in data_df:
        exec = row["evaluation"][model_name]["execution"]
        exact = row["evaluation"][model_name]["exact_matching"]
        human = int(row["evaluation"][model_name]["human"])
        tags = row["tags"]
        kind = "testset"
        if row["is_simplification"]:
            tags = row["simplifications_tags"]
            kind = "simplyset"
        for _tag in tags:
            if _tag.startswith("_"):
                continue
            stats[_tag] = stats.get(_tag, deepcopy(TESTSET_TEMPLATE))
            stats[_tag]["examples"][kind] += 1
            stats[_tag]["execution"][kind] += exec
            stats[_tag]["exact_matching"][kind] += exact
            stats[_tag]["human"][kind] += human
    with open(output_json, "w") as out_f:
        json.dump(stats, out_f)

    data_df = []
    for _k, _v in stats.items():
        row = {
            "name": _k,
            "examples_testset": _v["examples"]["testset"],
            "examples_simplyset": _v["examples"]["simplyset"],
            "exec_testset": _v["execution"]["testset"],
            "exec_simplyset": _v["execution"]["simplyset"],
            "exact_testset": _v["exact_matching"]["testset"],
            "exact_simplyset": _v["exact_matching"]["simplyset"],
            "human_testset": _v["human"]["testset"],
            "human_simplyset": _v["human"]["simplyset"]
        }
        data_df.append(row)
    assert len(data_df) > 0, "Empty data"
    with open(output_csv, "w", encoding='utf-8', newline='\n') as ds_file:
        fieldnames = list(data_df[0].keys())
        writer = csv.DictWriter(ds_file, fieldnames=fieldnames)
        writer.writeheader()
        for _s in data_df:
            writer.writerow(_s)
    # stats_df = pd.DataFrame(data=data_df)
    # stats_df.to_csv(output_csv, index=False, index_label=False)


if __name__ == "__main__":
    # result_path = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "three_evaluation.json")
    # output_csv = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "juierror_stats.csv")
    # output_json = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "juierror_stats.json")
    # model_name = "juierror"
    # calculate_stats(result_path, model_name, output_csv, output_json)


    # result_path = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "three_evaluation.json")
    # output_csv = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "defog-sqlcoder_stats.csv")
    # output_json = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "defog-sqlcoder_stats.json")
    # model_name = "defog-sqlcoder"
    # calculate_stats(result_path, model_name, output_csv, output_json)


    # source_path = os.path.join(ROOT_PATH, "to_evaluate", "four", "dataset_with_results.json")
    # model_name = "natural-sql-7b"
    # output_csv = os.path.join(ROOT_PATH, "to_evaluate", "four", f"{model_name}_stats.csv")
    # output_json = os.path.join(ROOT_PATH, "to_evaluate", "four", f"{model_name}_stats.json")
    # calculate_stats(source_path, model_name, output_csv, output_json)
    # model_name = "defog-sqlcoder"
    # output_csv = os.path.join(ROOT_PATH, "to_evaluate", "four", f"{model_name}_stats.csv")
    # output_json = os.path.join(ROOT_PATH, "to_evaluate", "four", f"{model_name}_stats.json")
    # calculate_stats(source_path, model_name, output_csv, output_json)


    source_path = os.path.join(ROOT_PATH, "to_evaluate", "five", "dataset_with_results.json")
    model_name = "natural-sql-7b"
    output_csv = os.path.join(ROOT_PATH, "to_evaluate", "five", f"{model_name}_stats.csv")
    output_json = os.path.join(ROOT_PATH, "to_evaluate", "five", f"{model_name}_stats.json")
    calculate_stats(source_path, model_name, output_csv, output_json)
    model_name = "defog-sqlcoder"
    output_csv = os.path.join(ROOT_PATH, "to_evaluate", "five", f"{model_name}_stats.csv")
    output_json = os.path.join(ROOT_PATH, "to_evaluate", "five", f"{model_name}_stats.json")
    calculate_stats(source_path, model_name, output_csv, output_json)