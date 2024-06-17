import csv
import os
import json
from copy import deepcopy
from typing import *
from configure import *


def correct_file(corrections_path: str, path_to_correct: str) -> NoReturn:
    with open(path_to_correct, "r") as f:
        samples = json.load(f)
    with open(corrections_path, "r", encoding='utf-8') as r_file:
        reader = csv.DictReader(r_file, delimiter=",")
        corrections = list(reader)

    # To lower case
    for _sample in samples:
        _sample["query"] = _sample["query"].lower()
        if "predictions" not in _sample.keys():
            continue
        for _k, _v in _sample["predictions"].items():
            _sample["predictions"][_k] = _v.lower()
            _sample["evaluation"]["natural-sql-7b"]["human"] = _sample["evaluation"]["natural-sql-7b"]["execution"]
            _sample["evaluation"]["defog-sqlcoder"]["human"] = _sample["evaluation"]["defog-sqlcoder"]["execution"]

    # Corrections
    mapping = {_s["id"]: _s for _s in samples}
    for _correction in corrections:
        if _correction["id"] not in mapping.keys():
            # print(corrections)
            continue
        sample_to_correct = mapping[_correction["id"]]
        if _correction["field"] in sample_to_correct.keys():
            sample_to_correct[_correction["field"]] = _correction["new"]
            continue
        if "predictions" not in sample_to_correct.keys():
            continue
        if _correction["field"] == "prediction_natural-sql-7b":
            sample_to_correct["predictions"]["natural-sql-7b"] = _correction["new"].lower()
        elif _correction["field"] == "prediction_defog-sqlcoder":
            sample_to_correct["predictions"]["defog-sqlcoder"] = _correction["new"].lower()
        elif _correction["field"] == "human_natural-sql-7b":
            sample_to_correct["evaluation"]["natural-sql-7b"]["human"] = _correction["new"]
        elif _correction["field"] == "human_defog-sqlcoder":
            sample_to_correct["evaluation"]["defog-sqlcoder"]["human"] = _correction["new"]
        else:
            print(f"There is no: {_correction}")
            continue

    with open(path_to_correct, "w") as f:
        json.dump(samples, f)


def correct_folder(corrections_path: str, folder_to_correct: str) -> NoReturn:
    files_to_correct = []
    folders = [_t for _t in os.listdir(folder_to_correct) if not _t.startswith(".")]
    for _folder in folders:
        all_files = os.listdir(os.path.join(folder_to_correct, _folder))
        full_paths = [os.path.join(folder_to_correct, _folder, _fi) for _fi in all_files if not _fi.startswith("tables_") and _fi.endswith(".json")]
        files_to_correct += full_paths
    for _f in files_to_correct:
        correct_file(corrections_path, _f)


folder_to_correct = ROOT_DATA_PATH
corrections_path = "data/corrections/corrections.csv"
path_to_correct = os.path.join(folder_to_correct, "dataset_with_results.json")
correct_file(corrections_path, path_to_correct)
#
# path_to_correct = os.path.join(folder_to_correct, "all", "simplyset_all.json")
# correct_file(corrections_path, path_to_correct)
#
# path_to_correct = os.path.join(folder_to_correct, "all", "spiderology.json")
# correct_file(corrections_path, path_to_correct)
#
# path_to_correct = os.path.join(folder_to_correct, "all", "testset_all.json")
# correct_file(corrections_path, path_to_correct)
# correct_folder(corrections_path, os.path.join(folder_to_correct, "testsets"))