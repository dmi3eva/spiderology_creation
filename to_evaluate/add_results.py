import json
import os
import shutil
from configure import *
from typing import *


def add_results(source_path: str, match_path: str, exec_path: str, model_name: str, out_path: str, reset=False) -> NoReturn:
    with open(source_path, "r") as f:
        source = json.load(f)
    with open(match_path, "r") as f:
        match = json.load(f)
    with open(exec_path, "r") as f:
        exec = json.load(f)
    for _s, _m, _e in zip(source, match, exec):
        if reset:
            _s["evaluation"] = {}
        else:
            _s["evaluation"] = _s.get("evaluation", {})
        _s["evaluation"][model_name] = {
            "exact_matching": _m["match"],
            "execution": _e["exec"]
        }
    with open(out_path, "w") as f:
        json.dump(source, f)


if __name__ == "__main__":
    # results_folder = os.path.join(RESULTS_PATH, "spiderology", "three")
    # source_path = os.path.join(results_folder, "two.json")
    # match_path = os.path.join(results_folder, "defog-sqlcoder_results.json")
    # exec_path = os.path.join(results_folder, "defog-sqlcoder_results.json")
    # out_path = os.path.join(results_folder, "three_evaluation.json")
    # add_results(source_path, match_path, exec_path, "defog-sqlcoder", out_path)


    # results_folder = os.path.join(ROOT_PATH, "to_evaluate", "lower")
    # source_path = os.path.join(results_folder, "three_evaluation.json")
    # match_path = os.path.join(results_folder, "natural.json")
    # exec_path = os.path.join(results_folder, "defog.json")
    # out_path = os.path.join(results_folder, "dataset_with_defog.json")
    # add_results(source_path, match_path, exec_path, "defog-sqlcoder", out_path)

    results_folder = os.path.join(ROOT_PATH, "to_evaluate", "dev")
    source_path = os.path.join(results_folder, "dev_with_predictions.json")
    out_path = os.path.join(results_folder, "dev_with_results.json")

    match_path = os.path.join(results_folder, "defog/results.json")
    exec_path = os.path.join(results_folder, "defog/results.json")

    add_results(source_path, match_path, exec_path, "defog-sqlcoder", out_path, reset=True)

    source_path = os.path.join(results_folder, "dev_with_results.json")
    match_path = os.path.join(results_folder, "natural_sql_7b/results.json")
    exec_path = os.path.join(results_folder, "natural_sql_7b/results_dequoted.json")

    add_results(source_path, match_path, exec_path, "natural-sql-7b", out_path, reset=False)
