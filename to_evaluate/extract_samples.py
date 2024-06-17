import json
import os
import shutil
from configure import *
from typing import *


def process_query_to_evaluation(query: str, replace_quotes=False) -> str:
    processed = query
    processed = processed.lower()
    processed = processed.replace("\n"," ").strip()
    processed = processed.replace("\t", " ").strip()
    processed = processed.replace("\r", " ").strip()
    processed = processed.replace("\v", " ").strip()
    processed = processed.replace(" ilike ", " like ")
    while "  " in processed:
        processed = processed.replace("  ", " ")
    if replace_quotes:
        processed = processed.replace("\"", "")
    return processed


def extract_samples(result_path: str, result_folder: str, gold_key: str, prediction_key: str) -> NoReturn:
    with open(result_path, "r") as source_f:
        data = json.load(source_f)
    gold_file = os.path.join(result_folder, "gold.txt")
    prediction_file = os.path.join(result_folder, f"{prediction_key}_prediction.txt")
    prediction_file_dequoted = os.path.join(result_folder, f"{prediction_key}_prediction_dequoted.txt")
    with open(gold_file, "w") as gold_f:
        with open(prediction_file, "w") as prediction_f:
            with open(prediction_file_dequoted, "w") as prediction_deq_f:
                for i, _row in enumerate(data):
                    gold_query = process_query_to_evaluation(_row[gold_key])
                    # if prediction_key not in _row.keys():
                    #     a = 7
                    #     raise ValueError(i)
                    # prediction_query_dequoted = process_query_to_evaluation(_row[prediction_key], replace_quotes=True)
                    # prediction_query = process_query_to_evaluation(_row[prediction_key])
                    # prediction_query_dequoted = process_query_to_evaluation(_row["predictions"][prediction_key], replace_quotes=True)
                    # prediction_query = process_query_to_evaluation(_row["predictions"][prediction_key])
                    prediction_query_dequoted = process_query_to_evaluation(_row[prediction_key], replace_quotes=True)
                    prediction_query = process_query_to_evaluation(_row[prediction_key])
                    gold_f.write(f"{gold_query}\t{_row['db_id']}\n")
                    prediction_f.write(f"{prediction_query}\n")
                    prediction_deq_f.write(f"{prediction_query_dequoted}\n")


def add_db_id(results_path: str, real_dev_path: str) -> NoReturn:
    with open(results_path, "r") as f:
        results = json.load(f)

    with open(real_dev_path, "r") as f:
        source = json.load(f)
    mapping = {_r["question"].lower(): _r["db_id"] for _r in source}
    for i, _r in enumerate(results):
        if _r["question"].lower() not in mapping.keys():
            raise ValueError(_r["question"])
        _r["db_id"] = mapping[_r["question"].lower()]
    with open(results_path, "w") as f:
        json.dump(results, f)


if __name__ == "__main__":
    # results_folder = os.path.join(RESULTS_PATH, "spiderology", "natural_sql_7b")
    # results_path = os.path.join(results_folder, "spiderology_all.json")
    # extract_samples(results_path, results_folder, "query", "prediction_natural-sql-7b")

    # results_folder = os.path.join(RESULTS_PATH, "spiderology", "three")
    # results_path = os.path.join(results_folder, "three.json")
    # extract_samples(results_path, results_folder, "query", "juierror")

    # results_folder = os.path.join(RESULTS_PATH, "spiderology", "three")
    # results_path = os.path.join(results_folder, "three.json")
    # extract_samples(results_path, results_folder, "query", "defog-sqlcoder")


    # results_folder = os.path.join(RESULTS_PATH, "spiderology", "important")
    # results_path = os.path.join(results_folder, "three_evaluation.json")
    # extract_samples(results_path, results_folder, "query", "natural-sql-7b")
    # real_dev_path = os.path.join(results_folder, "dev.json")
    # add_db_id(results_path, real_dev_path)
    # extract_samples(results_path, results_folder, "gt", "prediction")

    # path.join(ROOT_PATH, "resources", "results")
    # results_folder = os.path.join(ROOT_PATH, "to_evaluate", "four")
    # results_path = os.path.join(results_folder, "dataset.json")
    # extract_samples(results_path, results_folder, "query", "natural-sql-7b")
    # extract_samples(results_path, results_folder, "query", "defog-sqlcoder")


    # results_folder = os.path.join(ROOT_PATH, "to_evaluate", "five")
    # results_path = os.path.join(results_folder, "dataset.json")
    # extract_samples(results_path, results_folder, "query", "natural-sql-7b")
    # extract_samples(results_path, results_folder, "query", "defog-sqlcoder")

    results_folder = os.path.join(RESULTS_PATH, "dev", "defog_sqlcoder")
    # results_folder = os.path.join(ROOT_PATH, "to_evaluate", "five")
    results_path = os.path.join(results_folder, "dev_defog-sqlcoder.json")
    extract_samples(results_path, results_folder, "query", "predictions_defog-sqlcoder")