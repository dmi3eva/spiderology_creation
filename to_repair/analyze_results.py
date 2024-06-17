import csv
import json
import sqlite3
import os
import shutil
from configure import *
from typing import *


def extract_just_exec(input_path: str, output_path: str, model_name: str) -> NoReturn:
    with open(input_path, "r") as f:
        source = json.load(f)
    just_exec = []
    for _row in source:
        results = _row["evaluation"][model_name]
        if results["exact_matching"] < results["execution"]:
            just_exec.append(_row)
    with open(output_path, "w") as f:
        json.dump(just_exec, f)


def extract_just_exec_csv(input_path: str, output_path: str, model_name: str, dbs_path: str) -> NoReturn:
    with open(input_path, "r") as f:
        source = json.load(f)
    just_exec = []
    for _row in source:
        results = _row["evaluation"][model_name]
        if results["exact_matching"] < results["execution"]:
            db_id = _row["db_id"]
            db_path = os.path.join(dbs_path, db_id, f"{db_id}.sqlite")
            try:
                connector = sqlite3.connect(db_path)
                connector.text_factory = lambda b: b.decode(errors='ignore')
                cursor = connector.cursor()
                cursor.execute(_row["query"])
                gt_result = cursor.fetchall()
                cursor.execute(_row["prediction_natural-sql-7b"].replace("```", ""))
                prediction_result = cursor.fetchall()
                rowed = {
                    "id": _row["id"],
                    "db_id": db_id,
                    "question": _row["question"],
                    "prediction_query": _row["prediction_natural-sql-7b"],
                    "gt_query": _row["query"],
                    "gt_result": str(gt_result),
                    "prediction_result": str(prediction_result)
                }
                just_exec.append(rowed)
                cursor.close()
                connector.close()
            except Exception as e:
                print(_row["prediction_natural-sql-7b"])
                print(e.__str__())
                return None
    with open(output_path, "w", encoding='utf-8', newline='\n') as ds_file:
        fieldnames = list(just_exec[0].keys())
        writer = csv.DictWriter(ds_file, fieldnames=fieldnames)
        writer.writeheader()
        for _s in just_exec:
            writer.writerow(_s)


def repair_prediction(input_path: str, output_path: str, model_name: str) -> NoReturn:
    with open(input_path, "r") as f:
        source = json.load(f)
    for _row in source:
        _row["predictions"] = {model_name: _row["prediction_natural-sql-7b"]}
        del _row[f"prediction_{model_name}"]
        _row["evaluation"][model_name]["human"] = _row["evaluation"][model_name]["execution"]
    with open(output_path, "w") as f:
        json.dump(source, f)


if __name__ == "__main__":
    # result_path = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "natural_sql_7b", "eval_repaired.json")
    # output_path_eval = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "just_exec.json")
    # output_path_csv = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "just_exec.csv")
    # output_path_final = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "evaluation.json")
    # dbs_path = os.path.join(ROOT_DATA_PATH, "all", "databases")
    #
    # repair_prediction(result_path, output_path_final, "natural-sql-7b")
    # extract_just_exec(result_path, output_path_eval, "natural-sql-7b")
    # extract_just_exec_csv(result_path, output_path_csv, "natural-sql-7b", dbs_path)

    result_path = os.path.join(ROOT_PATH, "to_evaluate", "lower", "dataset_with_defog.json")
    output_path_eval = os.path.join(ROOT_PATH, "to_evaluate", "lower", "analysis", "defog.json")
    output_path_csv = os.path.join(ROOT_PATH, "to_evaluate", "lower", "analysis", "defog.csv")
    output_path_final = os.path.join(ROOT_PATH, "to_evaluate", "lower", "analysis", "evaluation.json")
    dbs_path = os.path.join(ROOT_PATH, "to_evaluate", "spiderology", "databases")

    # repair_prediction(result_path, output_path_final)
    extract_just_exec(result_path, output_path_eval, "defog")
    extract_just_exec_csv(result_path, output_path_csv, "defog", dbs_path)




















