import csv
import json
import os
import shutil
import sqlite3
from configure import *
from typing import *


def find_empty(input_path: str, output_path: str, dbs_path: str) -> NoReturn:
    with open(input_path, "r") as f:
        source = json.load(f)
    empties = []
    for _row in source:
        db_id = _row["db_id"]
        db_path = os.path.join(dbs_path, db_id, f"{db_id}.sqlite")
        try:
            connector = sqlite3.connect(db_path)
            connector.text_factory = lambda b: b.decode(errors='ignore')
            cursor = connector.cursor()
            cursor.execute(_row["query"])
            prediction_result = cursor.fetchall()
            if len(prediction_result) != 0 and (not isinstance(prediction_result, list) or not prediction_result[0][0] is None):
                continue
            rowed = {
                "id": _row["id"],
                "db_id": db_id,
                "question": _row["question"],
                # "prediction_query": _row["predictions"]["natural-sql-7b"],
                "gt_query": _row["query"],
                "prediction_result": str(prediction_result),
                "tags": str(_row["tags"]),
                "simplifications_tags": str(_row["simplifications_tags"])
            }
            empties.append(rowed)
            cursor.close()
            connector.close()
        except Exception as e:
            print(_row["query"])
            print(e.__str__())
            return None
    with open(output_path, "w", encoding='utf-8', newline='\n') as ds_file:
        fieldnames = list(empties[0].keys())
        writer = csv.DictWriter(ds_file, fieldnames=fieldnames)
        writer.writeheader()
        for _s in empties:
            writer.writerow(_s)


if __name__ == "__main__":
    input_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "evaluation.json")
    output_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "empty.csv")
    dbs_path = os.path.join(ROOT_DATA_PATH, "all", "databases")
    find_empty(input_path, output_path, dbs_path)




