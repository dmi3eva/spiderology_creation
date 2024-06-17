import json
import os
import shutil
import sqlite3
from configure import *
from typing import *


def check_sampleset(samples_path: str, dbs_path: str, tables_path: str) -> NoReturn:
    with open(tables_path, "r", encoding="utf-8") as json_f:
        tables = json.load(json_f)
    table_mapping = {_t["db_id"]: _t for _t in tables}
    with open(samples_path, "r", encoding="utf-8") as json_f:
        samples = json.load(json_f)
    for _sample in samples:
        try:
            sample_table = table_mapping[_sample['db_id']]
        except Exception as e:
            print(e.__str__())
            print(_sample)
            return None
        try:
            query = _sample["query"]
            db_path = os.path.join(dbs_path, _sample['db_id'], f"{_sample['db_id']}.sqlite")
            connector = sqlite3.connect(db_path)
            connector.text_factory = lambda b: b.decode(errors='ignore')
            cursor = connector.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
        except Exception as e:
            print(e.__str__())
            print(_sample)
            return None


if __name__ == "__main__":
    ready_path = os.path.join(ROOT_DATA_PATH, "all")
    dbs_path = os.path.join(ready_path, "databases")
    tables_path = os.path.join(ready_path, "tables_all.json")
    simplyset_path = os.path.join(ready_path, "simplyset_all.json")
    testset_path = os.path.join(ready_path, "testset_all.json")

    check_sampleset(testset_path, dbs_path, tables_path)
    check_sampleset(simplyset_path, dbs_path, tables_path)

