import json
import os
import shutil
from configure import *
from typing import *


def merge_json(source_folder: str, source_files: List[str], target_path: str, id: str) -> NoReturn:
    processed = []
    used = set()
    for _file in source_files:
        source_path = os.path.join(source_folder, _file)
        with open(source_path, "r", encoding="utf-8") as json_f:
            data = json.load(json_f)
            for _row in data:
                if _row[id] in used:
                    continue
                else:
                    used.add(_row[id])
                    processed.append(_row)
    with open(target_path, "w") as json_f:
        json.dump(processed, json_f)


def clean_jsonchiks(simplyset: str, testset: str, tables: str) -> NoReturn:
    with open(simplyset, "r", encoding="utf-8") as json_f:
        simplyset_old_data = json.load(json_f)
    with open(testset, "r", encoding="utf-8") as json_f:
        testset_old_data = json.load(json_f)
    with open(tables, "r", encoding="utf-8") as json_f:
        tables_old_data = json.load(json_f)
    # simplyset_new_data = []
    simply_dbs = set([_r["db_id"] for _r in simplyset_old_data])
    simply_parent_ids = set([_r["parents_id"] for _r in simplyset_old_data])
    testset_new_data = [_r for _r in testset_old_data if _r["id"] in simply_parent_ids]
    tables_new_data = [_r for _r in tables_old_data if _r["db_id"] in simply_dbs]
    with open(testset, "w") as json_f:
        json.dump(testset_new_data, json_f)
    # with open(simplyset, "w") as json_f:
    #     json.dump(simplyset_new_data, json_f)
    with open(tables, "w") as json_f:
        json.dump(tables_new_data, json_f)

def clean_databases(simplyset: str, dbs: str) -> NoReturn:
    with open(simplyset, "r", encoding="utf-8") as json_f:
        simplyset_old_data = json.load(json_f)
    simply_dbs = set([_r["db_id"] for _r in simplyset_old_data])
    filenames = os.listdir(dbs)
    for _file in filenames:
        if _file == ".DS_Store":
            continue
        if _file not in simply_dbs:
            shutil.rmtree(os.path.join(dbs, _file))


if __name__ == "__main__":
    merge_json(
        os.path.join(JOINS_PATH, "samples"),
        [
            "joins_from_4_to_3/joins_from_4_to_3.json",
            "joins_from_3_to_2/joins_from_3_to_2.json",
            "joins_from_2_to_1/joins_from_2_to_1.json"
        ],
        os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_minus_1", "simplyset_joins_minus_1.json"),
        "id"
    )

    merge_json(
        os.path.join(JOINS_PATH, "sources"),
        [
            "joins_from_4_to_3/joins_from_4_to_3.json",
            "joins_from_3_to_2/joins_from_3_to_2.json",
            "joins_from_2_to_1/joins_from_2_to_1.json"
        ],
        os.path.join(SIMPLYSET_SOURCES_PATH, "joins_minus_1", "testset_joins_minus_1.json"),
        "id"
    )

    merge_json(
        os.path.join(JOINS_PATH, "tables"),
        [
            "joins_from_4_to_3/joins_from_4_to_3.json",
            "joins_from_3_to_2/joins_from_3_to_2.json",
            "joins_from_2_to_1/joins_from_2_to_1.json"
        ],
        os.path.join(SIMPLYSET_TABLES_PATH, "joins_minus_1", "tables_joins_minus_1.json"),
        "db_id"
    )

    ###################
    # Total
    ###################

    merge_json(
        os.path.join(JOINS_PATH, "samples"),
        [
            "joins_from_4_to_0/joins_from_4_to_0.json",
            "joins_from_3_to_0/joins_from_3_to_0.json",
            "joins_from_2_to_0/joins_from_2_to_0.json",
            "joins_from_1_to_0/joins_from_1_to_0.json"
        ],
        os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_total", "simplyset_joins_total.json"),
        "id"
    )

    merge_json(
        os.path.join(JOINS_PATH, "sources"),
        [
            "joins_from_4_to_3/joins_from_4_to_3.json",
            "joins_from_3_to_2/joins_from_3_to_2.json",
            "joins_from_2_to_1/joins_from_2_to_1.json",
            "joins_from_1_to_0/joins_from_1_to_0.json"

        ],
        os.path.join(SIMPLYSET_SOURCES_PATH, "joins_total", "testset_joins_total.json"),
        "id"
    )

    merge_json(
        os.path.join(JOINS_PATH, "tables"),
        [
            "joins_from_4_to_0/joins_from_4_to_0.json",
            "joins_from_3_to_0/joins_from_3_to_0.json",
            "joins_from_2_to_0/joins_from_2_to_0.json",
            "joins_from_1_to_0/joins_from_1_to_0.json"
        ],
        os.path.join(SIMPLYSET_TABLES_PATH, "joins_total", "tables_joins_total.json"),
        "db_id"
    )

    clean_jsonchiks(
        os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_minus_1", "simplyset_joins_minus_1.json"),
        os.path.join(SIMPLYSET_SOURCES_PATH, "joins_minus_1", "testset_joins_minus_1.json"),
        os.path.join(SIMPLYSET_TABLES_PATH, "joins_minus_1", "tables_joins_minus_1.json"),
    )


    clean_jsonchiks(
        os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_total", "simplyset_joins_total.json"),
        os.path.join(SIMPLYSET_SOURCES_PATH, "joins_total", "testset_joins_total.json"),
        os.path.join(SIMPLYSET_TABLES_PATH, "joins_total", "tables_joins_total.json"),
    )

    clean_databases(
        os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_minus_1", "simplyset_joins_minus_1.json"),
        os.path.join(SIMPLYSET_DB_PATH, "databases_joins_minus_1"),
    )

    clean_databases(
        os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_total", "simplyset_joins_total.json"),
        os.path.join(SIMPLYSET_DB_PATH, "databases_joins_total"),
    )

