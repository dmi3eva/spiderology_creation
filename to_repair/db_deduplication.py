import json
import os
import re
import shutil
from copy import deepcopy

from configure import *
from typing import *


def extract_dedup_mapping(path: str, infix: str) -> Tuple[Dict[str, str], List[Dict]]:
    name_mapping: Dict[str, str]
    script_mapping = Dict[str, Dict[str, str]]
    count_mapping: Dict[str, int]
    db_names = os.listdir(path)
    name_mapping = dict()
    script_mapping = dict()
    count_mapping = dict()
    tables_addition = []
    tables_addition_ids = set()

    with open(SPIDER_PROCESSED_DB_SCHEMES, "r", encoding="utf-8") as json_f:
        tables = json.load(json_f)
    tables_mapping = {_t["db_id"]: _t for _t in tables}
    for db in db_names:
        if db.startswith("."):
            continue
        root = extract_root(db)
        if root not in tables_addition_ids:
            tables_addition_ids.add(root)
            tables_addition.append(tables_mapping[root])
            shutil.copytree(os.path.join(SPIDER_PROCESSED_DB_FOLDERS, root), os.path.join(path, root))
        if db == root:
            name_mapping[db] = db
            continue
        script_path = os.path.join(path, db, f"{db}.sql")
        with open(script_path, "r") as db_file:
            script = db_file.read()
        lines = re.findall(r'create table.*?\(', script)
        lines = [_l.replace("create table ", "").replace("(", "") for _l in lines]
        script = "#".join(lines)
        if root not in script_mapping.keys():
            script_mapping[root] = dict()
        if script in script_mapping[root].keys():
            name_mapping[db] = script_mapping[root][script]
            shutil.rmtree(os.path.join(path, db))
            continue
        count_mapping[root] = count_mapping.get(root, -1) + 1
        new_name = f"{root}_{infix}_{count_mapping[root]}"
        script_mapping[root][script] = new_name
        name_mapping[db] = new_name
        shutil.move(os.path.join(path, db), os.path.join(path, new_name))
        shutil.move(os.path.join(path, new_name, f"{db}.sql"), os.path.join(path, new_name, f"{new_name}.sql"))
        shutil.move(os.path.join(path, new_name, f"{db}.sqlite"), os.path.join(path, new_name, f"{new_name}.sqlite"))

    return name_mapping, tables_addition


def extract_root(db: str) -> str:
    if "dev" in db:
        return db.split("_dev")[0]
    elif "train" in db:
        return db.split("_train")[0]
    elif "other" in db:
        return db.split("_other")[0]
    return db


def repair_samples(path_to_samples: str, mapping: Dict[str, str]) -> NoReturn:
    with open(path_to_samples, "r", encoding="utf-8") as json_f:
        old = json.load(json_f)

    new_samples = []
    for _sample in old:
        row = deepcopy(_sample)
        row["db_id"] = mapping[row["db_id"]]
        new_samples.append(row)

    with open(path_to_samples, "w") as json_f:
        json.dump(new_samples, json_f)

def repair_tables(path_to_tables: str, mapping: Dict[str, str], tables_addition: List[Dict]) -> NoReturn:
    with open(path_to_tables, "r", encoding="utf-8") as json_f:
        old = json.load(json_f)
    ids = set()
    new_samples = tables_addition
    for _sample in old:
        row = deepcopy(_sample)
        row["db_id"] = mapping[row["db_id"]]
        if row["db_id"] not in ids:
            ids.add(row["db_id"])
            new_samples.append(row)
    with open(path_to_tables, "w") as json_f:
        json.dump(new_samples, json_f)


if __name__ == "__main__":
    path_to_dbs = os.path.join(SIMPLYSET_DB_PATH, "databases_joins_minus_1")
    path_to_samples = os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_minus_1", "simplyset_joins_minus_1.json")
    path_to_tables = os.path.join(SIMPLYSET_TABLES_PATH, "joins_minus_1", "tables_joins_minus_1.json")
    dedup_mapping, tables_addition = extract_dedup_mapping(path_to_dbs, "minus_1")
    repair_samples(path_to_samples, dedup_mapping)
    repair_tables(path_to_tables, dedup_mapping, tables_addition)

    path_to_dbs = os.path.join(SIMPLYSET_DB_PATH, "databases_joins_total")
    path_to_samples = os.path.join(SIMPLYSET_SAMPLES_PATH, "joins_total", "simplyset_joins_total.json")
    path_to_tables = os.path.join(SIMPLYSET_TABLES_PATH, "joins_total", "tables_joins_total.json")
    dedup_mapping, tables_addition = extract_dedup_mapping(path_to_dbs, "total")
    repair_samples(path_to_samples, dedup_mapping)
    repair_tables(path_to_tables, dedup_mapping, tables_addition)


