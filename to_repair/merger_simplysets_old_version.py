import os
import json
import shutil
import subprocess

from atomic import *
from typing import *
from configure import *


def merge_simplysets(source_folder: str, target_folder: str, folder_with_spider_dbs: str, spider_table_path: str) -> NoReturn:
    source_folder_database = os.path.join(source_folder, "dbs")
    source_folder_tables = os.path.join(source_folder, "tables")
    source_folder_samples = os.path.join(source_folder, "samples")
    target_folder_database = os.path.join(target_folder, "database")
    target_file_tables = os.path.join(target_folder, "tables.json")
    target_file_samples = os.path.join(target_folder, "spiderology.json")
    folder_with_spider_samples = os.path.join(source_folder, "sources")
    spider_samples = extract_spider_samples(folder_with_spider_samples)
    merge_dbs(source_folder_database, target_folder_database, spider_samples, folder_with_spider_dbs)
    merge_tables(source_folder_tables, target_file_tables, spider_samples, spider_table_path)
    merge_samples(source_folder_samples, target_file_samples, spider_samples)


def merge_dbs(source_folder: str, target_folder: str, spider_samples: List[Dict], folder_with_spider_dbs: str) -> NoReturn:
    # Moving Spiderology DBs
    folders = [_f for _f in os.listdir(source_folder) if not _f.startswith(".")]
    for _folder_with_dbs in folders:
        folder_path = os.path.join(source_folder, _folder_with_dbs)
        db_folder_names = [_f for _f in os.listdir(folder_path) if not _f.startswith(".")]
        for _db_folder_name in db_folder_names:
            db_folder = os.path.join(folder_path, _db_folder_name)
            target = os.path.join(target_folder, _db_folder_name)
            if not os.path.exists(target):
                shutil.copytree(db_folder, target)  # TO-DO: Raise exception

    # Moving source Spider DBs
    spider_db_names = list(set([_s["db_id"] for _s in spider_samples]))
    spider_db_source_paths = [os.path.join(folder_with_spider_dbs, _d) for _d in spider_db_names]
    spider_db_target_paths = [os.path.join(target_folder, _d) for _d in spider_db_names if _d is not None]
    for _source, _target in zip(spider_db_source_paths, spider_db_target_paths):
        if not os.path.exists(_target):
            shutil.copytree(_source, _target)


def merge_tables(source_folder: str, target_file: str, spider_samples: List[Dict], spider_table_path: str) -> NoReturn:
    folders = [_f for _f in os.listdir(source_folder) if not _f.startswith(".")]
    result_tables = []
    spider_db_ids = set()
    for _folder in folders:
        folder_path = os.path.join(source_folder, _folder)
        filename = os.listdir(folder_path)[0]
        filepath = os.path.join(folder_path, filename)
        with open(filepath, "r", encoding="utf-8") as current_file:
            all_samples = json.load(current_file)
        result_tables.extend(all_samples)
    for candidate in spider_samples:
        if candidate["db_id"] not in spider_db_ids:
            spider_db_ids.add(candidate["db_id"])
    with open(spider_table_path, "r", encoding="utf-8") as current_file:
        tables = json.load(current_file)
    corresponding_tables = [_t for _t in tables if _t["db_id"] in spider_db_ids]
    result_tables.extend(corresponding_tables)
    existing = []
    if os.path.exists(target_file):
        with open(target_file, "r", encoding="utf-8") as current_file:
            existing = json.load(current_file)
    existing_db_ids = [_e["db_id"] for _e in existing]
    for current in result_tables:
        if current["db_id"] not in existing_db_ids:
            existing_db_ids.append(current["db_id"])
            existing.append(current)
    with open(target_file, "w", encoding="utf-8") as current_file:
        json.dump(existing, current_file)


def merge_samples(source_folder_samples: str, target_file_samples: str, spider_samples: List[Dict]) -> NoReturn:
    folders = [_f for _f in os.listdir(source_folder_samples) if not _f.startswith(".")]
    result_samples = []
    id_prefix = ""
    for _folder in folders:
        folder_path = os.path.join(source_folder_samples, _folder)
        filename = os.listdir(folder_path)[0]
        id_prefix = filename.split(".json")[0].split("_")[0]
        filepath = os.path.join(folder_path, filename)
        with open(filepath, "r", encoding="utf-8") as current_file:
            all_samples = json.load(current_file)
        result_samples.extend(all_samples)
    existing = []
    if os.path.exists(target_file_samples):
        with open(target_file_samples, "r", encoding="utf-8") as current_file:
            existing = json.load(current_file)
    ids = [_e.get("id", None) for _e in existing if _e and _e.get("id", None) != -1]
    final_samples = []
    for candidate in spider_samples:
        if candidate["id"] not in ids:
            ids.append(candidate["id"])
            final_samples.append(candidate)
    corresponding_ids = [int(_e.split("_")[1]) for _e in ids if _e.startswith(f"{id_prefix}_")]
    last_n = max(corresponding_ids) if len(corresponding_ids) > 0 else -1
    ind = last_n + 1
    for current in result_samples:
        current["id"] = f"{id_prefix}_{ind}"
        ind += 1
        final_samples.append(current)
    with open(target_file_samples, "w", encoding="utf-8") as current_file:
        json.dump(final_samples, current_file)


def extract_spider_samples(folder_with_spider_samples: str) -> List[Dict]:
    folders = [_f for _f in os.listdir(folder_with_spider_samples) if not _f.startswith(".")]
    spider_samples = []
    mapping: Dict[str, Dict]
    mapping = dict()
    for _folder in folders:
        folder_path = os.path.join(folder_with_spider_samples, _folder)
        filename = [_f for _f in os.listdir(folder_path) if _f != ".DS_Store"][0]
        filepath = os.path.join(folder_path, filename)
        with open(filepath, "r", encoding="utf-8") as current_file:
            all_samples = json.load(current_file)
        appropriate_samples = all_samples
        # appropriate_samples = [_s for _s in all_samples if Tag.JOINED.name in _s["tags"]]
        for candidate in appropriate_samples:
            if candidate["id"] not in mapping.keys():
                mapping[candidate["id"]] = candidate
            else:
                all_tags = mapping[candidate["id"]]["tags"] + candidate["tags"]
                all_tags = list(set(all_tags))
                mapping[candidate["id"]]["tags"] = all_tags
                all_simplifications_tags = mapping[candidate["id"]]["simplifications_tags"] + candidate["simplifications_tags"]
                all_simplifications_tags = list(set(all_simplifications_tags))
                mapping[candidate["id"]]["simplifications_tags"] = all_simplifications_tags
        spider_samples = list(mapping.values())
    return spider_samples


if __name__ == "__main__":
    source_folder = SIMPLY_SET_PATH
    target_folder = os.path.join(SIMPLY_SET_PATH, "ready")
    folder_with_spider_dbs = SPIDER_PROCESSED_DB_FOLDERS
    spider_table_path = SPIDER_PROCESSED_DB_SCHEMES
    merge_simplysets(source_folder, target_folder, folder_with_spider_dbs, spider_table_path)