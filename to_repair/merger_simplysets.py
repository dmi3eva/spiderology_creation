import json
import os
import shutil
from configure import *
from typing import *


def merge_ready_jsons(source_folder: str, target_path: str, id: str, tags=False) -> NoReturn:
    processed = []
    used = set()
    source_files = [_f for _f in os.listdir(source_folder) if not _f.startswith(".")]
    mapping = dict()
    for _file in source_files:
        source_path = os.path.join(source_folder, _file)
        current_json = [_f for _f in os.listdir(source_path) if not _f.startswith(".")][0]
        full_path = os.path.join(source_path, current_json)
        with open(full_path, "r", encoding="utf-8") as json_f:
            data = json.load(json_f)
            for _row in data:
                if _row[id] in used:
                    if tags:
                        mapping[_row[id]]["tags"].extend( _row["tags"])
                        mapping[_row[id]]["simplifications_tags"].extend(_row["simplifications_tags"])
                        mapping[_row[id]]["tags"] = list(set(mapping[_row[id]]["tags"]))
                        mapping[_row[id]]["simplifications_tags"] = list(set(mapping[_row[id]]["simplifications_tags"]))
                    continue
                else:
                    used.add(_row[id])
                    processed.append(_row)
                    if tags:
                        mapping[_row[id]] = {
                            "tags": _row["tags"],
                            "simplifications_tags": _row["simplifications_tags"]
                        }
    if tags:
        tags_path = f'{target_path.replace(".json", "")}_tags.json'
        with open(tags_path, "w") as json_f:
            json.dump(mapping, json_f)
        for _row in processed:
            _row["tags"] = mapping[_row[id]]["tags"]
            _row["simplifications_tags"] = mapping[_row[id]]["simplifications_tags"]
    with open(target_path, "w") as json_f:
        json.dump(processed, json_f)



def merge_dbs(old_path: str, new_path: str) -> NoReturn:
    folders = [_f for _f in os.listdir(old_path) if not _f.startswith(".")]
    for _folder_with_dbs in folders:
        folder_path = os.path.join(old_path, _folder_with_dbs)
        db_folder_names = [_f for _f in os.listdir(folder_path) if not _f.startswith(".")]
        for _db_folder_name in db_folder_names:
            db_folder = os.path.join(folder_path, _db_folder_name)
            target = os.path.join(new_path, _db_folder_name)
            if not os.path.exists(target):
                shutil.copytree(db_folder, target)  # TO-DO: Raise exception


if __name__ == "__main__":
    ready_path = os.path.join(ROOT_DATA_PATH, "all")
    merge_dbs(SIMPLYSET_DB_PATH, os.path.join(ready_path, "databases"))
    merge_ready_jsons(SIMPLYSET_SAMPLES_PATH, os.path.join(ready_path, "simplyset_all.json"), "id", tags=True)
    merge_ready_jsons(SIMPLYSET_SOURCES_PATH, os.path.join(ready_path, "testset_all.json"), "id", tags=True)
    merge_ready_jsons(SIMPLYSET_TABLES_PATH, os.path.join(ready_path, "tables_all.json"), "db_id")


