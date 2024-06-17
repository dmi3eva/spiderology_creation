import csv
import json
import os
import shutil
from configure import *
from typing import *


def add_tags(tags_path: str, input_path: str, output_path: str) -> NoReturn:
    with open(tags_path, "r") as f:
        tags = json.load(f)
    with open(input_path, "r") as f:
        data = json.load(f)
    for _row in data:
        _row["tags"] = tags[_row["id"]]["tags"]
        _row["simplifications_tags"] = tags[_row["id"]]["simplifications_tags"]
    with open(output_path, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    tags_path = os.path.join(ROOT_PATH, "to_repair", "tags", "all_tags_all.json")
    input_path = os.path.join(ROOT_PATH, "to_repair", "tags", "eval.json")
    output_path = os.path.join(ROOT_PATH, "to_repair", "tags", "eval_repaired.json")
    add_tags(tags_path, input_path, output_path)


