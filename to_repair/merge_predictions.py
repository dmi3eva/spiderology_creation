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


if __name__ == "__main__":
    input_path_1 = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "juierror", "evaluation_with_juierror.json")
    with open(input_path_1, "r") as f:
        source_1 = json.load(f)

    input_path_2 = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "defog", "evaluation_with_defog-sqlcoder.json")
    with open(input_path_2, "r") as f:
        source_2 = json.load(f)
    mapping = {_s["id"]: _s["predictions"]["defog-sqlcoder"] for _s in source_2}

    for sample in source_1:
        sample["predictions"]["defog-sqlcoder"] = mapping[sample["id"]]

    output_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "three.json")
    with open(output_path, "w") as f:
        json.dump(source_1, f)
