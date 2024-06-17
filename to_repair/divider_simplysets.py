import os
import shutil
from configure import *
from typing import *


def move_files_from_folder(source_path: str, target_path: str) -> NoReturn:
    names = os.listdir(source_path)
    for _name in names:
        if _name == ".DS_Store":
            continue
        target = os.path.join(target_path, _name)
        source = os.path.join(source_path, _name)
        if not os.path.exists(target):
            os.makedirs(target)
        this_files = os.listdir(source)
        for _file in this_files:
            if _file == ".DS_Store":
                continue
            _source_file = os.path.join(source, _file)
            shutil.copy(_source_file, target)


def move_files_from_folder_db(source_path: str, target_path: str) -> NoReturn:
    names = os.listdir(source_path)
    for _name in names:
        if _name == ".DS_Store":
            continue
        root = _name.replace("databases_", "")
        target = os.path.join(target_path, root)
        source = os.path.join(source_path, _name)
        if not os.path.exists(target):
            os.makedirs(target)
        shutil.copytree(source, os.path.join(target, _name))


def move_files_from_folder_markup(source_path: str, target_path: str) -> NoReturn:
    names = os.listdir(source_path)
    for _name in names:
        if _name == ".DS_Store":
            continue
        target = os.path.join(target_path, _name.replace(".csv", ""), f"markup_{_name}")
        source = os.path.join(source_path, _name)
        shutil.copy(source, target)


if __name__ == "__main__":
    source_path = SIMPLY_SET_PATH
    target_path = ROLOGY_PATH

    move_files_from_folder(os.path.join(source_path, "samples"), target_path)
    move_files_from_folder(os.path.join(source_path, "sources"), target_path)
    move_files_from_folder(os.path.join(source_path, "tables"), target_path)
    move_files_from_folder_db(os.path.join(source_path, "dbs"), target_path)
    move_files_from_folder_markup(MARKUP_PATH, target_path)
