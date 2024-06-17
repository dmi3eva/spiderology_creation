import os
import json
from configure import *
from copy import deepcopy


def add_ids(prefix: str, filename: str):
    old_filepath = os.path.join(SPIDER_DB_PATH, "source_samples", filename)
    ds_with_ids = []
    with open(old_filepath, "r") as db_file:
        spider_ds = json.load(db_file)
        for ind, sample_json in enumerate(spider_ds):
            changed = deepcopy(sample_json)
            changed["id"] = f"{prefix}{str(ind).zfill(4)}"
            ds_with_ids.append(changed)
    new_filepath = os.path.join(SPIDER_DB_PATH, filename)
    with open(new_filepath, "w") as db_file:
        json.dump(ds_with_ids, db_file)


if __name__ == "__main__":
    add_ids("dev_", "dev.json")
    add_ids("train_", "train_spider.json")
    add_ids("other_", "train_others.json")
