import csv
import json
import os
from configure import *

model_name = "defog-sqlcoder"
predictions_path = os.path.join(ROOT_PATH, "resources", "datasets", "spider", "dev.json")
results_path = os.path.join(ROOT_DATA_PATH, "experiments", "long_questions", f"dev.json")

with open(predictions_path, "r") as f:
    data = json.load(f)

dev_length = []

for _sample in data:
    dev_length.append(len(_sample["question"]))


with open(results_path, "w") as f:
    json.dump(dev_length, f)