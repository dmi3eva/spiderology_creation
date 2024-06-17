import csv
import json
import os
from configure import *

model_name = "defog-sqlcoder"
predictions_path = os.path.join(ROOT_DATA_PATH, "experiments", "predictions", "spiderology_with_predictions.json")
results_path = os.path.join(ROOT_DATA_PATH, "experiments", "long_questions", f"{model_name}.json")

with open(predictions_path, "r") as f:
    data = json.load(f)

problem_length = []
correct_length = []

for _sample in data:
    if _sample["is_simplification"]:
        continue
    query_length = len(_sample["question"])
    if _sample["evaluation"][model_name]["human"] == 0:
        problem_length.append(query_length)
    else:
        correct_length.append(query_length)

results = {
    "problem": problem_length,
    "correct": correct_length
}

with open(results_path, "w") as f:
    json.dump(results, f)