import json
import os
import shutil
from configure import *
from typing import *


def add_predictions(source_path, prediction_path, column_name, model_name, out_path, reset=False) -> NoReturn:
    with open(source_path, "r") as f:
        source = json.load(f)
    with open(prediction_path, "r") as f:
        predictions = json.load(f)
    for _s,_p in zip(source,predictions):
        if reset:
            _s["predictions"] = {}
        else:
            _s["predictions"] = _s.get("predictions", {})
        _s["predictions"][model_name] = _p[column_name]
    with open(out_path, "w") as f:
        json.dump(source, f)


if __name__ == "__main__":
    results_folder = os.path.join(ROOT_PATH, "to_evaluate", "dev")
    source_path = os.path.join(results_folder, "dev_with_results.json")
    out_path = os.path.join(results_folder, "dev_with_predictions_and_results.json")
    prediction_path = os.path.join(results_folder, "defog/predictions.json")
    model_name = "defog-sqlcoder"

    add_predictions(source_path, prediction_path, "predictions_defog-sqlcoder", model_name, out_path, reset=True)

    source_path = os.path.join(results_folder, "dev_with_predictions_and_results.json")
    out_path = os.path.join(results_folder, "dev_with_predictions_and_results.json")
    prediction_path = os.path.join(results_folder, "natural_sql_7b/predictions.json")
    model_name = "natural-sql-7b"

    add_predictions(source_path, prediction_path, "prediction", model_name, out_path, reset=False)
