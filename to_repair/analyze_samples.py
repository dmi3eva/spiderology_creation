import csv
import json
import os
import shutil
from configure import *
from typing import *


def exract_rows_with_tag(tag: str, input_path: str, output_path: str, field: str) -> NoReturn:
    with open(input_path, "r") as f:
        source = json.load(f)
    filtered = [_s for _s in source if tag in _s[field]]
    with open(output_path, "w", encoding='utf-8', newline='\n') as ds_file:
        fieldnames = ["id", "parents_id", "db_id", "question", "query", "tags", "is_simplification", "source", 'simplifications_tags', 'source', 'query_toks_no_value', 'query_toks', 'prediction_natural-sql-7b', 'sql', 'question_toks']
        writer = csv.DictWriter(ds_file, fieldnames=fieldnames)
        writer.writeheader()
        for _s in filtered:
            writer.writerow(_s)


if __name__ == "__main__":
    tag = "AND_IN_SQL_BUT_NOT_IN_NL"
    input_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "simplyset_all_natural-sql-7b.json")
    output_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "simplyset_AND_IN_SQL_BUT_NOT_IN_NL.csv")
    exract_rows_with_tag(tag, input_path, output_path, "simplifications_tags")

    input_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "testset_all_natural-sql-7b.json")
    output_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "natural_sql_7b", "testset_AND_IN_SQL_BUT_NOT_IN_NL.csv")
    exract_rows_with_tag(tag, input_path, output_path, "tags")




