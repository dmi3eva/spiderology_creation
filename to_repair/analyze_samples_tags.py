import csv
import json
import os
import shutil
from configure import *
from typing import *


def exract_rows_with_tag(tag: str, input_path: str, output_path: str) -> NoReturn:
    with open(input_path, "r") as f:
        source = json.load(f)
    filtered = [_s for _s in source if tag in _s["tags"] or tag in _s["simplifications_tags"]]
    testset = [_s for _s in filtered if not _s["is_simplification"]]
    simplyset = [_s for _s in filtered if _s["is_simplification"]]
    mapping = {_s["id"]: _s for _s in testset}
    comparing = []
    for s_row in simplyset:
        t_row = mapping[s_row["parents_id"]]
        line = {
            "id": s_row["id"],
            "parents_id": s_row["parents_id"],
            "query": s_row["query"],
            "parents_query": t_row["query"],
            "question": s_row["question"],
            "parents_question": t_row["question"],
            "pr_natural-sql-7b": s_row["predictions"]["natural-sql-7b"],
            "pr_parents_natural-sql-7b": t_row["predictions"]["natural-sql-7b"],
            "pr_defog-sqlcoder": s_row["predictions"]["defog-sqlcoder"],
            "pr_parents_defog-sqlcoder": t_row["predictions"]["defog-sqlcoder"],
            "ev_natural-sql-7b": s_row["evaluation"]["natural-sql-7b"],
            "ev_parents_natural-sql-7b": t_row["evaluation"]["natural-sql-7b"],
            "ev_defog-sqlcoder": s_row["evaluation"]["defog-sqlcoder"],
            "ev_parents_defog-sqlcoder": t_row["evaluation"]["defog-sqlcoder"],
        }
        comparing.append(line)
    with (open(output_path, "w", encoding='utf-8', newline='\n') as ds_file):
        fieldnames = list(comparing[-1].keys())
        writer = csv.DictWriter(ds_file, fieldnames=fieldnames)
        writer.writeheader()
        for _s in comparing:
            writer.writerow(_s)


if __name__ == "__main__":
    tags = [
        "AGGREGATION_IN_COLUMN",
        "AGGREGATION_MONO",
        "AND_IN_NL",
        "AND_IN_SQL_BUT_NOT_IN_NL",
        "AND_WITH_OR_IN_NL",
        "ANY_SOME_IN_NL",
        "BINARY",
        "CHANGED_LOGICAL_CONNECTIVES",
        "COLUMN_OVERLAPPING",
        "DATETIME",
        "DEJOIN_MINUS_1",
        "DEJOIN_TOTAL",
        "GROUP_BY",
        "HAVING_IN_SQL",
        "HIDDEN_AVG",
        "HIDDEN_MAX",
        "HIDDEN_MIN",
        "HIDDEN_SUM",
        "HOW_MANY",
        "LIKE_IN_SQL",
        "MATH_MINUS",
        "MATH_PLUS",
        "MULTI_AGGREGATION",
        "NEGATION",
        "NEGATION_WITH_ANY_ALL",
        "NON_STRICT_INEQUALITY",
        "OR_IN_NL",
        "ORDER_BY",
        "ORDER_BY_COUNT",
        "SYNONYMS",
        "VALUES_WITHOUT_COLUMNS",
        "WHERE_MONO"
    ]
    input_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "important", "three_evaluation.json")
    for tag in tags:
        output_path = os.path.join(ROOT_PATH, "resources", "results", "spiderology", "analysis", f"{tag}.csv")
        exract_rows_with_tag(tag, input_path, output_path)
