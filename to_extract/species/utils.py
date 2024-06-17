from atomic import *
from to_extract.species import *


def get_join_amounts(sample: Sample) -> List[int]:
    parsed = sample.parsed_sql
    amounts = []
    if parsed["from"] and ("table_units" in parsed["from"].keys()):
        amounts.append(len(parsed["from"]["table_units"]) - 1)    # n tables => (n - 1) joins
    if parsed["intersect"] and parsed["intersect"]["from"] and ("table_units" in parsed["intersect"]["from"].keys()):
        amounts.append(len(parsed["intersect"]["from"]["table_units"]) - 1)
    if parsed["union"] and parsed["union"]["from"] and ("table_units" in parsed["union"]["from"].keys()):
        amounts.append(len(parsed["union"]["from"]["table_units"]) - 1)
    if parsed["except"] and parsed["except"]["from"] and ("table_units" in parsed["except"]["from"].keys()):
        amounts.append(len(parsed["except"]["from"]["table_units"]) - 1)
    return amounts


def has_multi_on(sample) -> bool:
    parsed = sample.parsed_sql
    if parsed["from"]:
        table_units = parsed["from"].get("table_units", [])
        conds = parsed["from"].get("conds", [])
        if len(conds) > 0 and 1 + len(conds) // 2 > len(table_units) - 1:
            return True
    if parsed["intersect"] and parsed["intersect"]["from"]:
        table_units = parsed["intersect"]["from"].get("table_units", [])
        conds = parsed["intersect"]["from"].get("conds", [])
        if len(conds) > 0 and 1 + len(conds) // 2 > len(table_units) - 1:
            return True
    if parsed["union"] and parsed["union"]["from"]:
        table_units = parsed["union"]["from"].get("table_units", [])
        conds = parsed["union"]["from"].get("conds", [])
        if len(conds) > 0 and 1 + len(conds) // 2 > len(table_units) - 1:
            return True
    if parsed["except"] and parsed["except"]["from"]:
        table_units = parsed["except"]["from"].get("table_units", [])
        conds = parsed["except"]["from"].get("conds", [])
        if len(conds) > 0 and 1 + len(conds) // 2 > len(table_units) - 1:
            return True
    return False


def is_join_appropriate(sample: Sample, demand_join_amount: int) -> bool:
    amounts = get_join_amounts(sample)
    return max(amounts) == demand_join_amount and not has_multi_on(sample)
