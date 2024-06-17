import os
import re
from atomic import *
from utils.spider.preprocess.parse_sql_one import *


def extract_source_samples(spider_samples_folder: str, mapper: Mapper) -> Dataset:
    data = Dataset()
    dev_path = os.path.join(spider_samples_folder, "dev.json")
    train_spider_path = os.path.join(spider_samples_folder, "train_spider.json")
    train_other_path = os.path.join(spider_samples_folder, "train_others.json")
    data = add_samples(data, dev_path, Source.SPIDER_DEV, mapper)
    data = add_samples(data, train_spider_path, Source.SPIDER_TRAIN, mapper)
    data = add_samples(data, train_other_path, Source.SPIDER_TRAIN_OTHERS, mapper)
    return data


def add_samples(data: Dataset, sample_path: str, source: Source, mapper: Mapper):
    with open(sample_path, "r") as db_file:
        spider_ds = json.load(db_file)
        for ind, sample_json in enumerate(spider_ds):
            current_sample = Sample()
            current_sample.init_from_source(None, sample_json, source, mapper)
            data.add(current_sample)
    return data


def replace_value(tokens: List[str]) -> List[str]:
    replaced = []
    last = ""
    where_flag = False
    for _token in tokens:
        if _token.lower() == "where":
            where_flag = True
        if _token.lower() == "on":
            where_flag = False
        if last in ["=", ">=", "<=", ">", "<", "\"", "``"] and _token not in ["\"", "``", "''"] and where_flag:
            replaced.append("value")
        elif _token not in ["\"", "``", "''"]:
            replaced.append(_token)
        last = _token
    return replaced


def get_parsed_sql(sql, row, source_tables_path):
    db_id = row["db_id"]
    schemas, db_names, tables = get_schemas_from_json(source_tables_path)
    schema = schemas[db_id]
    table = tables[db_id]
    schema_spider = Schema(schema, table)
    parsed_sql = get_sql(schema_spider, sql)
    return parsed_sql


def process_sql_before_spidering(sql: str)-> str:
    processed_sql = sql.replace("“", "\"")
    processed_sql = processed_sql.replace("‘", "\"")
    processed_sql = processed_sql.replace("’", "\"")
    processed_sql = processed_sql.replace("'", "\"")
    processed_sql = processed_sql.replace("“", "\"")
    processed_sql = processed_sql.replace("”", "\"")
    processed_sql = process_in_quotes(processed_sql)
    processed_sql = processed_sql.replace(";", "")
    return processed_sql


def process_in_quotes(text: str) -> str:
    processed = ""
    counter = 0
    text = text.replace("@", "[$$]")
    for _c in text:
        if _c != "\"":
            processed = f"{processed}{_c}"
            continue
        counter += 1
        if counter % 2 == 1:
            processed = f"{processed}#"
            continue
        processed = f"{processed}@"
    before = re.findall(r"#.*?@", processed)
    after = [f"#{process_value(_v[1:-1])}@" for _v in before]
    for _b, _a in zip(before, after):
        processed = processed.replace(_b, _a)
    processed = processed.replace("#", "\"").replace("@", "\"")
    processed = processed.replace("[$$]", "@")
    return processed


def process_value(value: str) -> str:
    processed = value.lower()
    processed = processed.strip()
    return processed


def postprocess_tokens(tokens: List[str]) -> List[str]:
    processed = []
    counter = 0
    for _token in tokens:
        if _token != "\"":
            processed.append(_token)
            continue
        counter += 1
        if counter % 2 == 1:
            processed.append("``")
            continue
        processed.append("''")
    return processed


if __name__ == '__main__':
    s = 'WHERE T1.Name  = " Afghanistan " AND IsOfficial = " F " AND q = "hello it is me   ";'
    print(s)
    processed = process_in_quotes(s)
    print(processed)
