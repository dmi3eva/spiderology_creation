import os
import json
import sqlite3
from configure import *
from copy import deepcopy
from random import randint, choices
from typing import *
from tqdm import tqdm

MAX_VALUES_NUMBER = 100


def create_folder(folder: str) -> NoReturn:
    if not os.path.exists(folder):
        os.mkdir(folder)


def save_script(source_sqlite: str, target_script: str, old_scheme: Dict) -> Dict:
    try:
        connector = sqlite3.connect(source_sqlite)
    except Exception as e:
        print(source_sqlite)
        print(target_script)
        print(e.__str__())
    connector.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connector.cursor()
    db = convert_sqlite_to_dict(cursor)
    new_scheme = make_scheme(old_scheme)
    script = construct_script(db)
    with open(target_script, "w", encoding="utf-8") as script_file:
        script_file.write(script)
    cursor.close()
    connector.close()
    return new_scheme


def make_scheme(old_scheme: Dict) -> Dict:
    new_column_names_original = [[-1, "*"]]
    new_column_types = ["text"]
    new_scheme = deepcopy(old_scheme)
    for _column in old_scheme["column_names_original"][1:]:
        processed = process_column(_column[1])
        new_column_tuple = [_column[0], processed]
        new_column_names_original.append(new_column_tuple)
    new_table_names_original = []
    for _table in old_scheme["table_names_original"]:
        processed = process_table(_table)
        new_table_names_original.append(processed)
    new_scheme["column_names_original"] = new_column_names_original
    new_scheme["table_names_original"] = new_table_names_original
    return new_scheme


def construct_script(db: Dict[str, Dict[Tuple[str, str], List]]) -> str:
    text = ""
    for table, table_content in db.items():
        text += construct_script_for_table(table, table_content)
    text = f"{text}\nCOMMIT;\n"
    return text


def construct_script_for_table(table: str, table_content: Dict[Tuple[str, str], List]):
    text = construct_create_part(table, table_content)
    text = extract_insert_part(table, table_content, text)
    return text


def extract_insert_part(table: str, table_content: Dict[Tuple, List], text: str) -> str:
    INSERT = "INSERT INTO {table} VALUES ({values});"
    insertions = []
    for line in zip(*table_content.values()):
        processed_values = [str(_v) if not isinstance(_v, str) else f"\"{_v}\"" for _v in line]
        current = INSERT.format(table=table, values=", ".join(processed_values))
        insertions.append(current)
    insertions_descriptions = "\n".join(insertions)
    text = f"{text}\n\n{insertions_descriptions}\n\n"
    return text


def construct_create_part(table: str, table_content: Dict[Tuple, List]) -> str:
    text = f'CREATE TABLE \"{table}\" (\n'
    columns = [f"\"{_n}\" {_t}" for _n, _t in table_content.keys()]
    column_description = ',\n'.join(columns)
    text = f"{text}{column_description}\n);"
    return text


def convert_sqlite_to_dict(cursor: sqlite3.Cursor) -> Dict:
    db = {}
    source_tables = get_tables(cursor)
    for _table in source_tables:
        table_processed = process_table(_table)
        db[table_processed] = {}
        columns = get_columns(cursor, _table)
        for _column in columns:
            column_processed = (process_column(_column[0]), _column[1])
            values = get_values(cursor, _table, _column)
            db[table_processed][column_processed] = None if values is None else [process_value(_v) for _v in values]
        sizes = [len(_v) for _v in db[table_processed].values() if _v is not None]
        amount = sizes[0] if len(sizes) > 0 else randint(5, 30)
        for _t, _column in zip(db[table_processed].items(), columns):
            if _t[1] is None:
                db[table_processed][_t[0]] = generate_values(_column[1], amount)
    return db


def process_table(name: str) -> str:
    processed = name.lower()
    while "  " in processed:
        processed = processed.replace("  ", " ")
    processed = processed.replace("sqlite", "something")
    words = processed.split(" ")
    processed = "_".join(words)
    return processed


def process_column(name: str) -> str:
    processed = name.lower()
    while "  " in processed:
        processed = processed.replace("  ", " ")
    words = processed.split(" ")
    processed = "_".join(words)
    return processed


def process_value(value: str) -> str:
    if value is None:
        return "NULL"
    if not isinstance(value, str):
        return value
    processed = value.lower()
    processed = processed.replace("\"", "")
    processed = processed.replace("\'", "")
    processed = processed.replace("\\", "")
    processed = processed.replace("(", "")
    processed = processed.replace(")", "")
    processed = processed.replace(", ", " ")
    processed = processed.replace(",", "")
    processed = processed.strip()
    return processed


def get_tables(cursor: sqlite3.Cursor) -> List[str]:
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    processed = [_t[0] for _t in tables]
    return processed


def get_columns(cursor: sqlite3.Cursor, table: str) -> List[Tuple[str, str]]:
    cursor.execute(f"SELECT * FROM `{table}`;")
    column_names = [_c[0] for _c in cursor.description]
    columns = []
    for _name in column_names:
        query = f"SELECT typeof(`{_name}`) FROM `{table}` LIMIT 1;"
        try:
            cursor.execute(query)
            value_types = cursor.fetchall()
            if len(value_types) > 0:
                current = (_name, value_types[0][0])
            else:
                current = (_name, "text")
            columns.append(current)
        except Exception as e:
            print(query)
            raise ValueError(f"Error: {e.__str__()}")
    return columns


def get_values(cursor: sqlite3.Cursor, table: str, column: Tuple[str, str]) -> Optional[List[str]]:
    cursor.execute(f"SELECT `{column[0]}` FROM `{table}`;")
    values = cursor.fetchall()
    if len(values) == 0:
        return None
    if len(values) > MAX_VALUES_NUMBER:
        values = values[:MAX_VALUES_NUMBER]
    processed = [_v[0] for _v in values]
    return processed


def generate_values(column_type: str, amount: int) -> List[Union[int, str]]:
    if column_type in ["integer", "int", "number"]:
        return choices(range(1, 50), k=amount)
    return choices("spiderology", k=amount)


def create_db(new_script_path: str, new_sqlite_path: str) -> NoReturn:
    if os.path.exists(new_sqlite_path):
        os.remove(new_sqlite_path)
    with open(new_script_path, 'r', encoding="utf-8") as script_file:
        script = script_file.read()
    commands = script.split(";\n")
    commands = [_c.strip() for _c in commands if len(_c.strip()) > 1]
    connector = sqlite3.connect(new_sqlite_path)
    connector.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connector.cursor()
    for _command in commands:
        try:
            cursor.execute(_command)
        except Exception as e:
            print(new_sqlite_path)
            print(_command)
            raise ValueError(f"Error: {e.__str__()}\nIn {new_script_path}")
    connector.commit()
    cursor.close()
    connector.close()


def process_db(name: str, source_sqlite: str, target_folder: str, old_scheme: Dict) -> Dict:
    folder = os.path.join(target_folder, name)
    db_path = os.path.join(folder, f"{name}.sqlite")
    target_script = os.path.join(folder, f"{name}.sql")
    create_folder(folder)
    scheme = save_script(source_sqlite, target_script, old_scheme)
    create_db(target_script, db_path)
    return scheme


def process_all_dbs(source_folder: str, target_folder: str, old_scheme_path: str, new_scheme_path: str) -> NoReturn:
    names = os.listdir(source_folder)
    if not os.path.exists(target_folder):
        os.mkdir(target_folder)
    if ".DS_Store" in names:
        names.remove(".DS_Store")
    with open(old_scheme_path, "r", encoding="utf-8") as scheme_file:
        old_schemes = json.load(scheme_file)
    mapping = {_d["db_id"]: _d for _d in old_schemes}
    for _name in tqdm(names):
        source_sqlite = os.path.join(source_folder, _name, f"{_name}.sqlite")
        target_sqlite = os.path.join(target_folder, _name, f"{_name}.sqlite")
        old_scheme = mapping[_name]
        new_scheme = process_db(_name, source_sqlite, target_folder, old_scheme)
        mapping[_name] = new_scheme
    new_schemes = list(mapping.values())
    with open(new_scheme_path, "w", encoding="utf-8") as scheme_file:
        json.dump(new_schemes, scheme_file, indent=4)


if __name__ == "__main__":
    process_all_dbs(SPIDER_DB_FOLDERS, SPIDER_PROCESSED_DB_FOLDERS, SPIDER_DB_SCHEMES, SPIDER_PROCESSED_DB_SCHEMES)
