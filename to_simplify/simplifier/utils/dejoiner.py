import os
import re
import shutil
import sqlite3
from copy import deepcopy
from typing import *
from atomic import *
from configure import *
from dataclasses import dataclass, field
from to_simplify.simplifier import *
from to_simplify.simplifier.utils.common import *
from utils.spider.preprocess.parse_sql_one import *

REG_EXP_NUMBER = re.compile(r'\d+(\.\d+){0,1}')


@dataclass
class SQL_column:
    name: Optional[str] = None
    type: Optional[str] = None
    primary: bool = False


@dataclass
class SQL_foreign:
    source_column: Optional[str] = None
    reference_column: Optional[str] = None
    reference_table: Optional[str] = None


@dataclass
class Incut:
    start: Optional[int] = None  # Index of the first token
    end: Optional[int] = None  # Index of the last token
    content: Optional[List[str]] = None


@dataclass
class SQL_token:
    position: int
    old_position: Optional[int]
    value: str


@dataclass
class Join_unit:
    current: str
    linked: Optional[str]


@dataclass
class Joinon:
    aliases: Optional[List[str]]
    content: Optional[List[str]]


#################################
# Get changes
#################################


def choose_tables_to_merge(sample: Sample, changes: Changes) -> Optional[Changes]:
    from_to_merge = get_best_from(sample.parsed_sql)  # JOIN can be at except, union or intersect parts
    if not from_to_merge:
        return None
    changes.table_left = sample.get_table(from_to_merge["table_units"][0][1])
    changes.table_right = sample.get_table(from_to_merge["table_units"][1][1])
    changes.column_left = sample.get_column(from_to_merge["conds"][0][2][1][1])
    changes.column_right = sample.get_column(from_to_merge["conds"][0][3][1])
    if changes.table_left.id == changes.table_right.id:
        return None
    changes = check_order_of_joined_columns(changes, sample)
    return changes


def check_order_of_joined_columns(changes: Changes, sample: Sample) -> Changes:
    """
    Sometimes order of columns are reversed (see DB "medicine_enzyme_interaction")
    """
    column_left_in_scheme = sample.scheme["column_names_original"][changes.column_left.id]
    if column_left_in_scheme[0] != changes.table_left.id:
        changes.column_left, changes.column_right = changes.column_right, changes.column_left
    return changes


def get_best_from(parsed: Dict) -> Optional[Dict]:
    max_tables = len(parsed["from"]["table_units"])
    best_from = parsed["from"]
    if parsed["intersect"] and parsed["intersect"]["from"] and len(
            parsed["intersect"]["from"]["table_units"]) > max_tables:
        best_from = parsed["intersect"]["from"]
        max_tables = len(parsed["intersect"]["from"]["table_units"])
    if parsed["union"] and parsed["union"]["from"] and len(parsed["union"]["from"]["table_units"]) > max_tables:
        best_from = parsed["union"]["from"]
        max_tables = len(parsed["union"]["from"]["table_units"])
    if parsed["except"] and parsed["except"]["from"] and len(parsed["except"]["from"]["table_units"]) > max_tables:
        best_from = parsed["except"]["from"]
        max_tables = len(parsed["except"]["from"]["table_units"])
    if len(best_from["table_units"]) >= 2:
        return best_from
    else:
        return None


def get_db_paths(sample: Sample, changes: Changes, folders_path: str, source_db_paths: str, target_db_paths: str) -> Changes:
    if sample.id is not None:
        filename_root = f"{sample.db}_{sample.id}_"
        all_source_folders = os.listdir(folders_path)
        all_simplyset_folders = os.listdir(target_db_paths)
        db_source_folders = [_f for _f in all_source_folders if sample.db in _f]
        db_simplysets_folders = [int(_f.replace(filename_root, "")) for _f in all_simplyset_folders if filename_root in _f]
        if len(db_source_folders) == 0:
            raise ValueError("There is no DB-file in source folder.")
        old_db = db_source_folders[0]
        old_db_path = os.path.join(folders_path, old_db)
        ind = 0
        if len(db_simplysets_folders) > 0:  # If we have already changed the DB, then source modified one, not the original
            ind = max(db_simplysets_folders)
            old_db = f"{filename_root}{ind}"
            old_db_path = os.path.join(source_db_paths, old_db)
        new_db = f"{filename_root}{ind+1}"
    else:
        parts = sample.db.split("_")
        new_ind = int(parts[-1]) + 1
        new_db_almost = "_".join(parts[:-1])
        new_db = f"{new_db_almost}_{str(new_ind)}"
        old_db = sample.db
        old_db_path = os.path.join(source_db_paths, old_db)

    new_db_path = os.path.join(target_db_paths, new_db)
    changes.folder = Shift(old=old_db_path, new=new_db_path)
    changes.db = Shift(old=old_db, new=new_db)
    return changes


def insert_new_aliases(sample: Sample, changes: Changes) -> Changes:
    new_table_name = get_new_alias_for_merged(changes.table_left.name, changes.table_right.name)
    changes.merged_table = Spider_DB_entity(id=changes.table_left.id, name=new_table_name, nature=Nature.TABLE)
    column = changes.column_left
    changes.merged_column = Spider_DB_entity(id=column.id, name=column.name, nature=Nature.COLUMN)
    return changes


def get_new_alias_for_merged(lef_alias: str, right_alias: str) -> str:
    new_table_name = f"{lef_alias}_{right_alias}"
    return new_table_name


#################################
# Create new DB
#################################


def copy_source_db(changes: Changes) -> NoReturn:
    shutil.copytree(changes.folder.old, changes.folder.new)
    old_db_path = os.path.join(changes.folder.new, f"{changes.db.old}.sqlite")
    new_db_path = os.path.join(changes.folder.new, f"{changes.db.new}.sqlite")
    shutil.move(old_db_path, new_db_path)


def change_sql_script(changes: Changes) -> Tuple[Changes, str, str, List[str]]:
    old_sql_filepath, new_sql_filepath = get_sql_filepaths(changes)
    new_values = get_new_values_for_merged_table(changes)
    create_tables = get_create_tables(old_sql_filepath, changes)
    left, right, others = divide_create_table(create_tables, changes)
    others = process_others(others)
    changes, merged = construct_new_create_table(left, right, new_values, changes)
    delete_old_sql_script(old_sql_filepath)
    add_new_sql_script(new_sql_filepath, others, merged)
    return changes, new_sql_filepath, merged, others


def get_sql_filepaths(changes: Changes) -> Tuple[str, str]:
    all_files = os.listdir(changes.folder.new)
    sql = [_f for _f in all_files if _f.endswith(".sql")]
    if len(sql) == 0:
        raise ValueError(f"No SQL-script in DB-folder ({changes.db.old})")
    old_name = sql[0]
    new_name = f"{changes.db.new}.sql"
    return os.path.join(changes.folder.new, old_name), os.path.join(changes.folder.new, new_name)


# -- Get values


def get_new_values_for_merged_table(changes: Changes) -> List[Tuple]:
    # Connecting
    db_path = os.path.join(changes.folder.new, f"{changes.db.new}.sqlite")
    connector = sqlite3.connect(db_path)
    connector.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connector.cursor()
    left_columns = get_column_names(db_path, changes.table_left.name)
    right_columns = get_column_names(db_path, changes.table_right.name)
    left_ids = [f"left.`{_c}`" for _c in left_columns]
    right_ids = [f"right.`{_c}`" for _c in right_columns if _c != changes.column_right.name]
    column_description_first = f"{', '.join(left_ids)}, {', '.join(right_ids)}"
    column_description_second = column_description_first.replace(f"left.`{changes.column_left.name}`", f"right.`{changes.column_right.name}` as {changes.column_left.name}")
    sql = f"""SELECT {column_description_first}        
        FROM {changes.table_left.name} as left
        LEFT JOIN {changes.table_right.name} as right
        ON left.{changes.column_left.name} = right.{changes.column_right.name}
        UNION ALL
        SELECT {column_description_second}
        FROM {changes.table_right.name} as right
        LEFT JOIN {changes.table_left.name} as left
        ON left.{changes.column_left.name} = right.{changes.column_right.name}
        WHERE left.{changes.column_left.name} IS NULL
    """
    try:
        cursor.execute(sql)
        res = cursor.fetchall()
        return res
    except Exception as e:
        raise ValueError(f"Something wrong with JOIN-request:\n{sql}\nDB: {changes.db.old}\nError: {e.__str__()}\nChanges:{changes.merged_table.name}")
    finally:
        cursor.close()
        connector.close()


def get_column_names(db_path, table_name) -> List[str]:
    connector = sqlite3.connect(db_path)
    connector.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connector.cursor()
    sql = f"PRAGMA table_info({table_name});"
    try:
        cursor.execute(sql)
        res = cursor.fetchall()
        columns = [process_column(_t[1]) for _t in res]
        return columns
    except:
        raise ValueError("Something wrong with GET-COLUMN-request")
    finally:
        cursor.close()
        connector.close()


def process_column(column_name: str):
    processed = column_name.lower()
    processed = processed.replace("eg agree objectives", "eg_agree_objectives")
    return processed

# -- Get create tables


def get_create_tables(sql_filepath: str, changes: Changes) -> List[str]:
    with open(sql_filepath, "r", encoding="utf-8") as file_obj:
        text = file_obj.read()
    text = preprocess_instructions(text)
    text, insertions = extract_insertions(text)
    parts = text.split("create ")
    parts = [f"create {_p.strip()}" if _p.strip().startswith("table") else _p.strip() for _p in parts]
    if len(insertions) == 0:
        creates = [_p for _p in parts if _p.startswith("create ")]
        insertions = extract_insertions_from_db(creates, changes)
    parts = integrate_insertions(parts, insertions)
    return parts


def extract_insertions_from_db(creates: List[str], changes: Changes) -> List[str]:
    db_path = os.path.join(changes.folder.new, f"{changes.db.new}.sqlite")
    connector = sqlite3.connect(db_path)
    connector.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connector.cursor()
    insertions = []
    for _create in creates:
        new_insertions = extract_insertions_from_one_db(_create, changes, cursor)
        if new_insertions is not None:
            processed_insertions = [process_value_from_db(changes.db.old, _v) for _v in new_insertions]
            insertions += processed_insertions
    cursor.close()
    connector.close()
    return insertions


def process_value_from_db(db: str, value: str) -> str:
    processed = value.lower()
    processed = processed.replace("\'", "")
    processed = processed.replace("long_beach,_california", "long_beach_california")
    processed = processed.replace("las_vegas,_nevada", "las_vegas_nevada")
    processed = processed.replace("nelson_piquet,_jr", "nelson_piquet_jr")
    processed = processed.replace("kurt_ahrens,_jr", "kurt_ahrens_jr")
    processed = processed.replace("congo, the democratic", "congo the democratic")
    processed = processed.replace("virgin islands, u.s.", "virgin islands u.s.")
    if db == "sakila_1":
        processed = re.sub(r"railers,[a-zA-Z]", "railers ", processed)
        processed = re.sub(r"eleted scenes,[a-zA-Z]", "eleted scenes ", processed)
        processed = re.sub(r"ommentaries,[a-zA-Z]", "ommentaries ", processed)
        processed = processed.replace(" railers", " trailers")
        processed = processed.replace(" eleted scenes", " deleted scenes")
        processed = processed.replace(" ommentaries", " commentaries")
    return processed


def extract_insertions_from_one_db(create: str, changes: Changes, cursor) -> Optional[List[str]]:
    insertions = []
    table_name = extract_table_from_create(create)
    if table_name in [changes.table_left.name, changes.table_right.name]:
        return None
    sql_columns = extract_sql_columns(create)
    column_names = [_c.name for _c in sql_columns]
    column_description = ", ".join(column_names)
    sql = f"""SELECT {column_description}        
    FROM {table_name}
    """
    try:
        cursor.execute(sql)
        res = cursor.fetchall()
    except Exception as e:
        raise ValueError(f"Something wrong with JOIN-request:\n{sql}\nError:\n{e.__str__()}")
    for _values in res:
        line = f"insert into {table_name} values {str(_values)};"
        insertions.append(line)
    return insertions


def extract_insertions(text: str) -> Tuple[str, List[str]]:
    commands = text.split(";\n")
    other = ""
    insertions = []
    for _command in commands:
        if "insert" in _command:
            insertions.append(f"{_command.strip()};")
        elif len(_command.strip().replace("\n", "")) > 0:
            other += f"{_command.strip()};"
    return other, insertions


def extract_table_from_create(description: str) -> str:
    create = description.replace("\n", " ")
    if "create table" not in create.lower():
        return ""
    name = create.split(" ")[2].strip()
    if name.endswith("("):
        name = name[:-1]
    if name.startswith("\'"):
        name = name[1:]
    if name.startswith("\""):
        name = name[1:]
    if name.endswith("\'"):
        name = name[:-1]
    if name.endswith("\""):
        name = name[:-1]
    return name


def integrate_insertions(parts, insertions) -> List[str]:
    map_table_to_inserts = {}
    for _insert in insertions:
        name = _insert.split(" ")[2]
        if name in map_table_to_inserts.keys():
            map_table_to_inserts[name].append(_insert)
        else:
            map_table_to_inserts[name] = [_insert]
    processed_parts = []
    for i, _part in enumerate(parts):
        new_part = _part
        if "create" in _part:
            words = _part.split(" ")
            assert len(words) > 2, f"Parts: {parts}. Insertions: insertions: {insertions}"
            name = words[2]
            insertions_to_insert = map_table_to_inserts.get(name, [])
            insert_part = "\n".join(insertions_to_insert)
            new_part = f"{_part}\n{insert_part}"
        processed_parts.append(new_part)
    return processed_parts


def preprocess_instructions(text: str) -> str:
    processed = text.lower()
    processed = processed.replace("`", "")
    processed = processed.replace("“", "\"")

    processed = processed.replace("let\'\'s", "lets")
    processed = processed.replace("lamb's", "lambs")
    processed = processed.replace("\"magdalen", "magdalen")
    processed = processed.replace("\"distrito federal", "distrito federal")
    processed = processed.replace("stark's", "starks")
    processed = processed.replace("queen's", "queens")
    processed = processed.replace("eg agree objectives", "eg_agree_objectives")  # for DB "tracking_grants_for_research"
    processed = processed.replace("%_change_2007", "percent_change_2007")
    processed = processed.replace("\'\"\'", "\'\'")

    # To avoid the problem with commas in values
    # processed = process_in_quotes(processed)
    processed = process_quotes_in_quotes(processed)
    processed = glue_ripped_lines(processed)
    processed = process_commas_in_quotes(processed)

    processed = processed.replace("\"", "")
    processed = processed.replace("\t", " ")
    processed = processed.replace("if not exists", "")
    while "  " in processed:
        processed = processed.replace("  ", " ")
    while "\n\n" in processed:
        processed = processed.replace("\n\n", "\n")
    processed = processed.replace(") ;", ");")
    processed = processed.replace("; \n", ";\n")
    processed = processed.replace("\n,", ",\n")
    processed = processed.replace("\n ", "\n")
    processed = processed.replace("ON DELETE NO ACTION ON UPDATE NO ACTION,\n".lower(), "")
    processed = processed.replace("ON DELETE NO ACTION ON UPDATE NO ACTION\n".lower(), "")
    processed = processed.replace("),foreign key", "),\nforeign key")
    processed = processed.replace("commit;", "")
    processed = re.sub(r"\/\*(.|\n)*?\*\/", "", processed)
    processed = re.sub(r"-- .*?\n", "\n", processed)
    processed = processed.replace("--\n", "")
    processed = re.sub(r"create index .*?\n", "", processed)
    processed = re.sub(r"create unique index .*?\n", "", processed)
    processed = re.sub(r"drop table .*?\n", "", processed)
    processed = re.sub(r"constraint .*?\n", "", processed)
    processed = processed.replace(",\n)", "\n)")
    # processed = re.sub(r"(,){0,1}\n.*?primary key.*?\n", "", processed)
    # processed = re.sub(r"(,){0,1}\n.*?foreign key.*?\n", "", processed)


    processed = re.sub(r"(,\n){0,1}unique \(.*?\)(,){0,1}", "", processed)
    processed = re.sub(r"(,\n){0,1}unique\(.*?\)(,){0,1}", "", processed)
    processed = processed.replace(" unique", "")
    processed = re.sub(r"uniq (.*?)", "", processed)
    processed = re.sub(r"uniq(.*?)", "", processed)

    processed = processed.replace("\n;", "")
    processed = processed.replace("\r", "")

    while "\n\n" in processed:  # new
        processed = processed.replace("\n\n", "\n")  # new
    # processed = glue_ripped_lines(processed)
    processed = provide_unify_insertions(processed)
    return processed


def process_commas_in_quotes(processed: str) -> str:
    in_quotes = re.findall(r"[\'\"].*?[\'\"]", processed)
    in_quotes_corrected = [_q.replace(",", "") if _q.strip() not in ["\",\"", "\',\'"] else _q for _q in in_quotes]
    for _before, _after in zip(in_quotes, in_quotes_corrected):
        processed = processed.replace(_before, _after)
    return processed


def process_quotes_in_quotes(processed: str) -> str:
    in_quotes = re.findall(r"[\"].*?[\"]", processed)
    in_quotes_corrected = [_q.replace("\'", "") if _q.strip() not in ["\",\"", "\',\'"] else _q for _q in in_quotes]
    for _before, _after in zip(in_quotes, in_quotes_corrected):
        processed = processed.replace(_before, _after)
    return processed


def glue_ripped_lines(text: str) -> str:
    lines = text.split("\n")
    processed_lines = []
    i = 0
    while i < len(lines):
        row = lines[i]
        insert_if = row.strip().startswith("insert into") and row.count("(") > row.count(")")
        other_if = row.count("\"") % 2 == 1 or row.count("\'") % 2 == 1
        if (insert_if or other_if) and i + 1 < len(lines):
            processed_lines.append(f"{row} {lines[i + 1]}")
            i += 1
        else:
            processed_lines.append(row)
        i += 1
    return "\n".join(processed_lines)


def provide_unify_insertions(processed: str) -> str:
    lines = processed.split("\n")
    processed_lines = []
    last_insert_title = ""
    for _line in lines:
        # To avoid the case when insertions are making by list
        if _line.strip() == "":
            continue
        if re.match(r"insert into .*? values(\n|$)", _line.strip()):
            last_insert_title = _line.strip()
            continue
        augmented_line = _line
        if re.match(r"\(.*?\)(\,|\;){0,1}(\n|$)", augmented_line.strip()):
            augmented_line = f"{last_insert_title} {augmented_line.strip()}"
            if augmented_line.endswith(","):
                augmented_line = augmented_line[:-1]
            if not augmented_line.endswith(";"):
                augmented_line = f"{augmented_line};"
        processed_lines.append(augmented_line.strip())
    processed = "\n".join(processed_lines)
    return processed


# -- Divide create tables


def divide_create_table(create_tables: List[str], changes: Changes) -> Tuple[str, str, List[str]]:
    """
    Divide SQL-script into 3 parts: art, relating to left, right and remain part. Later we will merge left and right
    part, while remain will be left unchanged.
    """
    left_name = changes.table_left.name
    right_name = changes.table_right.name
    others = []
    left = None
    right = None
    for description in create_tables:
        table_name = extract_table_from_create(description)
        if left_name == table_name:
            assert left is None
            left = description
        elif right_name == table_name:
            assert right is None
            right = description
        elif not description.startswith("/*") and not description.lower().startswith("pragma") and len(description) > 0:
            others.append(description)
    assert left and right, f"Don't find left or right. DB: {changes.db.old}\n{create_tables}"
    return left, right, others



# -- Construct new create table


def construct_new_create_table(left_sql: str, right_sql: str, new_values: List[Tuple], changes: Changes) -> Tuple[Changes, str]:
    changes, table_part = construct_table_part(left_sql, right_sql, changes)
    value_part = construct_value_part(new_values, changes)
    return changes, f"create table {changes.merged_table.name} (\n{table_part});\n{value_part}\n"


def construct_table_part(left_sql: str, right_sql: str, changes: Changes) -> Tuple[Changes, str]:
    descriptions = []
    changes, sql_lines = construct_sql_lines(left_sql, right_sql, changes)
    descriptions += sql_lines
    descriptions += construct_foreign_lines(left_sql, right_sql, changes)
    return changes, ",\n".join(descriptions)


def construct_sql_lines(left_sql: str, right_sql: str, changes: Changes) -> Tuple[Changes, List]:
    descriptions = []
    left_columns = extract_sql_columns(left_sql)
    right_columns = extract_sql_columns(right_sql)
    mapping = get_merged_column_name_mapping(changes, left_columns, right_columns)
    changes.column_name_mapping = mapping
    for column in left_columns:
        line = f"{mapping[changes.table_left.name][column.name]} {column.type}"
        descriptions.append(line)
    for column in right_columns:
        if column.name == changes.column_right.name:
            continue
        line = f"{mapping[changes.table_right.name][column.name]} {column.type}"
        descriptions.append(line)
    return changes, descriptions


def get_merged_column_name_mapping(changes: Changes, left_columns: List[SQL_column], right_columns: List[SQL_column]) -> Dict:
    left_column_names = [_p.name for _p in left_columns]
    right_column_names = [_p.name for _p in right_columns]
    left_unique = {_c: False if _c in right_column_names else True for _c in left_column_names}
    right_unique = {_c: False if _c in left_column_names else True for _c in right_column_names}
    mapping = {}
    table = changes.table_left.name
    mapping[table] = {}
    for column in left_column_names:
        mapping[table][column] = column
        if not left_unique[column]:
            mapping[table][column] = f"{column}_{table}"
        if column == changes.column_left.name:
            mapping[table][column] = changes.merged_column.name
    table = changes.table_right.name
    mapping[table] = {}
    for column in right_column_names:
        mapping[table][column] = column
        if not right_unique[column]:
            mapping[table][column] = f"{column}_{table}"
        if column == changes.column_right.name:
            mapping[table][column] = changes.merged_column.name
    return mapping


def construct_foreign_lines(left_sql: str, right_sql: str, changes: Changes):
    descriptions = []
    left_foreigns = extract_sql_foreigns(left_sql)
    right_foreigns = extract_sql_foreigns(right_sql)
    all_foreigns = left_foreigns + right_foreigns
    for foreign in all_foreigns:
        if foreign.reference_table in [changes.table_left.name, changes.table_right.name]:
            continue
        if foreign.source_column in [changes.column_left.name, changes.column_right.name]:
            foreign.source_column = changes.merged_column.name
        if foreign.reference_column in [changes.column_left.name, changes.column_right.name]:
            foreign.reference_column = changes.merged_column.name
        line = f"foreign key ({foreign.source_column}) references {foreign.reference_table}({foreign.reference_column})"
        descriptions.append(line)
    return descriptions


def construct_value_part(new_values: List[Tuple], changes: Changes) -> str:
    descriptions = []
    before = "\'"
    after = "\\\""
    for row_tuple in new_values:
        if row_tuple[0] == 340:
            a = 7
        values = deepcopy([str(_v).replace("\'", "") if isinstance(_v, int) or isinstance(_v, float) or _v is None or (_v.startswith("\'") and _v.endswith("\'")) else f"\'{_v.replace(before, after)}\'" for _v in row_tuple])
        row = ', '.join(values)
        line = f"insert into {changes.merged_table.name} values({row})"
        line = line.replace("None", "\'None\'")
        line = line.replace("\'None\'\'", "None\'")
        descriptions.append(line)
    insertions = ";\n".join(descriptions)
    all_insertions = f'{insertions};\n'
    return all_insertions


def extract_sql_columns(instruction: str) -> List[SQL_column]:
    column_description = instruction[instruction.index("(") + 1:].split(");")[0].strip()
    lines = column_description.split("\n")
    columns = []
    primaries = []
    for _line in lines:
        processed_line = _line.strip()
        processed_line = processed_line.replace("primary key(", "primary key (")
        if "foreign key" in processed_line:
            continue
        if processed_line.startswith("primary key"):  # For scripts with "PRAGMA foreign_keys = ON;"
            primaries_line = processed_line.split("primary key (")[1].split(")")[0]
            primaries = [_p.strip() for _p in primaries_line.split(',')]
            primaries += primaries
            continue
        parts = processed_line.split(" ")
        current_column = SQL_column()
        current_column.name = parts[0].strip()
        current_column.type = parts[1].strip().replace(",", "")
        if len(parts) > 2 and "primary" in parts[2]:
            current_column.primary = True
        columns.append(current_column)
    for primary_column_name in primaries:  # For scripts with "PRAGMA foreign_keys = ON;"
        for column in columns:
            if primary_column_name == column.name:
                column.primary = True
    return columns


def extract_sql_foreigns(instruction: str) -> List[SQL_foreign]:
    column_description = instruction[instruction.index("(") + 1:].split(");")[0].strip()
    lines = column_description.split("\n")
    columns = []
    for _line in lines:
        processed = _line.strip()
        processed = processed.replace("foreign key(", "foreign key (")
        if "foreign key" not in processed:
            continue
        current_column = SQL_foreign()
        current_column.source_column = processed.split("foreign key (")[1].split(")")[0]
        current_column.reference_table = processed.split(" references ")[1].split("(")[0]
        current_column.reference_column = processed.split(" references ")[1].split("(")[1][:-1]
        while current_column.reference_column.endswith(")"):
            current_column.reference_column = current_column.reference_column[:-1]
        columns.append(current_column)
    return columns


def process_others(others: List[str]) -> List[str]:
    return [process_one_other(_other) for _other in others]


def process_one_other(other: str) -> str:
    other = other.lower()
    lines = other.split(";\n")
    processed = []
    before = "\'"
    after = "\\\""
    for _line in lines:
        if not _line.strip().startswith("insert"):
            processed.append(_line)
            continue
        line_stripped = _line.strip().replace(");", ")").replace("christ, the bride, the", "christ the bride the")
        line_stripped = line_stripped.replace("values (", "values(")
        bracket_ind = line_stripped.index("values(") + 7  # To avoid the problem with brackets in values
        close_bracket_ind = len(line_stripped) - 1
        while line_stripped[close_bracket_ind] != ")":
            close_bracket_ind -= 1
        values_sub = line_stripped[bracket_ind:close_bracket_ind]
        # To avoid the problem with commas in values
        values_sub = process_commas_in_quotes(values_sub)
        values_sub = process_quotes_in_quotes(values_sub)
        values_sub = values_sub.replace(", ", ",")
        args = values_sub.split(",")
        correct_args = deepcopy([_v if (re.fullmatch(REG_EXP_NUMBER, _v) is not None or (_v.startswith("\'") and _v.endswith("\'"))) else f"\'{_v.replace(before, after)}\'" for _v in args])
        new_values_sub = ", ".join(correct_args)
        corrected_line = f"{line_stripped[:bracket_ind]}{new_values_sub})"
        processed.append(corrected_line)
    all_together = ";\n".join(processed)
    all_together = f"{all_together};\n"
    return all_together


# -- Rewrite SQL script


def delete_old_sql_script(old_sql_script_path) -> NoReturn:
    os.remove(old_sql_script_path)


def add_new_sql_script(sql_filepath: str, others: List[str], merged: str):
    with open(sql_filepath, "w", encoding="utf-8") as file_obj:
        file_obj.write("\n".join(others))
        file_obj.write(merged)


# Change DB


def delete_previous_db(changes: Changes) -> NoReturn:
    db_path = os.path.join(changes.folder.new, f"{changes.db.new}.sqlite")
    os.remove(db_path)


def create_new_db(changes: Changes, merged: str, others: List[str]) -> NoReturn:
    db_path = os.path.join(changes.folder.new, f"{changes.db.new}.sqlite")
    connector = sqlite3.connect(db_path)
    connector.text_factory = lambda b: b.decode(errors='ignore')
    cursor = connector.cursor()
    for script in others + [merged]:
        _command = ""
        try:
            commands = script.split(";\n")
            for _command in commands:
                cursor.execute(_command)
        except Exception as e:
            print(f"{changes.column_left.name=}")
            print(f"{changes.column_right.name=}")
            raise ValueError(f"Something wrong with DB-request: \n {_command} \nDB: {changes.db.old}\nTable: {changes.merged_table.name}\nColumn: {changes.merged_column.name}\nError: {e.__str__()}")
    connector.commit()
    cursor.close()
    connector.close()


# Change the scheme

def change_scheme(changes: Changes, source_scheme_path: str, target_scheme_path: str) -> Changes:
    old_tables = get_old_tables(source_scheme_path)
    changes.scheme.old = get_old_scheme(changes, old_tables)
    changes = create_new_scheme(changes)
    save_new_scheme(changes.scheme.new, target_scheme_path)
    return changes


def get_old_tables(scheme_path: str) -> List:
    with open(scheme_path, "r") as tables_file:
        old_schemes = json.load(tables_file)
    return old_schemes


def get_old_scheme(changes: Changes, old_tables: List) -> Dict:
    old_db_name = changes.db.old
    for _scheme in old_tables:
        if _scheme["db_id"] == old_db_name:
            _scheme["table_names_original"] = [_t.lower() for _t in _scheme["table_names_original"]]
            _scheme["column_names_original"] = [[_c[0], _c[1].lower()] for _c in _scheme["column_names_original"]]
            return _scheme
    raise ValueError(f"There is no scheme for DB = {old_db_name}")


def create_new_scheme(changes: Changes) -> Changes:
    tables, changes = create_tables(changes)
    columns, changes = create_columns(changes)
    primary_keys = create_primary_keys(changes)
    foreign_keys = create_foreign_keys(changes)
    changes.scheme.new = {
        "db_id": changes.db.new,
        "table_names": tables["table_names"],
        "table_names_original": tables["table_names_original"],
        "column_names": columns["column_names"],
        "column_names_original": columns["column_names_original"],
        "column_types": columns["column_types"],
        "primary_keys": primary_keys,
        "foreign_keys": foreign_keys
    }
    return changes


def create_tables(changes: Changes) -> Tuple[Dict, Changes]:
    left_name = changes.table_left.name
    right_name = changes.table_right.name

    table_names_original = [_t.lower() for _t in changes.scheme.old["table_names_original"]]
    left_id = table_names_original.index(left_name)
    right_id = table_names_original.index(right_name)
    new_name = changes.merged_table.name

    # Construct new originals as it was in names
    old_left_original = changes.scheme.old["table_names_original"][left_id]
    old_right_original = changes.scheme.old["table_names_original"][right_id]
    new_original = get_new_alias_for_merged(old_left_original, old_right_original)

    # Removing old tables and adding new
    new_names = []
    new_mapping = {}
    new_index = 0
    for i, _t in enumerate(changes.scheme.old["table_names"]):
        if i in [left_id, right_id]:
            new_mapping[_t] = None
        else:
            new_names.append(_t)
            new_mapping[_t] = new_index
            new_index += 1
    new_names.append(new_name)
    new_mapping[new_name] = new_index
    changes.merged_table.id = new_index
    old_ids = [left_id, right_id]
    new_origins = [_t for i, _t in enumerate(changes.scheme.old["table_names_original"]) if i not in old_ids]
    new_origins.append(new_original)

    # Constructing result
    new_tables = {
        "table_names": new_names,
        "table_names_original": new_origins
    }
    changes.table_name_mapping = new_mapping
    return new_tables, changes


def create_columns(changes: Changes) -> Tuple[Dict, Changes]:
    left_id = changes.column_left.id
    right_id = changes.column_right.id
    merged_table_id = changes.table_name_mapping[changes.merged_table.name]
    table_mapping = changes.table_name_mapping
    column_mapping = dict()

    new_names = [[-1, '*']]
    new_origins = [[-1, '*']]
    new_types = ['text']

    changes.column_name_mapping = complete_column_name_mapping(changes)

    for i, _pair in enumerate(changes.scheme.old["column_names"]):
        if i in [0, left_id, right_id]:
            continue
        old_table_name_original = changes.scheme.old["table_names_original"][_pair[0]]
        old_table_name = changes.scheme.old["table_names"][_pair[0]]
        current_original = changes.scheme.old["column_names_original"][i]
        current_type = changes.scheme.old["column_types"][i]
        new_table_id = table_mapping[old_table_name] if table_mapping[old_table_name] is not None else merged_table_id
        column_mapping[i] = len(new_names)
        new_column_original = changes.column_name_mapping[old_table_name_original][current_original[1]]
        new_column_name = changes.column_name_mapping[old_table_name_original][current_original[1]].replace("_", " ")
        new_names.append([new_table_id, new_column_name])
        new_origins.append([new_table_id, new_column_original])
        new_types.append(current_type)

    # Adding new column
    changes.merged_column.id = len(new_names)
    column_mapping[changes.merged_column.id] = len(new_names)
    new_names.append([merged_table_id, changes.merged_column.name])
    new_origins.append([merged_table_id, changes.merged_column.name.lower()])
    new_types.append(changes.scheme.old["column_types"][left_id])  # Without loss of generality, let's choose left type

    # Constructing result
    new_tables = {
        "column_names": new_names,
        "column_names_original": new_origins,
        "column_types": new_types
    }
    changes.column_id_mapping = column_mapping
    return new_tables, changes


def complete_column_name_mapping(changes: Changes) -> Dict[str, Dict]:
    mapping = changes.column_name_mapping
    existing = mapping.keys()
    mapping.update({_t: {} for _t in changes.scheme.old["table_names_original"] if _t not in existing})
    for i, old_table_name in enumerate(changes.scheme.old["table_names_original"]):
        if i in [changes.table_left.id, changes.table_right.id]:
            continue
        column_names = [_p[1] for _p in changes.scheme.old["column_names_original"] if _p[0] == i]
        mapping[old_table_name] = deepcopy({_c: _c for _c in column_names})
    return mapping


def create_primary_keys(changes: Changes) -> List:
    old_keys = [changes.column_left.id, changes.column_right.id]
    old_scheme = changes.scheme.old
    new_primary_keys = [changes.column_id_mapping[_k] for _k in old_scheme["primary_keys"] if _k not in old_keys]
    if len(new_primary_keys) != len(old_scheme["primary_keys"]):
        new_primary_keys.append(changes.merged_column.id)
        new_primary_keys.sort()
    return new_primary_keys


def create_foreign_keys(changes: Changes) -> List:
    old_keys = [changes.column_left.id, changes.column_right.id]
    old_scheme = changes.scheme.old
    new_foreign_keys = []
    for _pair in old_scheme["foreign_keys"]:
        if _pair[0] in old_keys and _pair[1] in old_keys:
            continue
        if _pair[0] in old_keys:
            new_foreign_keys.append([changes.merged_column.id, changes.column_id_mapping[_pair[1]]])
        elif _pair[1] in old_keys:
            new_foreign_keys.append([changes.column_id_mapping[_pair[0]], changes.merged_column.id])
        else:
            new_foreign_keys.append([changes.column_id_mapping[_pair[0]], changes.column_id_mapping[_pair[1]]])
    return new_foreign_keys


def fix_old_scheme(changes: Changes, old_tables: List, new_scheme: Dict) -> List:
    old_db_name = changes.db.old
    new_tables = [_s for _s in old_tables if _s["db_id"] != old_db_name]
    new_tables.append(new_scheme)
    return new_tables


def save_new_scheme(new_scheme: Dict, target_scheme_path: str) -> NoReturn:
    if not os.path.exists(target_scheme_path):
        new_tables = []
    else:
        with open(target_scheme_path, "r") as tables_file:
            new_tables = json.load(tables_file)
    new_tables.append(new_scheme)
    with open(target_scheme_path, "w") as tables_file:
        json.dump(new_tables, tables_file)


#################################
# Create the sample
#################################


def enrich_by_sql(source_sample: Sample, changes: Changes, simple_sample: Sample, target_scheme_path: str) -> Sample:
    simple_sample.tokenized_sql = glue_tables_in_sql_tokens(source_sample.tokenized_sql, changes)
    simple_sample.tokenized_sql_valueless = glue_tables_in_sql_tokens(source_sample.tokenized_sql_valueless, changes)
    simple_sample.sql = convert_tokens_to_sql(simple_sample.tokenized_sql, source_sample.sql)
    simple_sample.parsed_sql = get_sql_repr_with_spider_tools(changes, simple_sample.sql, target_scheme_path)
    return simple_sample


def convert_tokens_to_sql(tokens: List[str], source_sql: str) -> str:
    sqled = " ".join(tokens)
    sqled = sqled.replace("`` ", "\"")
    sqled = sqled.replace(" ''", "\"")
    sqled = sqled.replace("``", "\"")
    sqled = sqled.replace("''", "\"")
    sqled = sqled.replace("! =", "!=")
    sqled = sqled.replace("< =", "<=")
    sqled = sqled.replace("> =", ">=")
    in_quotes = re.findall(r"[\'\"].*?[\'\"]", sqled)
    in_quotes_corrected = [f"\'{_q[1:-1].strip()}\'".replace(" .", ".") for _q in in_quotes]
    for _before, _after in zip(in_quotes, in_quotes_corrected):
        sqled = sqled.replace(_before, _after)
    return sqled


def get_joinon(tokens: List[str]) -> Joinon:
    aliases = []
    for i, _token in enumerate(tokens):
        if "." in _token:
            aliases.append(_token.split(".")[0])
        elif i > 0 and tokens[i - 1] == "as":
            aliases.append(_token)
    return Joinon(aliases=aliases, content=tokens)


def align_joins_and_ons(joins: List[List[str]], ons: List[List[str]]) -> Tuple[List[Joinon], List[Joinon]]:
    """Greedy alignment"""
    joiners = [get_joinon(_j) for _j in joins]
    oners = [get_joinon(_o) for _o in ons]
    n = len(joiners)
    for i in range(n - 1):
        if joiners[i].aliases[0] not in oners[i].aliases:
            joiners[i], joiners[i + 1] = joiners[i + 1], joiners[i]
    return joiners, oners


def fix_joins_order(incut: Incut) -> Incut:
    content = incut.content
    blocks = {"join": [], "on": []}
    current_block = ["join"]  # First block has not "JOIN"
    for token in content:
        new_token = token if token != "and" else "on"
        if new_token in ["join", "on"]:
            blocks[current_block[0]].append(deepcopy(current_block))
            current_block = []
        current_block.append(new_token)
    if len(current_block) > 0:
        blocks[current_block[0]].append(deepcopy(current_block))
    assert len(blocks["join"]) == len(blocks["on"]) + 1, f"Content: {content}"
    ordered = blocks["join"][0][1:]  # First block has not "JOIN"
    ordered_joins, ordered_ons = align_joins_and_ons(blocks["join"][1:], blocks["on"])
    for _join, _on in zip(ordered_joins, ordered_ons):
        ordered.extend(_join.content)
        ordered.extend(_on.content)
    return Incut(start=incut.start, end=incut.end, content=ordered)


def glue_tables_in_sql_tokens(tokens: List[str], changes: Changes) -> List[str]:
    tokens = sql_preprocess(tokens)
    join_incuts = extract_join_incuts(tokens)  # Entire JOIN-block for one sub-request
    join_incuts = [fix_joins_order(_incut) for _incut in join_incuts]
    alias_table_mappings = [extract_table_alias_mappings(j) for j in join_incuts]
    join_graphs = [construct_join_graph(_j, _a, changes) for _j, _a in zip(join_incuts, alias_table_mappings)]
    sequences = [construct_join_sequence(_j, _a) for _j, _a in zip(join_incuts, alias_table_mappings)]
    union_mapping = construct_uinion_table_alias_mapping(alias_table_mappings, changes)
    fixed_table_alias_mappings = [fix_table_mapping(_m, changes, union_mapping) for _m in alias_table_mappings]
    fixed_sequences = [fix_sequence(_s, changes) for _s in sequences]
    pair = zip(join_graphs, fixed_table_alias_mappings)
    fixed_join_graphs = [fix_join_graphs(_g, changes) for _g, _m, in pair]
    quadro = zip(fixed_join_graphs, fixed_sequences, fixed_table_alias_mappings, join_incuts)
    join_incut_with_fixed_content = [fix_join_incut(_g, _s, _m, _i) for _g, _s, _m, _i in quadro]
    fixed_sql_tokens = fix_tokens_by_incuts(tokens, join_incut_with_fixed_content)
    fixed_join_incuts = extract_join_incuts(fixed_sql_tokens)
    fixed_sql_tokens = replace_other_id_tokens(alias_table_mappings, changes, join_incuts, fixed_join_incuts, fixed_sql_tokens, tokens, fixed_table_alias_mappings)
    return fixed_sql_tokens


def replace_other_id_tokens(old_alias_table_mappings: List[Dict], changes: Changes, old_join_incuts: List[Incut], new_join_incuts: List[Incut], sql_tokens: List[str], old_sql_tokens: List[str], new_table_alias_mappings: List[Dict]) -> List[str]:
    other_sql_tokens = extract_sql_tokens(sql_tokens, old_alias_table_mappings, new_join_incuts, changes)
    other_sql_tokens = add_old_positions(other_sql_tokens, old_sql_tokens, old_join_incuts)

    # For case, where len(other_sql_tokens) != len(fixed_join_incuts)
    augmented_old_alias_table_mappings = []
    augmented_new_table_alias_mappings = []
    incut_ind = 0
    for token_list in other_sql_tokens:
        while len(token_list) > 0 and incut_ind < len(old_join_incuts) - 1 and token_list[0].old_position > old_join_incuts[incut_ind].end:
            incut_ind += 1
        augmented_old_alias_table_mappings.append(old_alias_table_mappings[incut_ind])
        augmented_new_table_alias_mappings.append(new_table_alias_mappings[incut_ind])

    triple = zip(other_sql_tokens, augmented_old_alias_table_mappings, augmented_new_table_alias_mappings)
    fixed_other_sql_tokens = [fix_other_sql_tokens(_i, _a, _t, changes) for _i, _a, _t in triple]  # TO-DO: repair
    flat_sql_tokens = flatten_incuts(fixed_other_sql_tokens)  # TO-DO: implement
    sql_tokens = fix_tokens_by_SQLTokens(sql_tokens, flat_sql_tokens)
    return sql_tokens


def add_old_positions(other_sql_tokens: List[List[SQL_token]], old_sql_tokens: List[str], old_join_incuts: List[Incut]) -> List[List[SQL_token]]:
    ind = 0
    for token_list in other_sql_tokens:
        for new_token in token_list:
            while old_sql_tokens[ind] != new_token.value or from_join_incut(ind, old_join_incuts):
                ind += 1
            new_token.old_position = ind
    return other_sql_tokens


def extract_join_incuts(tokens: List[str]) -> List[Incut]:
    incuts = []
    started = False  # JOIN-block of one sub-request was starting
    new_incut = Incut()

    for i, token in enumerate(tokens):
        if token == "join" and not started:
            started = True
            start_ind = i - 3 if tokens[i - 2] == "as" else i - 1
            new_incut = Incut(start=start_ind)
        totally_last = "join" not in tokens[i + 1:]
        amid = [] if totally_last else tokens[i + 1: i + 1 + tokens[i + 1:].index("join")]
        next_exists = "intersect" in amid or "union" in amid or "having" in amid or "in" in amid or "except" in amid or "(" in amid
        new_subrequest = not totally_last and next_exists
        this_is_the_last_in_the_subrequest = totally_last or new_subrequest
        if token == "join" and started and this_is_the_last_in_the_subrequest:
            started = False
            new_incut.end = i + 1
            content = tokens[new_incut.start: new_incut.end]
            equals_n = sum([1 for _t in content if _t == "="])
            joins_n = sum([1 for _t in content if _t == "join"])
            for j, remain_token in enumerate(tokens[i + 1:]):
                content.append(remain_token)
                if remain_token == "=":
                    equals_n += 1
                if equals_n == joins_n:
                    content.append(tokens[i + 1:][j + 1])
                    new_incut.end += j + 1
                    break
            new_incut.content = content
            incuts.append(new_incut)
    if started:
        new_incut.end = new_incut.start + tokens[new_incut.start:].index("=")
        content = tokens[new_incut.start: new_incut.end]
        equals_n = sum([1 for _t in content if _t == "="])
        joins_n = sum([1 for _t in content if _t == "join"])
        for j, remain_token in enumerate(tokens[new_incut.end:]):
            content.append(remain_token)
            if remain_token == "=":
                equals_n += 1
            if equals_n == joins_n:
                content.append(tokens[new_incut.end:][j + 1])
                new_incut.end += j + 1
                break
        new_incut.content = content
        incuts.append(new_incut)
    return incuts


def extract_select_incuts(tokens: List[str]) -> List[Incut]:
    start_idx = [i + 1 for i, x in enumerate(tokens) if x == "select"]
    end_idx = [i - 1 for i, x in enumerate(tokens) if x == "from"]
    incuts = [Incut(start=s, end=e, content=tokens[s:e+1]) for s, e in zip(start_idx, end_idx)]
    return incuts


def extract_sql_tokens(tokens: List[str], alias_table_mapping: List[Dict], join_incuts: List, changes: Changes) -> List[List[SQL_token]]:
    """
    Every list corresponds to one sub-request and contains list of ID-tokens (alias.table like t1.student)
    """
    extracted = []
    descriptions = extract_token_description(tokens, join_incuts)
    aliases = []
    tables = []
    for _mapping in alias_table_mapping:
        aliases.extend(list(_mapping.keys()))
        tables.extend(list(_mapping.values()))
    for _descriptions in descriptions:
        new_portion = extract_subrequest_sql_tokens(_descriptions, aliases, tables, changes)  # Crutch
        extracted.append(new_portion)
    return extracted


def aliases_in_subrequest(subrequest: List[SQL_token]) -> bool:
    for _token in subrequest:
        if "." in _token.value and len(_token.value.split(".")) == 2:
            return True
    return False


def extract_subrequest_sql_tokens(descriptions: List[SQL_token], aliases: List[str], tables: List[str], changes: Changes) -> List[SQL_token]:
    """
    Filtering list of ID-tokens (alias.table like t1.student) from list of tokens
    """
    incuts = []
    for description in descriptions:
        parts = description.value.split(".")
        is_id = len(parts) == 1 and parts[0] in tables
        is_id = is_id or len(parts) == 1 and parts[0] in [changes.table_left, changes.table_right]
        is_id = is_id or (len(parts) == 2 and parts[0] in aliases + tables)
        if is_id:
            incuts.append(description)
    return incuts


def extract_token_description(tokens: List[str], join_incuts: List[Incut]) -> List[List[SQL_token]]:
    """
    Every list corresponds to one sub-request and contains list of tokens and their positions
    """
    descriptions = []
    current = []
    for i, token in enumerate(tokens):
        if token == "select" and i > 0:
            descriptions.append(deepcopy(current))
            current = []
        if not from_join_incut(i, join_incuts):
            current.append(SQL_token(position=i, value=token, old_position=None))
    descriptions.append(deepcopy(current))
    return descriptions


def from_join_incut(i: int, join_incuts: List[Incut]):
    """
    Checking if the token is in the one of the JOIN-incut
    """
    for incut in join_incuts:
        if incut.start <= i <= incut.end:
            return True
        if i < incut.start:
            return False
    return False


def extract_table_alias_mappings(incut: Incut) -> Dict:
    mapping = dict()
    for i, token in enumerate(incut.content):
        if token == "as":
            mapping[incut.content[i + 1]] = incut.content[i - 1]
    return mapping


def construct_join_graph(incut: Incut, mapping: Dict, changes: Changes) -> Dict:
    """
    Returning graph of joining as: graph[table_1][table_2] = column_from_table_1
    """
    graph = dict()
    for i, token in enumerate(incut.content):
        if token == "on":
            left = incut.content[i + 1]
            right = incut.content[i + 3]
            table_left, column_left = left.split(".")
            if table_left in mapping.keys():
                table_left = mapping[table_left]
            table_right, column_right = right.split(".")
            if table_right in mapping.keys():
                table_right = mapping[table_right]
            if table_left not in graph.keys():
                graph[table_left] = {}
            graph[table_left][table_right] = column_left
            if table_right not in graph.keys():
                graph[table_right] = {}
            graph[table_right][table_left] = column_right
    return graph


def list_split(tokens: List[str], separator: str) -> List[List[str]]:
    splitted = []
    current = []
    for _token in tokens:
        if _token == separator:
            splitted.append(current)
            current = []
        else:
            current.append(_token)
    if len(current) > 0:
        splitted.append(current)
    return splitted


def construct_join_sequence(incut: Incut, mapping: Dict) -> List[Join_unit]:
    sequence = []
    parts = list_split(incut.content, "join")
    for i, _part in enumerate(parts):
        current_table_name = _part[0]
        linked_table_name = None
        if i > 0:
            assert "=" in _part, f"There is no = in {_part}. Incut: {incut.content}"
            equal_idx = _part.index("=")
            left_table_name = extract_table_from_term(_part[equal_idx - 1], mapping)
            right_table_name = extract_table_from_term(_part[equal_idx + 1], mapping)
            all_names = [left_table_name, right_table_name]
            assert current_table_name in all_names, f"There is no {current_table_name} in {all_names}. Incut: {incut}"
            all_names.remove(current_table_name)
            linked_table_name = all_names[0]
        new_unit = Join_unit(current=current_table_name, linked=linked_table_name)
        sequence.append(new_unit)
    return sequence


def extract_table_from_term(term: str, mapping: Dict) -> str:
    if "." not in term:
        raise ValueError()
    parts = term.split(".")
    alias = parts[0]
    return mapping.get(alias, alias)


def construct_uinion_table_alias_mapping(alias_table_mappings: List[dict], changes: Changes) -> Dict:
    union_mapping = {
        changes.merged_table.name: "t1"
    }
    ind = 2
    removed_tables = [changes.table_left.name, changes.table_right.name]
    for mapping in alias_table_mappings:
        for table in mapping.values():
            if table in union_mapping.keys() or table in removed_tables:
                continue
            union_mapping[table] = f"t{ind}"
            ind += 1
    return union_mapping


def fix_table_mapping(alias_table_mapping: Dict[str, str], changes: Changes, union_mapping: Dict) -> Dict[str, str]:
    fixed_mapping = dict()
    removed_tables = [changes.table_left.name, changes.table_right.name]
    for old_alias, old_table in alias_table_mapping.items():
        if old_table not in removed_tables:
            fixed_mapping[old_table] = union_mapping[old_table]
        else:
            fixed_mapping[changes.merged_table.name] = union_mapping[changes.merged_table.name]
    return fixed_mapping


def fix_sequence(sequence: List[Join_unit], changes: Changes) -> List[Join_unit]:
    fixed_sequence = []
    removed_tables = [changes.table_left.name, changes.table_right.name]
    merged_table_mentioned = False
    for _unit in sequence:
        if _unit.current in removed_tables and not merged_table_mentioned and _unit.linked not in removed_tables:
            merged_table_mentioned = True
            new_unit = Join_unit(current=changes.merged_table.name, linked=_unit.linked)
            fixed_sequence.append(new_unit)
        elif _unit.current not in removed_tables:
            new_linked = _unit.linked if _unit.linked not in removed_tables else changes.merged_table.name
            new_unit = Join_unit(current=_unit.current, linked=new_linked)
            fixed_sequence.append(new_unit)
    return fixed_sequence


def fix_join_graphs(graph: Dict[str, Dict], changes: Changes) -> Dict:
    if changes.table_right.name in graph.get(changes.table_left.name, {}).keys():
        _ = graph[changes.table_left.name].pop(changes.table_right.name)
    if changes.table_left.name in graph.get(changes.table_right.name, {}).keys():
        _ = graph[changes.table_right.name].pop(changes.table_left.name)
    merged_tables = [changes.table_left.name, changes.table_right.name]
    merged_columns = [changes.column_left.name, changes.column_right.name]
    fixed_graph = dict()
    mapping = changes.column_name_mapping
    for left_table, connections in graph.items():
        table = changes.merged_table.name if left_table in merged_tables else left_table
        fixed_graph[table] = fixed_graph.get(table, {})
        for right_table, left_column in connections.items():
            if right_table in merged_tables:
                fixed_graph[table][changes.merged_table.name] = mapping[left_table][left_column]
            elif left_table in merged_tables and left_column in merged_columns:
                fixed_graph[table][right_table] = changes.merged_column.name
            else:
                fixed_graph[table][right_table] = mapping[left_table][left_column]
    return fixed_graph


def fix_join_incut(graph: Dict, sequence: List, mapping: Dict, old_incut: Incut) -> Incut:
    content = []
    for i, _unit in enumerate(sequence):
        table_content = []
        alias = mapping.get(_unit.current, "t100")
        table_content += [_unit.current, 'as', alias]
        if _unit.linked is not None:
            previous_table = _unit.linked
            previous_alias = mapping[previous_table]
            previuos_column = graph[previous_table][_unit.current]
            current_alias = mapping[_unit.current]
            current_column = graph[_unit.current][previous_table]
            on_part = ["on", f"{previous_alias}.{previuos_column}", "=", f"{current_alias}.{current_column}"]
            table_content += on_part
        if i < len(sequence) - 1:
            table_content.append('join')
        content += table_content
    fixed_incut = Incut(start=old_incut.start, end=old_incut.end, content=content)
    return fixed_incut


def fix_tokens_by_incuts(old_tokens: List[str], fixed_join_incuts: List[Incut]) -> List[str]:
    last = 0
    fixed_tokens = []
    for incut in fixed_join_incuts:
        fixed_tokens += old_tokens[last:incut.start]
        fixed_tokens += incut.content
        last = incut.end + 1
    if len(fixed_join_incuts) > 0:
        last_x = fixed_join_incuts[-1].end + 1
        fixed_tokens += old_tokens[last_x:]
    return fixed_tokens


def fix_tokens_by_SQLTokens(old_tokens: List[str], fixed_sql_tokens: List[SQL_token]) -> List[str]:
    last = 0
    fixed_tokens = []
    for sql_token in fixed_sql_tokens:
        fixed_tokens += old_tokens[last:sql_token.position]
        fixed_tokens += [sql_token.value]
        last = sql_token.position + 1
    if len(fixed_sql_tokens) > 0:
        last_x = fixed_sql_tokens[-1].position + 1
        fixed_tokens += old_tokens[last_x:]
    return fixed_tokens


def fix_other_sql_tokens(sql_tokens: List[SQL_token], old_alias_table_mappings: Dict[str, str], new_table_alias_mappings: Dict[str, str], changes: Changes) -> List[SQL_token]:
    fixed_content = []
    for token in sql_tokens:
        fixed = None
        if "." in token.value:
            fixed = fix_id_token(token.value, old_alias_table_mappings, new_table_alias_mappings, changes)
        elif token.value in old_alias_table_mappings.keys():
            fixed = fix_token_alias(token.value, old_alias_table_mappings, new_table_alias_mappings, changes)
        elif token.value in old_alias_table_mappings.values():
            fixed = fix_token_table(token.value, new_table_alias_mappings, changes)
        else:
            raise ValueError(f"Some strange token to replace: {token}")
        fixed_sql_token = SQL_token(position=token.position, value=fixed, old_position=None)
        fixed_content.append(fixed_sql_token)
    return fixed_content


def fix_token_alias(token: str, alias_table_mappings: Dict[str, str], table_alias_mappings: Dict[str, str], changes: Changes) -> str:
    old_table_name = alias_table_mappings.get(token, token)
    if old_table_name in [changes.table_left.name, changes.table_right.name]:
        old_table_name = changes.merged_table.name
    new_table_name = table_alias_mappings.get(old_table_name, old_table_name)
    return new_table_name


def fix_token_table(old_table_name: str, table_alias_mappings: Dict[str, str], changes: Changes) -> str:
    if old_table_name in [changes.table_left.name, changes.table_right.name]:
        return changes.merged_table.name
    return old_table_name


def flatten_incuts(incuts: List[List[SQL_token]]) -> List[SQL_token]:
    flatten = []
    for list_of_sql_token in incuts:
        flatten.extend(list_of_sql_token)
    return flatten


def fix_id_token(token: str, alias_table_mappings: Dict, table_alias_mappings: Dict, changes: Changes) -> str:
    fixed_parts = []
    old_table_name = token
    if "." not in token:
        old_column = token
    else:
        old_table_desription, old_column = token.split(".")
        if old_table_desription in alias_table_mappings.keys():
            old_table_alias = old_table_desription
            old_table_name = alias_table_mappings[old_table_alias]
            new_table_alias = fix_token_alias(old_table_alias, alias_table_mappings, table_alias_mappings, changes)
            fixed_parts.append(new_table_alias)
        else:
            old_table_name = old_table_desription
            new_table_alias = fix_token_table(old_table_name, table_alias_mappings, changes)
            fixed_parts.append(new_table_alias)
    new_column = changes.column_name_mapping[old_table_name][old_column]
    fixed_parts.append(new_column)
    fixed_token = ".".join(fixed_parts)
    return fixed_token


# Change the SQL representation


def get_sql_repr_with_spider_tools(changes: Changes, sql: str, target_scheme_path: str) -> Dict:
    db_id = changes.db.new
    schemas, db_names, tables = get_schemas_from_json(target_scheme_path)
    schema = schemas[db_id]
    table = tables[db_id]
    schema_spider = Schema(schema, table)
    sql_label = get_sql(schema_spider, sql)
    return sql_label


def sql_preprocess(tokens: List[str]) -> List[str]:
    preprocessed = [_t.lower() for _t in tokens]
    merged = []
    i = 0
    while i < len(preprocessed):
        if i + 2 < len(preprocessed) and preprocessed[i + 1] == "." and re.match(r"t\d+", preprocessed[i]):
            merged.append(f"{preprocessed[i]}.{preprocessed[i + 2]}")
            i += 3
        else:
            merged.append(preprocessed[i])
            i += 1
    return merged

#################################
# Process result to compare
#################################


def process_result(result: List[Tuple]) -> List[Tuple]:
    processed = []
    for _row in result:
        processed.append(tuple([process_one_value_from_result(_v) for _v in _row if isinstance(_v, str)]))
    return processed


def process_one_value_from_result(one_value: str) -> str:
    processed = one_value.lower()
    processed = processed.replace("\"", "")
    processed = processed.replace("\'", "")
    processed = processed.replace("\\", "")
    processed = processed.strip()
    return processed


