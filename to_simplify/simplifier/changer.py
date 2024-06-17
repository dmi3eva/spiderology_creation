import csv
import json
import os
from nltk.tokenize import WordPunctTokenizer
import shutil

from atomic import *
from to_simplify.simplifier import *
from to_simplify.simplifier.utils.common import *
from to_simplify.simplifier.utils.changers import *
from to_simplify.settings import *

from utils import *


class Changer:
    gene = Kind.CHANGER

    def __init__(self, markup_path: str, spider_samples_folder: str, source_tables_path: str, name: str, source_tag: Tag, db_folders_path: str, target_folder: str, queries_should_be_equal=False):
        self.source_tables_path = source_tables_path
        self.mapper = Mapper(source_tables_path)
        source_samples = extract_source_samples(spider_samples_folder, self.mapper)
        self.hash_mapping = {
            get_samples_hash(_s.db, _s.nl): _s
            for _s in source_samples.content
        }
        self.id_mapping = {
            _s.id: _s
            for _s in source_samples.content
        }
        with open(markup_path, "r", encoding='utf-8') as r_file:
             reader = csv.DictReader(r_file, delimiter=",")
             self.markup = list(reader)
        self.name = name
        self.main_tag = source_tag
        self.source_db_folder_path = db_folders_path
        self.target_folder = target_folder
        self.tokenizer = WordPunctTokenizer()
        self.queries_should_be_equal = queries_should_be_equal

    def simplify_csv(self):
        dbs = set()
        simple_samples = Dataset()
        source_sample_ids = set()
        source_samples = []
        new_source_samples = []
        for ind, row in enumerate(self.markup):
            simple_sample, _new_source_sample, _source_sample, db_id = self.simplify_row(row, ind)
            dbs.add(db_id)
            parents_id = None
            if _source_sample is not None:
                if _source_sample.id in source_sample_ids:
                    print(f"This is already exists: {_source_sample}")
                    # raise ValueError(f"This is already exists: {_source_sample}")
                source_sample_ids.add(_source_sample.id)
                source_samples.append(_source_sample)
                parents_id = _source_sample.id
            elif _new_source_sample is not None:
                new_source_samples.append(_new_source_sample)
                parents_id = _new_source_sample.id
            else:
                raise ValueError("Something wrong with source")
            simple_sample.parents_id = parents_id
            simple_samples.add(simple_sample)
            try:
                if not self.check(row):
                    raise ValueError(f"Wrong simplification: {simple_sample.nl}")
            except Exception as e:
                print(self.name)
                print(e.__str__())
                print(simple_sample.nl)
                print("-----")
        if len(simple_samples.content) != len(source_samples) + len(new_source_samples):
            raise ValueError("Number of simple samples not equal the number of source samples")
        self.create_all_folders()
        self.write_dbs(dbs)
        self.write_tables(dbs)
        self.write_source_samples(source_samples, new_source_samples)
        self.write_simple_samples(simple_samples)

    def simplify_row(self, row: Dict, id: int) -> Tuple[Sample, Optional[Sample], Optional[Sample], str]:
        db_id = row["db_id"]
        hash = get_samples_hash(db_id, row["question"])
        sample = self.hash_mapping.get(hash, None)
        new_source_sample = None
        simple_sample = self.extract_sample_from_row(row, id)
        if sample is None:
            new_source_sample = self.extract_new_source_sample_from_row(row, id)
        else:
            sample.tags.extend(simple_sample.simplifications_tags)
        return simple_sample, new_source_sample, sample, db_id

    def extract_new_source_sample_from_row(self, row: Dict, id: int) -> Sample:
        nl = row["question"]
        simplifications_tags = [self.main_tag]
        sql = row["query"].lower()
        extracted = self.construct_sample(nl, row, sql, simplifications_tags, False)
        extracted.source = Source.ADDED
        id_str = str(id)
        extracted.id = f"added_for_{self.name}_{id_str.zfill(4)}"
        return extracted

    def extract_sample_from_row(self, row: Dict, id: int) -> Sample:
        nl = row["simple_question"]
        tags = [self.main_tag]
        sql = row.get("simple_query", row["query"]).lower()
        extracted = self.construct_sample(nl, row, sql, tags, True)
        extracted.source = Source.SIMPLIFIED
        id_str = str(id)
        extracted.id = f"{self.name}_{id_str.zfill(4)}"
        return extracted

    def construct_sample(self, nl, row, sql, tags, is_simplification: bool):
        extracted = Sample()
        extracted.db = row["db_id"]
        processed_sql = process_sql_before_spidering(sql)
        extracted.nl = nl
        extracted.sql = processed_sql
        extracted.is_simplification = is_simplification
        additional_tag_value = row.get("additional_tag", None)
        additional_tags = []
        if additional_tag_value is not None and additional_tag_value != "":
            additional_tags = self.extract_additional_tags(str(additional_tag_value))
        if is_simplification:
            extracted.simplifications_tags = tags + additional_tags
        else:
            extracted.tags = tags + additional_tags
        extracted.tokenized_nl = self.tokenizer.tokenize(extracted.nl)
        extracted.tokenized_sql = postprocess_tokens(self.tokenizer.tokenize(extracted.sql))
        extracted.tokenized_sql_valueless = replace_value(extracted.tokenized_sql)
        try:
            extracted.parsed_sql = get_parsed_sql(extracted.sql, row, self.source_tables_path)
        except Exception as e:
            print(e.__str__())
            print(extracted.nl)
        return extracted

    def extract_additional_tags(self, additional_tag_value: str) -> List[Tag]:
        if additional_tag_value is None:
            return []
        current_tag = TAGS_MAPPING[additional_tag_value]
        extracted_tags = [current_tag]
        # db_id = row["db_id"]
        # hash = get_samples_hash(db_id, row["question"])
        # sample = self.hash_mapping.get(hash, None)
        # if sample is not None:
        #     sample.tags.append(current_tag)
        return extracted_tags

    def create_all_folders(self):
        self.create_folder("tables")
        self.create_folder("dbs")
        self.create_folder("samples")
        self.create_folder("sources")

    def create_folder(self, folder):
        folder_path = os.path.join(self.target_folder, folder, self.name)
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

    def write_dbs(self, dbs: Set[str]) -> NoReturn:
        source_folder = self.source_db_folder_path
        target_folder = os.path.join(self.target_folder, "dbs", f"databases_{self.name}")
        for _db in dbs:
            source = os.path.join(source_folder, _db)
            target = os.path.join(target_folder, _db)
            if not os.path.exists(target):
                shutil.copytree(source, target)

    def write_tables(self, dbs: Set[str]) -> NoReturn:
        tables = []
        for _db_id in dbs:
            db_table = self.mapper.map[_db_id]
            tables.append(db_table)
        target_filepath = os.path.join(self.target_folder, "tables", self.name, f"tables_{self.name}.json")
        with open(target_filepath, "w") as out_file:
            json.dump(tables, out_file, ensure_ascii=False)

    def write_source_samples(self, source_samples: List[Optional[Sample]], new_source_samples: List[Optional[Sample]]) -> NoReturn:
        samples = Dataset()
        for _sample in source_samples:
            samples.add(_sample)
        for _new_source_sample in new_source_samples:
            samples.add(_new_source_sample)
        target_filepath = os.path.join(self.target_folder, "sources", self.name, f"testset_{self.name}.json")
        samples.dump(target_filepath)

    def write_simple_samples(self, simple_samples: Dataset) -> NoReturn:
        target_filepath = os.path.join(self.target_folder, "samples", self.name, f"simplyset_{self.name}.json")
        simple_samples.dump(target_filepath)

    def check(self, row: Dict) -> bool:
        if "simple_query" not in row.keys():
            return True
        source_query = row["query"]
        simple_query = row["simple_query"]
        db_path = os.path.join(self.source_db_folder_path, row['db_id'], f"{row['db_id']}.sqlite")
        connector = sqlite3.connect(db_path)
        connector.text_factory = lambda b: b.decode(errors='ignore')
        cursor = connector.cursor()
        cursor.execute(simple_query)
        simple_results = cursor.fetchall()
        if self.queries_should_be_equal:
            cursor.execute(source_query)
            source_results = cursor.fetchall()
            equality = simple_results == source_results
            return equality
        return True

