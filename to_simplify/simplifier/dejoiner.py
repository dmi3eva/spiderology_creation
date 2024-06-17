from to_simplify.simplifier import *
from to_simplify.simplifier.utils.dejoiner import *

# NLTK configuration
import nltk
try:
    nltk.data.find("tokenizers/punkt/{english}.pickle")
except LookupError as e:
    nltk.download('punkt')


class Dejoiner(Simplifier):
    gene = Kind.DEJOINER

    def __init__(self, files: Dict[str, str], additional_tags: Optional[List[Tag]] = None):
        self.source_scheme_path = files.get("source_scheme_path", SPIDER_PROCESSED_DB_SCHEMES)
        self.source_db_folders = files.get("source_db_folder", SPIDER_PROCESSED_DB_FOLDERS)
        self.source_db_path = files.get("source_db_path", SPIDER_PROCESSED_DB_PATH)
        self.target_scheme_path = files.get("target_scheme_path", SIMPLYSET_DB_SCHEMES)
        self.target_db_path = files.get("target_db_path", SIMPLYSET_DB_PATH)
        self.tags = additional_tags
        self.current_id = 0

    def simplify(self, sample: Sample) -> Simplification:
        pass

    def simplify_step(self, sample: Sample) -> Optional[Simplification]:
        changes = self.__get_changes(sample, self.source_db_folders, self.source_db_path, self.target_db_path)  # Choosing tables and columns in them to merge
        if changes is None:
            return None
        changes = self.__create_new_db(changes, self.source_scheme_path, self.target_scheme_path)  # Creating new DB
        simplifications = self.__get_simplifications(sample, changes, self.target_scheme_path, tags=self.tags)  # Changing sample
        return simplifications

    def check(self, source_sample: Sample, simplification: Simplification) -> bool:
        try:
            simple_sample = simplification.simple_sample
            source_db = os.path.join(simplification.changes.folder.old, f"{simplification.changes.db.old}.sqlite")
            simple_db = os.path.join(simplification.changes.folder.new, f"{simplification.changes.db.new}.sqlite")
            source_query = source_sample.sql
            simple_query = simple_sample.sql
            source_connector = sqlite3.connect(source_db)
            simple_connector = sqlite3.connect(simple_db)
            source_connector.text_factory = lambda b: b.decode(errors='ignore')
            simple_connector.text_factory = lambda b: b.decode(errors='ignore')
            source_cursor = source_connector.cursor()
            simple_cursor = simple_connector.cursor()
            source_cursor.execute(source_query)
            simple_cursor.execute(simple_query)
            source_result = source_cursor.fetchall()
            simple_result = simple_cursor.fetchall()
            source_result = process_result(source_result)
            simple_result = process_result(simple_result)
            source_cursor.close()
            simple_cursor.close()
            source_connector.close()
            simple_connector.close()
            equality = source_result == simple_result
            return equality
        except Exception as e:
            print(f"Problem in checking {source_sample.id}. SQL: {simple_sample.sql}. Error: {e.__str__()}")
            return False

    def __get_changes(self, sample: Sample, db_folders: str, source_db_path: str, target_db_path: str) -> Optional[Changes]:
        changes = Changes()
        changes = choose_tables_to_merge(sample, changes)
        if changes is None:
            return None
        changes = get_db_paths(sample, changes, db_folders, source_db_path, target_db_path)
        changes = insert_new_aliases(sample, changes)
        return changes

    def __create_new_db(self, changes: Changes, source_scheme_path: str, target_scheme_path: str) -> Changes:
        copy_source_db(changes)
        changes, sql_filepath, merged, others = change_sql_script(changes)
        delete_previous_db(changes)
        create_new_db(changes, merged, others)
        changes = change_scheme(changes, source_scheme_path, target_scheme_path)
        return changes

    def __get_simplifications(self, sample: Sample, changes: Changes, target_scheme_path: str, tags: Optional[List[Tag]]=None) -> Simplification:
        # new_id = changes.db.new.replace(f"{changes.db.old}_", "")
        # simple_sample = Sample(id=new_id)  # DB is creating for every simplification
        simple_sample = Sample()
        simple_sample.db = changes.db.new
        simple_sample.scheme = changes.scheme.new
        simple_sample.nl = sample.nl
        simple_sample.tokenized_nl = sample.tokenized_nl
        simple_sample.source = sample.source
        simple_sample.mapper = {changes.db.new: changes.scheme.new}
        simple_sample = enrich_by_sql(sample, changes, simple_sample, target_scheme_path)
        simple_sample.source = Source.SIMPLIFIED
        simple_sample.is_simplification = True
        return Simplification(simple_sample=simple_sample, changes=changes)
