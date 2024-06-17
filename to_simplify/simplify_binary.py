from configure import *
from to_simplify.simplifier.changer import *


if __name__ == "__main__":
    markup_path = os.path.join(MARKUP_PATH, "binary.csv")
    spider_samples_folder = SPIDER_DB_PATH
    source_tables_path = SPIDER_PROCESSED_DB_SCHEMES
    db_folders_path = SPIDER_PROCESSED_DB_FOLDERS
    target_folder = SIMPLY_SET_PATH
    name = "binary"
    changer = Changer(markup_path, spider_samples_folder, source_tables_path, name, Tag.BINARY, Tag.DEBINARY, db_folders_path, target_folder, queries_should_be_equal=False)
    changer.simplify_csv()
