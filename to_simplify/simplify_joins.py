import os
import json

from atomic import *
from settings import *
from to_simplify.settings import DB_WITH_PROBLEMS, SAMPLES_WITH_PROBLEMS
from to_simplify.simplifier import *
from to_simplify.simplifier.dejoiner import *
from tqdm import tqdm


def simplify_testset(folder: str, filename: str, files: Dict, dejoiner: Dejoiner, name: str, restriction=200) -> NoReturn:
    simply_folder = os.path.join(files["simply_set_path"], "samples", folder)
    dbs_folder = os.path.join(files["simply_set_path"], "dbs", folder)
    source_folder = os.path.join(files["simply_set_path"], "sources", folder)
    error_folder = os.path.join(files["simply_set_path"], "errors", folder)
    table_folder = os.path.join(files["simply_set_path"], "tables", folder)

    simply_path = os.path.join(simply_folder, filename)
    source_path = os.path.join(source_folder, filename)
    table_path = os.path.join(table_folder, filename)
    error_path = os.path.join(error_folder, filename.replace(".json", ".csv"))

    if os.path.exists(simply_folder):
        shutil.rmtree(simply_folder)
    if os.path.exists(dbs_folder):
        shutil.rmtree(dbs_folder)
    if os.path.exists(source_folder):
        shutil.rmtree(source_folder)
    if os.path.exists(error_folder):
        shutil.rmtree(error_folder)
    if os.path.exists(table_folder):
        shutil.rmtree(table_folder)

    if not os.path.exists(simply_folder):
        os.mkdir(simply_folder)
    if not os.path.exists(dbs_folder):
        os.mkdir(dbs_folder)
    if not os.path.exists(source_folder):
        os.mkdir(source_folder)
    if not os.path.exists(error_folder):
        os.mkdir(error_folder)
    if not os.path.exists(table_folder):
        os.mkdir(table_folder)

    dejoiner.target_scheme_path = table_path
    dejoiner.target_db_path = dbs_folder

    mapper = Mapper(files["source_scheme_path"])
    simply_ds = Dataset()
    source_ds = Dataset()
    input_path = files["test_set_path"]
    errors = []
    current_id = 0
    counter = 0
    with open(input_path, "r", encoding="utf-8") as db_file:
        source_samples = json.load(db_file)
        for ind, sample_json in tqdm(enumerate(source_samples)):
            current_sample = Sample()
            current_sample.init_from_source(sample_json.get("id", None), sample_json, Source.SPIDER_DEV, mapper)
            if current_sample.db in files["db_with_problems"]:
                continue
            if current_sample.id in files["samples_with_problems"]:
                continue
            simplification = None
            if not current_sample.source is Source.SPIDER_DEV and counter > restriction:
                break
            try:
                simplification = dejoiner.simplify_step(current_sample)
            except Exception as e:
                print(f"\"{current_sample.id}\",  # {current_sample.db}")
                print(e.__str__())
                if simplification is not None and os.path.exists(os.path.join(dbs_folder, simplification.changes.db.new)):
                    os.remove(os.path.join(files["simply_set_path"], "db", simplification.changes.db.new))
            if simplification is None:
                continue
            if dejoiner.check(current_sample, simplification):
                counter += 1
                simple_sample = simplification.simple_sample
                simple_sample.id = f"{name}_{str(current_id).zfill(4)}"
                current_id += 1
                simple_sample.parents_id = current_sample.id if current_sample.parents_id is None else current_sample.parents_id
                simple_sample.tags = [dejoiner.tags[3], dejoiner.tags[1]]
                simple_sample.simplifications_tags = []
                current_sample.tags = []  # ?
                if dejoiner.tags[4] is not None:
                    simple_sample.simplifications_tags.append(dejoiner.tags[4])
                    current_sample.tags.append(dejoiner.tags[4])
                if dejoiner.tags[5] is not None:
                    simple_sample.simplifications_tags.append(dejoiner.tags[5])
                    current_sample.tags.append(dejoiner.tags[5])
                current_sample.tags.append(dejoiner.tags[2])
                if dejoiner.tags[0] is not None:
                    current_sample.tags.append(dejoiner.tags[0])
                simply_ds.add(simple_sample)
                source_ds.add(current_sample)
            else:
                this_error = {
                    "id": current_sample.id,
                    "old_query": current_sample.sql,
                    "new_query": simplification.simple_sample.sql
                }
                errors.append(this_error)
                old_path = os.path.join(dbs_folder, simplification.changes.db.new)
                new_path = os.path.join(error_folder, simplification.changes.db.new)
                shutil.move(old_path, new_path)
        simply_ds.dump(simply_path)
        source_ds.dump(source_path)

        with open(error_path, "w", encoding='utf-8') as error_file:
            json.dump(errors, error_file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    start_join_number = 4
    finish_join_number = 1
    # finish_join_number = 4
    for source in range(start_join_number, finish_join_number - 1, -1):
        for target in range(source - 1, -1, -1):
            name = f"from_{source}_to_{target}"
            name_to_id = f"dejoin_from_{source}_to_{target}"
            file_folder = f"joins_{name}"
            file_name = f"{file_folder}.json"
            dejoiner = Dejoiner(files[name], additional_tags=join_tags[name])
            simplify_testset(file_folder, file_name, files[name], dejoiner, name_to_id, restriction=100)
