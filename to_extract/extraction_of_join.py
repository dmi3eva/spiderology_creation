import os
import json

from atomic import *
from configure import *
from to_extract.species import *
from to_extract.species.zoo import *


if __name__ == "__main__":
    species_0 = Multiple_join()
    species_1 = Sole_join()
    species_2 = Paired_join()
    species_3 = Triple_join()
    species_4 = Quad_join()
    species_n = Very_multiple_join()

    many_json_ds = Dataset()
    sole_json_ds = Dataset()
    paired_json_ds = Dataset()
    triple_json_ds = Dataset()
    quad_json_ds = Dataset()
    very_many_json_ds = Dataset()

    mapper = Mapper(SPIDER_DB_SCHEMES)

    source = Source.SPIDER_DEV
    input_path = os.path.join(SPIDER_DB_PATH, "dev.json")
    with open(input_path, "r") as db_file:
        spider_ds = json.load(db_file)
        for ind, sample_json in enumerate(spider_ds):
            current_sample = Sample()
            current_sample.init_from_source(f"{source.name}_{str(ind)}", sample_json, source, mapper)
            if species_0.is_appropriate(current_sample):
                many_json_ds.add(current_sample)
            if species_1.is_appropriate(current_sample):
                sole_json_ds.add(current_sample)
            if species_2.is_appropriate(current_sample):
                paired_json_ds.add(current_sample)
            if species_3.is_appropriate(current_sample):
                triple_json_ds.add(current_sample)
            if species_4.is_appropriate(current_sample):
                quad_json_ds.add(current_sample)
            if species_n.is_appropriate(current_sample):
                very_many_json_ds.add(current_sample)

        source = Source.SPIDER_TRAIN
        input_path = os.path.join(SPIDER_DB_PATH, "train_spider.json")
        with open(input_path, "r") as db_file:
            spider_ds = json.load(db_file)
            for ind, sample_json in enumerate(spider_ds):
                current_sample = Sample()
                current_sample.init_from_source(f"{source.name}_{str(ind)}", sample_json, source, mapper)
                if species_0.is_appropriate(current_sample):
                    many_json_ds.add(current_sample)
                if species_1.is_appropriate(current_sample):
                    sole_json_ds.add(current_sample)
                if species_2.is_appropriate(current_sample):
                    paired_json_ds.add(current_sample)
                if species_3.is_appropriate(current_sample):
                    triple_json_ds.add(current_sample)
                if species_4.is_appropriate(current_sample):
                    quad_json_ds.add(current_sample)
                if species_n.is_appropriate(current_sample):
                    very_many_json_ds.add(current_sample)

        source = Source.SPIDER_TRAIN_OTHERS
        input_path = os.path.join(SPIDER_DB_PATH, "train_others.json")
        with open(input_path, "r") as db_file:
            spider_ds = json.load(db_file)
            for ind, sample_json in enumerate(spider_ds):
                current_sample = Sample()
                current_sample.init_from_source(f"{source.name}_{str(ind)}", sample_json, source, mapper)
                if species_0.is_appropriate(current_sample):
                    many_json_ds.add(current_sample)
                if species_1.is_appropriate(current_sample):
                    sole_json_ds.add(current_sample)
                if species_2.is_appropriate(current_sample):
                    paired_json_ds.add(current_sample)
                if species_3.is_appropriate(current_sample):
                    triple_json_ds.add(current_sample)
                if species_4.is_appropriate(current_sample):
                    quad_json_ds.add(current_sample)
                if species_n.is_appropriate(current_sample):
                    very_many_json_ds.add(current_sample)

        output_path_0 = os.path.join(TEST_SET_PATH, "joins", "joins_many.json")
        output_path_1 = os.path.join(TEST_SET_PATH, "joins", "joins_01.json")
        output_path_2 = os.path.join(TEST_SET_PATH, "joins", "joins_02.json")
        output_path_3 = os.path.join(TEST_SET_PATH, "joins", "joins_03.json")
        output_path_4 = os.path.join(TEST_SET_PATH, "joins", "joins_04.json")
        output_path_n = os.path.join(TEST_SET_PATH, "joins", "joins_very_many.json")

        many_json_ds.dump_sources_json(output_path_0)
        sole_json_ds.dump_sources_json(output_path_1)
        paired_json_ds.dump_sources_json(output_path_2)
        triple_json_ds.dump_sources_json(output_path_3)
        quad_json_ds.dump_sources_json(output_path_4)
        very_many_json_ds.dump_sources_json(output_path_n)
