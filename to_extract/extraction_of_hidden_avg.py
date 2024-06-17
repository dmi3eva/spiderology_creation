import os
import json

from atomic import *
from configure import *
from to_extract.species import *
from to_extract.species.zoo import *


if __name__ == "__main__":
    species = Hidden_Aggregation()

    sum_ds = Dataset()
    max_ds = Dataset()
    min_ds = Dataset()
    avg_ds = Dataset()
    all_ds = Dataset()
    count_ds = Dataset()

    mapper = Mapper(SPIDER_PROCESSED_DB_SCHEMES)

    source = Source.SPIDER_DEV
    input_path = os.path.join(SPIDER_DB_PATH, "dev.json")
    with open(input_path, "r") as db_file:
        spider_ds = json.load(db_file)
        for ind, sample_json in enumerate(spider_ds):
            current_sample = Sample()
            current_sample.init_from_source(f"{source.name}_{str(ind)}", sample_json, source, mapper)
            if species.is_sum(current_sample):
                sum_ds.add(current_sample)
            if species.is_max(current_sample):
                max_ds.add(current_sample)
            if species.is_min(current_sample):
                min_ds.add(current_sample)
            if species.is_avg(current_sample):
                avg_ds.add(current_sample)
            if species.is_appropriate(current_sample):
                all_ds.add(current_sample)
            if species.is_count(current_sample):
                count_ds.add(current_sample)

        source = Source.SPIDER_TRAIN
        input_path = os.path.join(SPIDER_DB_PATH, "train_spider.json")
        with open(input_path, "r") as db_file:
            spider_ds = json.load(db_file)
            for ind, sample_json in enumerate(spider_ds):
                current_sample = Sample()
                current_sample.init_from_source(f"{source.name}_{str(ind)}", sample_json, source, mapper)
                if species.is_sum(current_sample):
                    sum_ds.add(current_sample)
                if species.is_max(current_sample):
                    max_ds.add(current_sample)
                if species.is_min(current_sample):
                    min_ds.add(current_sample)
                if species.is_avg(current_sample):
                    avg_ds.add(current_sample)
                if species.is_appropriate(current_sample):
                    all_ds.add(current_sample)
                if species.is_count(current_sample):
                    count_ds.add(current_sample)

        source = Source.SPIDER_TRAIN_OTHERS
        input_path = os.path.join(SPIDER_DB_PATH, "train_others.json")
        with open(input_path, "r") as db_file:
            spider_ds = json.load(db_file)
            for ind, sample_json in enumerate(spider_ds):
                current_sample = Sample()
                current_sample.init_from_source(f"{source.name}_{str(ind)}", sample_json, source, mapper)
                if species.is_sum(current_sample):
                    sum_ds.add(current_sample)
                if species.is_max(current_sample):
                    max_ds.add(current_sample)
                if species.is_min(current_sample):
                    min_ds.add(current_sample)
                if species.is_avg(current_sample):
                    avg_ds.add(current_sample)
                if species.is_appropriate(current_sample):
                    all_ds.add(current_sample)
                if species.is_count(current_sample):
                    count_ds.add(current_sample)

        output_path_sum = os.path.join(TEST_SET_PATH, "hidden_aggs", "hidden_sum.csv")
        output_path_max = os.path.join(TEST_SET_PATH, "hidden_aggs", "hidden_max.csv")
        output_path_min = os.path.join(TEST_SET_PATH, "hidden_aggs", "hidden_min.csv")
        output_path_avg = os.path.join(TEST_SET_PATH, "hidden_aggs", "hidden_avg.csv")
        output_path_all = os.path.join(TEST_SET_PATH, "hidden_aggs", "hidden_all.csv")
        output_path_count = os.path.join(TEST_SET_PATH, "hidden_aggs", "how_many.csv")

        sum_ds.dump_sources_csv(output_path_sum)
        max_ds.dump_sources_csv(output_path_max)
        min_ds.dump_sources_csv(output_path_min)
        avg_ds.dump_sources_csv(output_path_avg)
        all_ds.dump_sources_csv(output_path_all)
        count_ds.dump_sources_csv(output_path_count)

