from atomic import *
from to_extract.species import *
from to_extract.species.utils import *


class Sole_join(Speciec):
    gene = Genus.SOLE_JOIN

    def is_appropriate(self, sample: Sample) -> bool:
        return is_join_appropriate(sample, 1)


class Paired_join(Speciec):
    gene = Genus.PAIRED_JOIN

    def is_appropriate(self, sample: Sample) -> bool:
        return is_join_appropriate(sample, 2)


class Triple_join(Speciec):
    gene = Genus.TRIPLE_JOIN

    def is_appropriate(self, sample: Sample) -> bool:
        return is_join_appropriate(sample, 3)


class Quad_join(Speciec):
    gene = Genus.QUAD_JOIN

    def is_appropriate(self, sample: Sample) -> bool:
        return is_join_appropriate(sample, 4)


class Multiple_join(Speciec):
    gene = Genus.MULTIPLE_JOIN

    def is_appropriate(self, sample: Sample) -> bool:
        amounts = get_join_amounts(sample)
        return max(amounts) > 1


class Very_multiple_join(Speciec):
    gene = Genus.VERY_MULTIPLE_JOIN

    def is_appropriate(self, sample: Sample) -> bool:
        amounts = get_join_amounts(sample)
        return max(amounts) > 4


class Hidden_Aggregation(Speciec):
    gene = Genus.HIDDEN_AGGREGATION

    def is_sum(self, sample: Sample) -> bool:
        sql = sample.sql.lower()
        nl = sample.nl.lower()
        if "sum" in sql and "sum" not in nl:
            return True
        return False

    def is_max(self, sample: Sample) -> bool:
        sql = sample.sql.lower()
        nl = sample.nl.lower()
        if "max" in sql and "max" not in nl:
            return True
        return False

    def is_min(self, sample: Sample) -> bool:
        sql = sample.sql.lower()
        nl = sample.nl.lower()
        if "min" in sql and "min" not in nl:
            return True
        return False

    def is_avg(self, sample: Sample) -> bool:
        sql = sample.sql.lower()
        nl = sample.nl.lower()
        if "avg" in sql and "average" not in nl:
            return True
        return False

    def is_count(self, sample: Sample) -> bool:
        sql = sample.sql.lower()
        nl = sample.nl.lower()
        if "how many" in nl or "how much" in nl:
            return True
        return False

    def is_appropriate(self, sample: Sample) -> bool:
        return self.is_sum(sample) or self.is_max(sample) or self.is_min(sample) or self.is_avg(sample)
