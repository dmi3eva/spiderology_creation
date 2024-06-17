from abc import ABCMeta, abstractmethod
from atomic import *
from dataclasses import dataclass, field


@dataclass
class Shift:
    new: Union[str, Dict] = None
    old: Union[str, Dict] = None


@dataclass
class Changes:
    db: Optional[Shift] = None
    folder: Optional[Shift] = None
    table_left: Optional[Spider_DB_entity] = None
    table_right: Optional[Spider_DB_entity] = None
    column_left: Optional[Spider_DB_entity] = None
    column_right: Optional[Spider_DB_entity] = None
    merged_table: Optional[Spider_DB_entity] = None
    merged_column: Optional[Spider_DB_entity] = None
    table_name_mapping: Optional[Dict] = None
    column_id_mapping: Optional[Dict] = None
    column_name_mapping: Optional[Dict[str, Dict]] = None  # column_name_mapping[old_table_name] = {old_column_name: new_column_name}
    scheme: Shift = Shift()


@dataclass
class Simplification:
    simple_sample: Sample
    changes: Changes


class Simplifier():
    __metaclass__ = ABCMeta
    kind: Kind

    @abstractmethod
    def simplify(self, sample: Sample) -> Simplification:
        return NotImplemented

    @abstractmethod
    def simplify_step(self, sample: Sample) -> Optional[Simplification]:
        return NotImplemented

    @abstractmethod
    def check(self, source_sample: Sample, simplification: Simplification) -> bool:
        return NotImplemented
