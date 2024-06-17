from abc import ABCMeta, abstractmethod
from atomic import *


class Speciec():
    __metaclass__ = ABCMeta
    gene: Genus

    @abstractmethod
    def is_appropriate(self, sample: Sample) -> bool:
        return NotImplemented
