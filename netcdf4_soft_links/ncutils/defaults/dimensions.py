# External:
import numpy as np
from collections import OrderedDict


def retrieve_dimension(*args, **kwargs):
    attributes = dict()
    dimension_dataset = np.array([])
    return dimension_dataset, attributes


def dimension_compatibility(*args, **kwargs):
    return False


def check_dimensions_compatibility(*args, **kwargs):
    return False


def find_dimension_type(*args, **kwargs):
    return OrderedDict()
