# External:
import numpy as np


def retrieve_dimension_list(*args, **kwargs):
    dimensions = tuple()
    return dimensions


def retrieve_dimensions_no_time(*args, **kwargs):
    dimensions_data = dict()
    attributes = dict()
    return dimensions_data, attributes


def retrieve_variables(*args, **kwargs):
    return args[1]


def retrieve_variables_no_time(*args, **kwargs):
    return args[1]


def retrieve_container(*args, **kwargs):
    return np.array([])


def grab_indices(*args, **kwargs):
    return np.array([])
