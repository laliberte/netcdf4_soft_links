from netcdf4_soft_links import retrieval_manager
from argparse import Namespace
import pytest


def test__get_time_var():
    options = Namespace(time_var='TIME')
    assert retrieval_manager._get_time_var(options) == 'TIME'
