# External:
import numpy as np
from six import string_types

# Internal
from .defaults import core as ncu_defaults

DEFAULT_MAX_REQUEST = 450.0


def default(mod=None):
    def decorator(f):
        def func_wrapper(*args, **kwargs):
            new_kwargs = {key: kwargs[key] for key in kwargs
                          if key != 'default'}
            if ('default' in kwargs and
               kwargs['default']):
                # Get default:
                return getattr(mod, f.__name__)(*args, **new_kwargs)
            else:
                return f(*args, **new_kwargs)
        return func_wrapper
    return decorator


@default(mod=ncu_defaults)
def check_if_opens(dataset):
    return True


def setncattr(output, att, att_val):
    if isinstance(att_val, string_types):
        output.setncattr_string(att, att_val)
    else:
        output.setncattr(att, att_val)
    return


def getncattr(dataset, att):
    return _toscalar(np.asarray(dataset.getncattr(att)))


def _toscalar(x):
    try:
        return np.asscalar(x)
    except (AttributeError, ValueError):
        return x


def find_time_name_from_list(list_of_names, time_var):
    try:
        return next(v for v in list_of_names
                    if v.lower() == time_var)
    except StopIteration:
        return None
