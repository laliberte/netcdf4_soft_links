# External:
import numpy as np
from six import string_types

# Internal
from .defaults import core as ncu_defaults

DEFAULT_MAX_REQUEST = 450.0
DEFAULT_ENCODING = 'utf-8'


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
    att_val = _toscalar(np.asarray(att_val))
    if isinstance(att_val, string_types):
        output.setncattr_string(att, att_val)
    else:
        output.setncattr(att, att_val)
    return


def getncattr(dataset, att):
    att_val = _toscalar(np.asarray(dataset.getncattr(att)))
    try:
        att_val = att_val.decode(DEFAULT_ENCODING)
    except AttributeError:
        pass
    return att_val


def maybe_conv_bytes_to_str_array(x):
    if (hasattr(x, 'dtype') and
       isinstance(x.dtype, np.dtype) and
       x.dtype.kind == 'O' and
       np.min(x.shape) > 0):
        if (hasattr(x.item(0), 'decode') and
           hasattr(x.item(0), 'encode')):
            # Assume it is a string object and make sure it is not
            # a byte string.
            def decode(y):
                return str(y.encode(DEFAULT_ENCODING, 'replace')
                           .decode(DEFAULT_ENCODING))
            x = np.vectorize(decode)(x)
        elif hasattr(x.item(0), 'decode'):
            # Assume it is a string object and make sure it is not
            # a byte string.
            def decode(y):
                return y.decode(DEFAULT_ENCODING)
            x = np.vectorize(decode)(x)
    return x


def maybe_conv_bytes_to_str(x):
    if (hasattr(x, 'decode') and
       hasattr(x, 'encode')):
        try:
            return str(x.encode(DEFAULT_ENCODING, 'replace')
                       .decode(DEFAULT_ENCODING))
        except UnicodeDecodeError:
            return str(x)
    elif hasattr(x, 'decode'):
        try:
            return str(x.decode(DEFAULT_ENCODING))
        except UnicodeDecodeError:
            return str(x)
    else:
        return x


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
