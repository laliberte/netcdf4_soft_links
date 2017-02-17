# External:
import time
import os
import datetime
import hashlib
import errno
from socket import error as SocketError

import requests
from six.moves.urllib.error import HTTPError, URLError

# External but related:
from ..netcdf4_pydap import http_Dataset, httpserver

# Internal:
from . import safe_handling


class http_netCDF:
    def __init__(self, url,
                 semaphores=dict(),
                 remote_data_node='',
                 timeout=120,
                 cache=None,
                 expire_after=datetime.timedelta(hours=1),
                 session=None,
                 authentication_uri=None,
                 username=None,
                 password=None,
                 use_certificates=False):
        self.url = url
        self.semaphores = semaphores
        self.timeout = timeout
        self.cache = cache
        self.expire_after = expire_after
        self.session = session
        self.authentication_uri = authentication_uri
        self.username = username
        self.password = password
        self.use_certificates = use_certificates

        if remote_data_node in self.semaphores.keys():
            self.semaphore = semaphores[remote_data_node]
            self.handle_safely = True
        else:
            self.semaphore = safe_handling.dummy_semaphore()
            self.handle_safely = False
        return

    def __enter__(self):
        self.semaphore.acquire()
        return self

    def __exit__(self, *_):
        if self.handle_safely:
            # Do not release semaphore right away
            # if data is not local:
            time.sleep(0.01)
        self.semaphore.release()
        return

    def check_if_opens(self, num_trials=5):
        success = False
        # If ftp, assume available:
        if (len(self.url) > 3 and
           self.url[:3] == 'ftp'):
            return True
        for trial in range(num_trials):
            if not success:
                try:
                    with http_Dataset(
                                 self.url,
                                 cache=self.cache,
                                 timeout=self.timeout,
                                 expire_after=self.expire_after,
                                 session=self.session,
                                 authentication_uri=self.authentication_uri,
                                 use_certificates=self.use_certificates,
                                 username=self.username,
                                 password=self.password):
                        pass
                    success = True
                except (requests.exceptions.ReadTimeout,
                        HTTPError):
                    time.sleep(3*(trial + 1))
                    pass
                except URLError as e:
                    if e.message == ('<urlopen error [Errno 110] '
                                     'Connection timed out>'):
                        time.sleep(3*(trial + 1))
                        pass
                    else:
                        raise
                except requests.exceptions.ConnectionError as e:
                    time.sleep(3*(trial + 1))
                    pass
                except httpserver.RemoteEmptyError as e:
                    print(e)
                    break
                except SocketError as e:
                    if e.errno != errno.ECONNRESET:
                        raise
                    time.sleep(3*(trial+1))
                    pass
        return success

    def download(self, var, pointer_var,
                 checksum='', checksum_type='MD5',
                 out_dir='.', version='v1'):
        dest_name = destination_download_files(self.url, out_dir,
                                               var, version, pointer_var)

        if checksum == '':
            if os.path.isfile(dest_name):
                return ('File ' + dest_name + ' found but could NOT '
                        'check checksum of existing file because '
                        'checksum was not a priori available. Not retrieving.')
        else:
            try:
                # Works only if file exists!
                comp_checksum = checksum_for_file(checksum_type, dest_name)
            except Exception:
                comp_checksum = ''
            if comp_checksum == checksum:
                return ('File ' + dest_name + ' found. ' + checksum_type +
                        ' OK! Not retrieving.')

        with http_Dataset(
                     self.url,
                     cache=self.cache,
                     timeout=self.timeout,
                     expire_after=self.expire_after,
                     session=self.session,
                     authentication_uri=self.authentication_uri,
                     username=self.username,
                     password=self.password,
                     use_certificates=self.use_certificates) as dataset:
            size_string = dataset.wget(dest_name, progress=True)

        if checksum == '':
            return (size_string + '\n' + 'Could NOT check checksum of '
                    'retrieved file because checksum was not a '
                    'priori available.')
        else:
            try:
                comp_checksum = checksum_for_file(checksum_type, dest_name)
            except:
                comp_checksum = ''
            if comp_checksum != checksum:
                try:
                    os.remove(dest_name)
                except:
                    pass
                return (size_string + '\n' + 'File ' + dest_name +
                        ' does not have the same ' + checksum_type +
                        ' checksum as published on the ESGF. '
                        'Removing this file...')
            else:
                return (size_string + '\n' + 'Checking ' + checksum_type +
                        ' checksum of retrieved file... ' +
                        checksum_type + ' OK!')


def destination_download_files(url, out_dir, var, version, pointer_var):
    if out_dir.split('/')[-2] == 'tree':
        dest_name = out_dir.replace('tree', '/'.join(pointer_var)) + '/'
    else:
        dest_name = out_dir.replace('tree', '/'.join(pointer_var[:-1])) + '/'
        dest_name = dest_name.replace('var', var)
        dest_name = dest_name.replace('version', version)

    dest_name += url.split('/')[-1]
    return os.path.abspath(os.path.expanduser(os.path.expandvars(dest_name)))


def checksum_for_file(checksum_type, dest_name, block_size=2**20):
    checksum = getattr(hashlib, checksum_type.lower())()
    with open(dest_name, 'rb') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            checksum.update(data)
    return checksum.hexdigest()
