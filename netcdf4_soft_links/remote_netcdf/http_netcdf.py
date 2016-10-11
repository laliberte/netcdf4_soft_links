#External:
import time
import os
import errno
import datetime
import hashlib

from socket import error as SocketError
from requests.exceptions import ReadTimeout
from netcdf4_pydap.requests_HTTPServer import Dataset, RemoteEmptyError

#Internal:
from . import safe_handling

class http_netCDF:
    def __init__(self,url,semaphores=dict(),
                               remote_data_node='',
                               timeout=120,
                               cache=None,
                               expire_after=datetime.timedelta(hours=1),
                               session=None,
                               authentication_url=None,
                               username=None,
                               password=None,
                               use_certificates=False):
        self.url=url
        self.semaphores=semaphores
        self.timeout=timeout
        self.cache=cache
        self.expire_after=expire_after
        self.session=session
        self.authentication_url=authentication_url
        self.username=username
        self.password=password
        self.use_certificates=use_certificates

        if (remote_data_node in  self.semaphores.keys()):
            self.semaphore=semaphores[remote_data_node]
            self.handle_safely=True
        else:
            self.semaphore=safe_handling.dummy_semaphore()
            self.handle_safely=False
        return

    def __enter__(self):
        self.semaphore.acquire()
        return self

    def __exit__(self,type,value,traceback):
        if self.handle_safely:
            #Do not release semaphore right away if data is not local:
            time.sleep(0.01)
        self.semaphore.release()
        return

    def check_if_opens(self,num_trials=5):
        #If ftp, assume available:
        if len(self.url)>3 and self.url[:3]=='ftp':
            return True
        success=False
        for trial in range(num_trials):
            if not success:
                try:
                    with Dataset(self.url,
                                 cache=self.cache,
                                 timeout=self.timeout,
                                 expire_after=self.expire_after,
                                 session=self.session,
                                 authentication_url=self.authentication_url,
                                 username=self.username,
                                 password=self.password,
                                 use_certificates=self.use_certificates) as dataset:
                        pass
                    success=True
                except SocketError as e:
                    #http://stackoverflow.com/questions/20568216/python-handling-socket-error-errno-104-connection-reset-by-peer
                    if e.errno != errno.ECONNRESET:
                        raise
                    time.sleep(3*(trial+1))
                    pass
                except ReadTimeout as e:
                    time.sleep(3*(trial+1))
                    pass
                except RemoteEmptyError as e:
                    print(e)
                    break
        return success

    def download(self,var,pointer_var,checksum='',checksum_type='MD5',out_dir='.',version='v1'):
        dest_name=destination_download_files(self.url,out_dir,var,version,pointer_var)

        if checksum=='':
            if os.path.isfile(dest_name):
                return 'File '+dest_name+' found but could NOT check checksum of existing file because checksum was not a priori available. Not retrieving.'
        else:
            try: #Works only if file exists!
                comp_checksum=checksum_for_file(checksum_type,dest_name)
            except:
                comp_checksum=''
            if comp_checksum==checksum:
                return 'File '+dest_name+' found. '+checksum_type+' OK! Not retrieving.'

        with Dataset(self.url,
                     cache=self.cache,
                     timeout=self.timeout,
                     expire_after=self.expire_after,
                     session=self.session,
                     authentication_url=self.authentication_url,
                     username=self.username,
                     password=self.password,
                     use_certificates=self.use_certificates) as dataset:
           
            size_string=dataset.wget(dest_name,progress=True)

        if checksum=='':
            return size_string+'\n'+'Could NOT check checksum of retrieved file because checksum was not a priori available.'
        else:
            try: 
                comp_checksum=checksum_for_file(checksum_type,dest_name)
            except:
                comp_checksum=''
            if comp_checksum!=checksum:
                try:
                    os.remove(dest_name)
                except:
                    pass
                return size_string+'\n'+'File '+dest_name+' does not have the same '+checksum_type+' checksum as published on the ESGF. Removing this file...'
            else:
                return size_string+'\n'+'Checking '+checksum_type+' checksum of retrieved file... '+checksum_type+' OK!'

def destination_download_files(url,out_dir,var,version,pointer_var):
    if out_dir.split('/')[-2]=='tree':
        dest_name=out_dir.replace('tree','/'.join(pointer_var))+'/'
    else:
        dest_name=out_dir.replace('tree','/'.join(pointer_var[:-1]))+'/'
        dest_name=dest_name.replace('var',var)
        dest_name=dest_name.replace('version',version)

    dest_name+=url.split('/')[-1]
    return os.path.abspath(os.path.expanduser(os.path.expandvars(dest_name)))

def md5_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()

def sha_for_file(f, block_size=2**20):
    sha = hashlib.sha256()
    while True:
        data = f.read(block_size)
        if not data:
            break
        sha.update(data)
    return sha.hexdigest()

def checksum_for_file(checksum_type,dest_name, block_size=2**20):
    checksum = getattr(hashlib,checksum_type.lower())()
    with open(dest_name,'rb') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            checksum.update(data)
    return checksum.hexdigest()
