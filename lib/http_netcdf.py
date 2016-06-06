#External:
import netCDF4
import time
import os
from socket import error as SocketError
import errno
import warnings
import requests
import requests_cache
import datetime

#Internal:
import safe_handling
import esgf_http

class http_netCDF:
    def __init__(self,url_name,semaphores=dict(),remote_data_node='',timeout=120,cache=None,expire_after=datetime.timedelta(hours=1),session=None):
        self.url_name=url_name
        self.semaphores=semaphores
        self.timeout=timeout
        self.cache=cache
        self.expire_after=expire_after
        self.session=session

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
        if len(self.url_name)>3 and self.url_name[:3]=='ftp':
            return True
        success=False
        for trial in range(num_trials):
            if not success:
                try:
                    with esgf_http.Dataset(self.url_name,
                                            cache=self.cache,
                                            timeout=self.timeout,
                                            expire_after=self.expire_after,
                                            session=self.session) as dataset:
                        pass
                    success=True
                except SocketError as e:
                    #http://stackoverflow.com/questions/20568216/python-handling-socket-error-errno-104-connection-reset-by-peer
                    if e.errno != errno.ECONNRESET:
                        raise
                    time.sleep(3*(trial+1))
                    pass
                except requests.exceptions.ReadTimeout as e:
                    time.sleep(3*(trial+1))
                    pass
                except esgf_http.RemoteEmptyError as e:
                    print(e)
                    break
        return success

    def check_if_opens_wget(self,num_trials=5):
        #If ftp, assume available:
        if len(url_name)>3 and url_name[:3]=='ftp':
            return True

        success=False
        for trial in range(num_trials):
            if not success:
                wget_call='wget --timeout={2} --spider --ca-directory={0} --certificate={1} --private-key={1}'.format(os.environ['X509_CERT_DIR'],os.environ['X509_USER_PROXY'],self.timeout).split(' ')
                wget_call.append(self.url_name)

                proc=subprocess.Popen(wget_call,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                (out, err) = proc.communicate()

                status_string='HTTP request sent, awaiting response... '
                error_codes=[ int(line.replace(status_string,'').split(' ')[0]) for line in err.splitlines() if status_string in line]
                length_string='Length: '
                lengths=[ int(line.replace(length_string,'').split(' ')[0]) for line in err.splitlines() if length_string in line]
               
                if 200 in error_codes and max(lengths)>0:
                    success=True
            if not success:
                time.sleep(3*(trial+1))
        return success

    def download(self,var,pointer_var,checksum='',checksum_type='MD5',out_dir='.',version='v1'):
        dest_name=destination_download_files(self.url_name,out_dir,var,version,pointer_var)

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

        with esgf_http.Dataset(self.url_name,
                                cache=self.cache,
                                timeout=self.timeout,
                                expire_after=self.expire_after,
                                session=self.session) as dataset:
            try: 
                file_size = int(dataset.response.headers["Content-Length"][0])
            except KeyError:
                file_size = False

            if file_size:
                size_string="Downloading: %s MB: %s" % (dest_name, file_size/2.0**20)
            else:
                size_string="Downloading: %s MB: Unknown" % (dest_name)
            
            directory=os.path.dirname(dest_name)
            if not os.path.exists(directory):
                os.makedirs(directory)
            file_size_dl = 0
            block_sz = 8192
            
            with open(dest_name, 'wb') as dest_file:
                for buffer in dataset.response.iter_content(block_sz):
                    file_size_dl += len(buffer)
                    dest_file.write(buffer)
                    if file_size:
                        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                        status = status + chr(8)*(len(status)+1)

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

def destination_download_files(url_name,out_dir,var,version,pointer_var):
    dest_name=out_dir.replace('tree','/'.join(pointer_var[:-1]))+'/'
    dest_name=dest_name.replace('var',var)
    dest_name=dest_name.replace('version',version)

    dest_name+=url_name.split('/')[-1]
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
    with open(dest_name,'r') as f:
        checksum = getattr(hashlib,checksum_type.lower())()
        while True:
            data = f.read(block_size)
            if not data:
                break
            checksum.update(data)
    return checksum.hexdigest()
