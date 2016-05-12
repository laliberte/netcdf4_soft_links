#External:
import netCDF4
import time
import os

#Internal:
import safe_handling

class http_netCDF:
    def __init__(self,url_name,semaphores=dict(),remote_data_node=''):
        self.url_name=url_name
        self.semaphores=semaphores
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
            time.sleep(0.1)
        self.semaphore.release()
        return

    def check_if_opens(self,num_trials=5):
        #If ftp, assume available:
        if len(url_name)>3 and url_name[:3]=='ftp':
            return True
        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.url_name)

        #if pydap.lib.CACHE:
        #    sessions_kwargs={'cache':pydap.lib.CACHE+'/esgf_cache','expire_after'=datetime.timedelta(hours=1)}
        #else:
        #    sessions_kwargs={'backend'='memory'}

        redirection=opendap_netcdf.suppress_stdout_stderr()
        success=False
        for trial in range(num_trials):
            if not success:
                #try:
                #Capture errors. Important to prevent curl errors from being printed:
                with redirection:
                    with requests.Session() as r:
                        with warnings.catch_warnings():
                            warnings.filterwarnings('ignore', message='Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html')
                            headers = {'connection': 'close'}
                            response = r.get(url, cert=(os.environ['X509_USER_PROXY'],os.environ['X509_USER_PROXY']),verify=False,headers=headers)
                if response.status_code == requests.codes.ok and response.headers['Content-Length'][0]:
                    success=True
                #except Exception as e:
                #    time.sleep(3*(trial+1))
                #    pass
        redirection.close()
        #if not success:
        #    print(e)
        return success

    def check_if_opens_wget(self,num_trials=5):
        #If ftp, assume available:
        if len(url_name)>3 and url_name[:3]=='ftp':
            return True

        error_statement=' '.join('''
The url {0} could not be opened. 
Copy and paste this url in a browser and try downloading the file.
If it works, you can stop the download and retry using cdb_query. If
it still does not work it is likely that your certificates are either
not available or out of date.'''.splitlines()).format(self.url_name)

        redirection=opendap_netcdf.suppress_stdout_stderr()
        success=False
        for trial in range(num_trials):
            if not success:
                #try:
                #Capture errors. Important to prevent curl errors from being printed:
                with redirection:
                    wget_call='wget --spider --ca-directory={0} --certificate={1} --private-key={1}'.format(os.environ['X509_CERT_DIR'],os.environ['X509_USER_PROXY']).split(' ')
                    wget_call.append(self.url_name)

                    proc=subprocess.Popen(wget_call,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    (out, err) = proc.communicate()

                    status_string='HTTP request sent, awaiting response... '
                    error_codes=[ int(line.replace(status_string,'').split(' ')[0]) for line in err.splitlines() if status_string in line]
                    length_string='Length: '
                    lengths=[ int(line.replace(length_string,'').split(' ')[0]) for line in err.splitlines() if length_string in line]
                   
                    if 200 in error_codes and max(lengths)>0:
                        success=True
                #except Exception as e:
                #    time.sleep(3*(trial+1))
                #    pass
        redirection.close()
        #if not success:
        #    print(e)
        return success
