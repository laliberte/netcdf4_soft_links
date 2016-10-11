#External:
import os
import shutil

import ftplib
import copy
import numpy as np

unique_file_id_list=['checksum_type','checksum','tracking_id']

def download_secure_FTP(url_name,dest_name,username=None,user_pass=None):
    if (username!=None and 
        user_pass!=None):
        #Use credentials:
        ftp=ftplib.FTP(url_name.split('/')[2],username,user_pass)

    else:
        #Do not use credentials and hope for anonymous:
        ftp=ftplib.FTP(url_name.split('/')[2])

    directory=os.path.dirname(dest_name)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(dest_name,'wb') as local_file:
        try:
            ftp.retrbinary('RETR %s' % '/'+'/'.join(url_name.split('/')[3:]), local_file.write)
        except ftplib.error_perm:
            #Permission error. Try again!
            ftp.retrbinary('RETR %s' % '/'+'/'.join(url_name.split('/')[3:]), local_file.write)
    
    ftp.close()
    file_size=np.float(os.stat(dest_name).st_size)
    return "Downloading: %s MB: %s" % (dest_name, file_size/2.0**20)

def download_secure_local(url_name,dest_name):
    directory=os.path.dirname(dest_name)
    if not os.path.exists(directory):
        os.makedirs(directory)
    shutil.copy(url_name,dest_name)
    file_size=np.float(os.stat(dest_name).st_size)
    return "Downloading: %s MB: %s" % (dest_name, file_size/2.0**20)


