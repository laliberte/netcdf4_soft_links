#External:
import multiprocessing
import datetime
#import sys
#from StringIO import StringIO
import netCDF4
import requests
import requests_cache

#Internal:
from . import netcdf_utils, requests_sessions, retrieval_utils
from .certificates import certificates
from .remote_netcdf import remote_netcdf

def start_download_processes(options,q_manager,previous_processes=dict()):
    remote_netcdf_kwargs=dict()
    if 'download_cache' in dir(options) and options.download_cache:
        remote_netcdf_kwargs['cache']=options.download_cache.split(',')[0]
        if len(options.download_cache.split(','))>1:
            remote_netcdf_kwargs['expire_after']=datetime.timedelta(hours=float(options.download_cache.split(',')[1]))

    #Add credentials:
    remote_netcdf_kwargs.update({opt: getattr(options,opt) for opt in ['openid','username','password','use_certificates'
                                                                     ] if opt in dir(options)})
    #This allows time variables with different names:
    if 'time_var' in dir(options) and options.time_var:
        time_var=options.time_var
    else:
        time_var='time'

    #Start processes for download. Can be run iteratively for an update.
    processes=previous_processes
    if not ('serial' in dir(options) and options.serial):
        processes=start_download_processes_no_serial(q_manager,options.num_dl,processes,time_var=time_var,
                                                                                        remote_netcdf_kwargs=remote_netcdf_kwargs)
    return processes

def start_download_processes_no_serial(q_manager,num_dl,processes,time_var='time',remote_netcdf_kwargs=dict()):
    for data_node in q_manager.queues.keys():
        for simultaneous_proc in range(num_dl):
            process_name=data_node+'-'+str(simultaneous_proc)
            if not process_name in processes.keys():
                processes[process_name]=multiprocessing.Process(target=worker_retrieve, 
                                                name=process_name,
                                                args=(q_manager,data_node,time_var,remote_netcdf_kwargs))
                processes[process_name].start()
    return processes

def worker_retrieve(q_manager,data_node,time_var,remote_netcdf_kwargs):
    if ( 'session' in dir(q_manager) and
        (isinstance(q_manager.session,requests.Session) or
            isinstance(q_manager.session,requests_cache.core.CachedSession)
            )):
        session=q_manager.session
    else:
        #Create one session per worker:
        session=requests_sessions.create_single_session(**remote_netcdf_kwargs)

    #Loop indefinitely. Worker will be terminated by main process.
    while True:
        item = q_manager.queues.get(data_node)
        if item=='STOP': break
        try:
            thread_id=item[0]
            trial=item[1]
            path_to_retrieve=item[2]
            file_type=item[3]
            remote_data=remote_netcdf.remote_netCDF(path_to_retrieve,file_type,session=session,
                                                                               time_var=time_var,
                                                                               **remote_netcdf_kwargs)

            var_to_retrieve=item[4]
            pointer_var=item[5]
            result=remote_data.download(var_to_retrieve,pointer_var,download_kwargs=item[-1])
            q_manager.put_for_thread_id(thread_id,(file_type,result))
        except Exception:
            if trial==3:
                print('Download failed with arguments ',item)
                raise
                q_manager.put_for_thread_id(thread_id,(file_type,'FAIL'))
            else:
                #Put back in the queue. Do not raise. Simply put back in the queue so that failure
                #cannnot occur while working downloads work:
                item_new=(trial+1,path_to_retrieve,file_type, var_to_retrieve,pointer_var,item[-1])
                q_manager.put_again_to_data_node_from_thread_id(thread_id,data_node,item_new)
    return

def worker_exit(q_manager,data_node_list,queues_size,start_time,renewal_time,output,options):
    failed=False
    while True:
        item = q_manager.get_for_thread_id()
        if item=='STOP': break
        renewal_time,failed=progress_report(item[0],item[1],q_manager,data_node_list,queues_size,start_time,renewal_time,failed,output,options)
    return renewal_time, failed

def launch_download(output,data_node_list,q_manager,options):
    remote_netcdf_kwargs=dict()
    if 'download_cache' in dir(options) and options.download_cache:
        remote_netcdf_kwargs['cache']=options.download_cache.split(',')[0]
        if len(options.download_cache.split(','))>1:
            remote_netcdf_kwargs['expire_after']=datetime.timedelta(hours=float(options.download_cache.split(',')[1]))

    start_time = datetime.datetime.now()
    renewal_time = start_time
    queues_size=dict()
    if 'silent' in dir(options) and not options.silent:
        for data_node in data_node_list:
            queues_size[data_node]=q_manager.queues.qsize(data_node)
        string_to_print=['0'.zfill(len(str(queues_size[data_node])))+'/'+str(queues_size[data_node])+' paths from "'+data_node+'"' for
                            data_node in data_node_list if queues_size[data_node]>0 ]
        if len(string_to_print)>0:
            print('Remaining retrieval from data nodes:')
            print(' | '.join(string_to_print))
            print('Progress: ')

    if 'serial' in dir(options) and options.serial:
        for data_node in data_node_list:
            q_manager.queues.put(data_node,'STOP')
            worker_retrieve(q_manager,data_node,remote_netcdf_kwargs)
            renewal_time, failed=worker_exit(q_manager,data_node_list,queues_size,start_time,renewal_time,output,options)
    else:
        renewal_time, failed=worker_exit(q_manager,data_node_list,queues_size,start_time,renewal_time,output,options)

    if failed:
        raise Exception('Retrieval failed')
                
    if ( 'silent' in dir(options) and not options.silent and
         len(string_to_print)>0):
        print
        print('Done!')
    return output

def progress_report(file_type,result,q_manager,data_node_list,queues_size,start_time,renewal_time,failed,output,options):
    elapsed_time = datetime.datetime.now() - start_time
    renewal_elapsed_time=datetime.datetime.now() - renewal_time

    if file_type=='HTTPServer':
        if not result=='FAIL':
            if 'silent' in dir(options) and not options.silent:
                #print '\t', queues['end'].get()
                if result!=None:
                    print('\t' + result)
                    print(str(elapsed_time))
        else:
            failed=True
    else:
        if not result=='FAIL':
            assign_tree(output,*result)
            output.sync()
            if 'silent' in dir(options) and not options.silent:
                string_to_print=[str(queues_size[data_node]-q_manager.queues.qsize(data_node)).zfill(len(str(queues_size[data_node])))+
                                 '/'+str(queues_size[data_node]) for
                                    data_node in data_node_list if queues_size[data_node]>0]
                print(str(elapsed_time)+', '+' | '.join(string_to_print)+'\r'),
        else:
            failed=True

    #Maintain certificates:
    if ('username' in dir(options) and 
        options.username!=None and
        options.password!=None and
        options.use_certificates and
        renewal_elapsed_time > datetime.timedelta(hours=1)):
        #Reactivate certificates:
        certificates.retrieve_certificates(options.username,'ceda',user_pass=options.password)
        renewal_time=datetime.datetime.now()
    return renewal_time, failed

def assign_tree(output,val,sort_table,tree):
    if len(tree)>1:
        if tree[0]!='':
            assign_tree(output.groups[tree[0]],val,sort_table,tree[1:])
        else:
            assign_tree(output,val,sort_table,tree[1:])
    else:
        output.variables[tree[0]][sort_table,...]=val
    return
