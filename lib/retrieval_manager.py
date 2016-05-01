#External:
import multiprocessing
import datetime
#import sys
#from StringIO import StringIO
import netCDF4

#Internal:
import netcdf_utils
import remote_netcdf
import certificates
import retrieval_utils

def start_download_processes(options,queues_manager,previous_processes=dict()):
    #Start processes for download. Can be run iteratively for an update.
    processes=previous_processes
    if not ('serial' in dir(options) and options.serial):
        processes=start_download_processes_no_serial(queues_manager,options.num_dl,processes)
    return processes

def start_download_processes_no_serial(queues_manager,num_dl,processes):
    for data_node in queues_manager.queues.keys():
        for simultaneous_proc in range(num_dl):
            process_name=data_node+'-'+str(simultaneous_proc)
            if not process_name in processes.keys():
                processes[process_name]=multiprocessing.Process(target=worker_retrieve, 
                                                name=process_name,
                                                args=(queues_manager,data_node))
                processes[process_name].start()
    return processes

def worker_retrieve(queues_manager,data_node):
    #Loop indefinitely. Worker will be terminated by main process.
    while True:
        item = queues_manager.queues.get(data_node)
        if item=='STOP': break
        try:
            result = function_retrieve(item[1:])
            queues_manager.put_for_thread_id(item[0],(item[1],result))
        except:
            if item[2]['trial']>=3:
                print('Download failed with arguments ',item)
                raise
            #Put back in the queue. Do not raise. Simply put back in the queue so that failure
            #cannnot occur while working downloads work:
            item[2]['trial']+=1
            queues_manager.put_to_data_node_from_thread_id(item[0],item[2]['data_node'],item[1:])
    return

def function_retrieve(item):
    return item[0](item[1],item[2])

def worker_exit(queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options):
    while True:
        item = queues_manager.get_for_thread_id()
        if item=='STOP': break
        renewal_time=progress_report(item[0],item[1],queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options)
    return renewal_time

def launch_download(output,data_node_list,queues_manager,options):
    start_time = datetime.datetime.now()
    renewal_time = start_time
    queues_size=dict()
    if 'silent' in dir(options) and not options.silent:
        for data_node in data_node_list:
            queues_size[data_node]=queues_manager.queues.qsize(data_node)
        print('Remaining retrieval from data nodes:')
        string_to_print=['0'.zfill(len(str(queues_size[data_node])))+'/'+str(queues_size[data_node])+' paths from "'+data_node+'"' for
                            data_node in data_node_list]
        print ' | '.join(string_to_print)
        print 'Progress: '

    if 'serial' in dir(options) and options.serial:
        for data_node in data_node_list:
            queues_manager.queues.put(data_node,'STOP')
            worker_retrieve(queues_manager,data_node)
            renewal_time=worker_exit(queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options)
    else:
        renewal_time=worker_exit(queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options)
                
    if 'silent' in dir(options) and not options.silent:
        print
        print('Done!')
    return output

def progress_report(retrieval_function_handle,result,queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options):
    elapsed_time = datetime.datetime.now() - start_time
    renewal_elapsed_time=datetime.datetime.now() - renewal_time

    if retrieval_function_handle==retrieval_utils.download_files:
        if 'silent' in dir(options) and not options.silent:
            #print '\t', queues['end'].get()
            if result!=None:
                print '\t', result
                print str(elapsed_time)
    else:
        retrieval_utils.assign_tree(output,*result)
        output.sync()
        if 'silent' in dir(options) and not options.silent:
            string_to_print=[str(queues_size[data_node]-queues_manager.queues.qsize(data_node)).zfill(len(str(queues_size[data_node])))+
                             '/'+str(queues_size[data_node]) for
                                data_node in data_node_list]
            print str(elapsed_time)+', '+' | '.join(string_to_print)+'\r',

    #Maintain certificates:
    if ('username' in dir(options) and 
        options.username!=None and
        options.password!=None and
        renewal_elapsed_time > datetime.timedelta(hours=1)):
        #Reactivate certificates:
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password)
        renewal_time=datetime.datetime.now()
    return renewal_time

