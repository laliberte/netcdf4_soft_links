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

def start_download_processes(data_node_list,queues_manager,options):
    processes=dict()
    if not ('serial' in dir(options) and options.serial):
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_dl):
                process_name=data_node+'-'+str(simultaneous_proc)
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
        result = function_retrieve(item[1:])
        queues_manager.put_for_thread_id(item[0],result)
    return

def function_retrieve(item):
    return item[0](item[1],item[2])

def worker_exit(queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options):
    while True:
        item = queues_manager.get_for_thread_id()
        if item=='STOP': break
        renewal_time=progress_report(item,queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options)
    return renewal_time

def launch_download(output,data_node_list,queues_manager,options):
    #Second step: Process the queues:
    #print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    start_time = datetime.datetime.now()
    renewal_time = start_time
    queues_size=dict()
    if 'silent' in dir(options) and not options.silent:
        print('Remaining retrieval from data nodes:')
        for data_node in data_node_list:
            queues_size[data_node]=queues_manager.queues.qsize(data_node)
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
        #for data_node in data_node_list:
        #    for simultaneous_proc in range(options.num_dl):
        #        queues_manager.queues.put(data_node,'STOP')
        #for data_node in data_node_list:
        #    for simultaneous_proc in range(options.num_dl):
        renewal_time=worker_exit(queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options)
                
    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):
        output.close()

    if 'silent' in dir(options) and not options.silent:
        print
        print('Done!')
    return

def progress_report(item,queues_manager,data_node_list,queues_size,start_time,renewal_time,output,options):
    elapsed_time = datetime.datetime.now() - start_time
    renewal_elapsed_time=datetime.datetime.now() - renewal_time

    if (isinstance(output,netCDF4.Dataset) or
        isinstance(output,netCDF4.Group)):

        netcdf_utils.assign_tree(output,*item)
        output.sync()
        if 'silent' in dir(options) and not options.silent:
            string_to_print=[str(queues_size[data_node]-queues_manager.queues.qsize(data_node)).zfill(len(str(queues_size[data_node])))+
                             '/'+str(queues_size[data_node]) for
                                data_node in data_node_list]
            print str(elapsed_time)+', '+' | '.join(string_to_print)+'\r',
    else:
        if 'silent' in dir(options) and not options.silent:
            #print '\t', queues['end'].get()
            if item!=None:
                print '\t', item
                print str(elapsed_time)

    #Maintain certificates:
    if ('username' in dir(options) and 
        options.username!=None and
        options.password!=None and
        renewal_elapsed_time > datetime.timedelta(hours=1)):
        #Reactivate certificates:
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password)
        renewal_time=datetime.datetime.now()
    return renewal_time
