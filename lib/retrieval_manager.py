#External:
import multiprocessing
import datetime
import sys
from StringIO import StringIO

#Internal:
import netcdf_utils
import remote_netcdf
import certificates

def start_processes(options,data_node_list):
    queues=define_queues(options,data_node_list)
    #Redefine data nodes:
    data_node_list=queues.keys()
    data_node_list.remove('end')

    if not ('serial' in dir(options) and options.serial):
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_procs):
                process=multiprocessing.Process(target=worker_retrieve, 
                                                name=data_node+'-'+str(simultaneous_proc),
                                                args=(queues[data_node], queues['end']))
                process.start()
    return queues, data_node_list

class MyStringIO(StringIO):
    def __init__(self, queue, *args, **kwargs):
        StringIO.__init__(self, *args, **kwargs)
        self.queue = queue
    def flush(self):
        self.queue.put((multiprocessing.current_process().name, self.getvalue()))
        self.truncate(0)

def initializer(queue):
     sys.stderr = sys.stdout = MyStringIO(queue)

def worker_retrieve(input, output):
    for tuple in iter(input.get, 'STOP'):
        result = tuple[0](tuple[1],tuple[2])
        output.put(result)
    output.put('STOP')
    return

def launch_download_and_remote_retrieve(output,data_node_list,queues,retrieval_function,options,user_pass=None):
    #Second step: Process the queues:
    #print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    start_time = datetime.datetime.now()
    renewal_time = datetime.datetime.now()
    print('Remaining retrieval from data nodes:')
    queues_size=dict()
    for data_node in data_node_list:
        queues_size[data_node]=queues[data_node].qsize()
    string_to_print=['0'.zfill(len(str(queues_size[data_node])))+'/'+str(queues_size[data_node])+' paths from "'+data_node+'"' for
                        data_node in data_node_list]
    print ' | '.join(string_to_print)
    print 'Progress: '

    if 'serial' in dir(options) and options.serial:
        for data_node in data_node_list:
            queues[data_node].put('STOP')
            worker_retrieve(queues[data_node], queues['end'])
            for tuple in iter(queues['end'].get, 'STOP'):
                renewal_time=progress_report(options,retrieval_function,output,tuple,queues,queues_size,data_node_list,start_time,renewal_time,user_pass=user_pass)
        
    else:
        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_procs):
                queues[data_node].put('STOP')
        #processes=dict()
        #for data_node in data_node_list:
        #    queues[data_node].put('STOP')
        #    processes[data_node]=multiprocessing.Process(target=worker_retrieve, args=(queues[data_node], queues['end']))
        #    processes[data_node].start()

        for data_node in data_node_list:
            for simultaneous_proc in range(options.num_procs):
                for tuple in iter(queues['end'].get, 'STOP'):
                    renewal_time=progress_report(options,retrieval_function,output,tuple,queues,queues_size,data_node_list,start_time,renewal_time,user_pass=user_pass)

    if retrieval_function=='retrieve_path_data':
        output.close()

    print
    print('Done!')
    #print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return

def progress_report(options,retrieval_function,output,tuple,queues,queues_size,data_node_list,start_time,renewal_time,user_pass=None):
    elapsed_time = datetime.datetime.now() - start_time
    renewal_elapsed_time=datetime.datetime.now() - renewal_time
    if retrieval_function=='retrieve_path':
        #print '\t', queues['end'].get()
        if tuple!=None:
            print '\t', tuple
            print str(elapsed_time)
    elif retrieval_function=='retrieve_path_data':
        netcdf_utils.assign_tree(output,*tuple)
        output.sync()
        string_to_print=[str(queues_size[data_node]-queues[data_node].qsize()).zfill(len(str(queues_size[data_node])))+
                         '/'+str(queues_size[data_node]) for
                            data_node in data_node_list]
        print str(elapsed_time)+', '+' | '.join(string_to_print)+'\r',

    #Maintain certificates:
    if ('username' in dir(options) and 
        options.username!=None and
        user_pass!=None and
        renewal_elapsed_time > datetime.timedelta(hours=1)):
        #Reactivate certificates:
        certificates.retrieve_certificates(options.username,options.service,user_pass=user_pass)
        renewal_time=datetime.datetime.now()
    return renewal_time

def define_queues(options,data_node_list):
    queues={data_node : multiprocessing.Queue() for data_node in data_node_list}
    queues['end']= multiprocessing.Queue()
    if 'source_dir' in dir(options) and options.source_dir!=None:
        queues[remote_netcdf.get_data_node(options.source_dir,'local_file')]=multiprocessing.Queue()
    return queues

