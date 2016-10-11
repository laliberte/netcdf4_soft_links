#External:
import multiprocessing
import numpy as np
import Queue
import requests
import requests_cache

#Internal:

#http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing
class Shared_Counter(object):
    #Shared counter class
    def __init__(self,manager):
        self.val = manager.Value('i', 0)
        self.lock = manager.Lock()

    def increment(self,n=1):
        with self.lock:
            return self.increment_no_lock(n=n)

    def increment_no_lock(self,n=1):
        value=self.val.value
        self.val.value += n
        return value + n

    def decrement(self,n=1):
        with self.lock:
            return self.decrement_no_lock(n=n)

    def decrement_no_lock(self,n=1):
        value=self.val.value
        self.val.value -= n
        return value - n

    @property
    def value(self):
        with self.lock:
            return self.val.value

    @property
    def value_no_lock(self):
        return self.val.value

class Semaphores_data_node:
    #Shared semaphores class
    def __init__(self,manager,n=20,num_concurrent=1):
        self.dict = manager.dict()
        self.lock = manager.Lock()
        self.list = [manager.Semaphore(num_concurrent) for id in range(n)]

    def add_new_data_node(self,data_node): 
        #Add a pointer to a semaphore:
        with self.lock:
            if not data_node in self.dict.keys():
                if len(self.dict.keys())>0:
                    self.dict[data_node]=1+np.max([self.dict[key] for key in self.dict.keys()])
                else:
                    self.dict[data_node]=0
        return

    def __getitem__(self,data_node):
        #Just send the semaphore object:
        return self.list[self.dict[data_node]]

    def keys(self):
        #Just send the semaphore object:
        return self.dict.keys()

class Queues_data_node:
    #Shared queues class
    def __init__(self,manager,n=20):
        self.dict = manager.dict()
        self.lock = manager.Lock()
        self.list = [manager.Queue() for id in range(n)]
        self.list_expected = [Shared_Counter(manager) for id in range(n)]

    def add_new_data_node(self,data_node): 
        #Add a pointer to a queue:
        with self.lock:
            if not data_node in self.dict.keys():
                if len(self.dict.keys())>0:
                    self.dict[data_node]=1+np.max([self.dict[key] for key in self.dict.keys()])
                else:
                    self.dict[data_node]=0
        return

    def put(self,data_node,item):
        #Just put the item in the queue and increment the counter:
        with self.list_expected[self.dict[data_node]].lock:
            self.list[self.dict[data_node]].put(item)
            self.list_expected[self.dict[data_node]].increment_no_lock()
        return 

    def get(self,data_node):
        #Just send the queue object:
        item=self.list[self.dict[data_node]].get()
        #Decrement after, in case there is an error in the get command:
        self.list_expected[self.dict[data_node]].decrement()
        return item

    def qsize(self,data_node):
        return self.list_expected[self.dict[data_node]].value

    def keys(self):
        return self.dict.keys()

class NC4SL_queues_manager:
    def __init__(self,options,processes_names,manager=None,remote_netcdf_kwargs=dict()):
        if manager==None:
            self.manager=multiprocessing.Manager()
        else:
            self.manager=manager

        #Shared sessions among downloads. Appears tricky to use...
        #self.session=requests_sessions.create_single_session(**remote_netcdf_kwargs)
        ##Set the pool size to number of downloads:
        #adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=options.num_dl)
        #self.session.mount('http://',adapter)
        #self.session.mount('https://',adapter)

        self.semaphores=Semaphores_data_node(self.manager,num_concurrent=options.num_dl)
        self.queues=Queues_data_node(self.manager)
        #Create gather download_queues:
        for proc_id in processes_names:
            thread_id='download_'+proc_id
            setattr(self,thread_id, self.manager.Queue())
            setattr(self,thread_id+'_expected', Shared_Counter(self.manager))
            setattr(self,thread_id+'_closed', self.manager.Event())
        return
                
    def put_to_data_node(self,data_node,item):
        thread_id='download_'+multiprocessing.current_process().name
        #thread_id='download_'+process_name
        self.put_to_data_node_from_thread_id(thread_id,data_node,item)
        return

    def put_to_data_node_from_thread_id(self,thread_id,data_node,item):
        #Increment expected counter
        with getattr(self,thread_id+'_expected').lock:
            #Pass the thread_id to data_node queue:
            self.queues.put(data_node,(thread_id,)+item)
            getattr(self,thread_id+'_expected').increment_no_lock()
        return

    def put_again_to_data_node_from_thread_id(self,thread_id,data_node,item):
        #The expectation has already been recorded:
        self.queues.put(data_node,(thread_id,)+item)
        return

    def set_closed(self):
        thread_id='download_'+multiprocessing.current_process().name
        #thread_id='download_'+process_name
        getattr(self,thread_id+'_closed').set()
        return

    def set_opened(self):
        thread_id='download_'+multiprocessing.current_process().name
        #thread_id='download_'+process_name
        getattr(self,thread_id+'_closed').clear()
        return

    def put_for_thread_id(self,thread_id,item):
        getattr(self,thread_id).put(item)
        return

    def get_for_thread_id(self):
        timeout=0.1
        thread_id='download_'+multiprocessing.current_process().name
        #thread_id='download_'+process_name
        while not (getattr(self,thread_id+'_closed').is_set() 
                   and getattr(self,thread_id+'_expected').value==0):
            try:
                with getattr(self,thread_id+'_expected').lock:
                    item = getattr(self,thread_id).get(True,timeout)
                    #Decrement expected counter
                    getattr(self,thread_id+'_expected').decrement_no_lock()
                return item
            except Queue.Empty:
                pass
        return 'STOP'
