#External:
import numpy as np
from itertools import groupby, count

def get_indices_from_dim(source,output):
    #This function finds which indices from source should be used and in which order:
    indices=np.arange(max(source.shape))[np.in1d(source,output)]
    try:
        return np.array([ indices[source[indices]==val][0] for val in output ])
    except IndexError:
        #The in1d might have encountered afloating point error. Make the equality fuzzy:
        #warnings.warn('Dimension matching was done to floating point tolerance for some model',UserWarning)
        indices=np.arange(max(source.shape))[np.array([np.any(np.isclose(output,item,atol=1e-5)) for item in source])]
        #try:
        return np.array([ indices[np.isclose(source[indices],val,atol=1e-5)][0] for val in output ])
        #except IndexError:
        #    print indices
        #    print(source,output)
        #    raise

def convert_indices_to_slices(indices):
    #This feature is currently broken (December 2014):
    if len(indices)>1:
        steps=np.maximum(np.unique(np.diff(indices)),1)
        optimal_step=steps[np.argmin(map(lambda x: len(convert_indices_to_slices_step(indices,x)),steps))]
        return convert_indices_to_slices_step(indices,optimal_step)
    else:
        return convert_indices_to_slices_step(indices,1)

def convert_indices_to_slices_step(indices,step):
    slices = []
    for key, it in groupby(enumerate(indices), lambda x: x[1] - step*x[0]):
        indices_slice = [y for x, y in it]
        slices.append(slice(indices_slice[0], indices_slice[-1]+1,step))
        #if len(indices) == 1:
        #    slices.append(slice(indices[0],indices[0]+1))
        #else:
        #    slices.append(slice(indices[0], indices[-1]+1))
    return slices

def slice_a_slice(initial_slice,slice_to_use):
    return convert_indices_to_slices_step(range(initial_slice.start,initial_slice.stop,initial_slice.step)[slice_to_use],initial_slice.step*slice_to_use.step)[0]

def prepare_indices(indices):
    sort_indices=np.argsort(indices)
    #Sort:
    indices=indices[sort_indices]
    #provide the inverse:
    unsort_indices=np.argsort(sort_indices)

    #always retrieve the first index (bug in netCDF4 python):
    #if not 0 in indices:
    #    indices=np.insert(indices[sort_indices],0,0,axis=0)
    #    unsort_indices+=1

    #Finally, convert the indices to slices:
    indices=convert_indices_to_slices(indices)
    return indices, unsort_indices

def largest_hyperslab(slices_dict):
    return np.prod([max([slice_length(item) for item in slices_dict[dim]])
                for dim in slices_dict.keys()])

def slice_length(slice_item):
    return len(range(slice_item.start,slice_item.stop,slice_item.step))
        
def retrieve_slice(variable,indices,unsort_indices,dim,dimensions,dim_id,max_request,getitem_tuple=tuple(),default=False):
    if default: return np.array([])
    if len(dimensions)>0:
        return np.take(np.concatenate(map(lambda x: retrieve_slice(variable,
                                                 indices,
                                                 unsort_indices,
                                                 dimensions[0],
                                                 dimensions[1:],
                                                 dim_id+1,
                                                 max_request,
                                                 getitem_tuple=getitem_tuple+(x,)),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
    else:
        return np.take(np.concatenate(map(lambda x: getitem_from_variable(variable,getitem_tuple+(x,),max_request),
                                                 indices[dim]),
                              axis=dim_id),unsort_indices[dim],axis=dim_id)
        #return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_tuple+(x,)),
        #                                         indices[dim]),
        #                      axis=dim_id),unsort_indices[dim],axis=dim_id)

def getitem_from_variable(variable,getitem_tuple,max_request):
    if ( max_request==None or
         max_request*1024*1024>32*np.prod([( (item.stop-item.start)//item.step) for item in getitem_tuple]) ):
        return variable.__getitem__(getitem_tuple)
    else:
        #Max number of steps along first dimension: 
        max_steps=max(int(np.floor(max_request*1024*1024/(32*np.prod([( (item.stop-item.start)//item.step) for item in getitem_tuple[1:]])))),1)
        return np.concatenate(map(lambda x: variable.__getitem__((x,)+getitem_tuple[1:]),
                                    [slice(getitem_tuple[0].start+id*max_steps*getitem_tuple[0].step,
                                           np.minimum(getitem_tuple[0].start+(id+1)*max_steps*getitem_tuple[0].step,getitem_tuple[0].stop),
                                           getitem_tuple[0].step) for id in range((getitem_tuple[0].stop//(max_steps*getitem_tuple[0].step))+1)]),
                                    axis=0)
                                            

#def getitem_pedantic(shape,getitem_tuple):
#    getitem_tuple_fixed=()
#    for item_id, item in enumerate(getitem_tuple):
#        indices_list=range(shape[item_id])[item]
#        if indices_list[-1]+item.step>shape[item_id]:
#            #Must fix the slice:
#            #getitem_tuple_fixed+=(slice(item.start,shape[item_id],item.step),)
#            getitem_tuple_fixed+=(indices_list,)
#        else:
#            getitem_tuple_fixed+=(item,)
#    return getitem_tuple_fixed
#        
#def retrieve_slice_pedantic(variable,indices,unsort_indices,dim,dimensions,dim_id,getitem_tuple=tuple()):
#    if len(dimensions)>0:
#        return np.take(np.concatenate(map(lambda x: retrieve_slice_pedantic(variable,
#                                                 indices,
#                                                 unsort_indices,
#                                                 dimensions[0],
#                                                 dimensions[1:],
#                                                 dim_id+1,
#                                                 getitem_tuple=getitem_tuple+(x,)),
#                                                 indices[dim]),
#                              axis=dim_id),unsort_indices[dim],axis=dim_id)
#    else:
#        shape=variable.shape
#        return np.take(np.concatenate(map(lambda x: variable.__getitem__(getitem_pedantic(variable.shape,getitem_tuple+(x,))),
#                                                 indices[dim]),
#                              axis=dim_id),unsort_indices[dim],axis=dim_id)
