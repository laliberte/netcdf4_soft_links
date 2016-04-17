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
    #if len(indices)>1:
    #    steps=np.unique(np.diff(indices))
    #    optimal_step=steps[np.argmin(map(lambda x: len(convert_indices_to_slices_step(indices,x)),steps))]
    #    return convert_indices_to_slices_step(indices,optimal_step)
    #else:
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
        
