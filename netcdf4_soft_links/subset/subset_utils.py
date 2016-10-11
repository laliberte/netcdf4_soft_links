from __future__ import division, absolute_import, print_function

#External:
import netCDF4
import numpy as np
import itertools
import scipy.interpolate as interpolate
import scipy.spatial as spatial
import copy
#import spherical_geometry.great_circle_arc as great_circle_arc
import pandas

#Internal:
from .. import netcdf_utils

default_box=[0.0,360.0,-90.0,90.0]
def subset(input_file,output_file,lonlatbox=default_box,lat_var='lat',lon_var='lon',output_vertices=False):
    """
    Function to subset a hierarchical netcdf file. Its latitude and longitude
    should follow the CMIP5 conventions.
    """
    #Modify lonlatbox to handle periodic longitudes
    mod_lonlatbox=np.array(copy.copy(lonlatbox))
    if np.diff(np.mod(lonlatbox[:2],360))==0:
        if np.diff(lonlatbox[:2])>0:
            mod_lonlatbox[1]-=1e-6
        elif np.diff(lonlatbox[:2])<0:
            mod_lonlatbox[1]+=1e-6
    optimal_slice = (lambda x: get_optimal_slices(x,mod_lonlatbox,lat_var,lon_var,output_vertices))
    with netCDF4.Dataset(input_file,'r') as dataset:
        with netCDF4.Dataset(output_file,'w') as output:
            if output_vertices:
                transform = (lambda x,y,z: get_and_write_vertices(x,y,lat_var,lon_var,z))
                netcdf_utils.replicate_full_netcdf_recursive(dataset,output,transform=transform,slices=optimal_slice,check_empty=True)
            else:
                netcdf_utils.replicate_full_netcdf_recursive(dataset,output,slices=optimal_slice,check_empty=True)
    return

def get_optimal_slices(data,lonlatbox,lat_var,lon_var,output_vertices):
    if set([lat_var,lon_var]).issubset(data.variables.keys()):
        lat=data.variables[lat_var][:]
        lon=np.mod(data.variables[lon_var][:],360.0)
        if output_vertices or check_basic_consistency(data,lat_var,lon_var):
            lat_vertices, lon_vertices=get_vertices(data,lat_var,lon_var)
            region_mask=get_region_mask(lat_vertices,lon_vertices,lonlatbox)
            if ( set([lat_var+'_bnds',lon_var+'_bnds']).issubset(data.variables.keys())
                and not ( data.variables[lat_var+'_bnds'].shape==data.variables[lat_var].shape+(4,) and
                          data.variables[lon_var+'_bnds'].shape==data.variables[lon_var].shape+(4,))):
                dimensions=(lat_var,lon_var)
            else:
                dimensions=data.variables[lat_var].dimensions
        else:
            if len(lat.shape) == 1 and len(lon.shape) == 1:
                # Broadcast:
                LON, LAT = np.meshgrid(lon, lat)
                region_mask=get_region_mask(LAT[...,np.newaxis],LON[...,np.newaxis],lonlatbox)
            else:
                region_mask=get_region_mask(lat[...,np.newaxis],lon[...,np.newaxis],lonlatbox)
            if ( data.variables[lat_var].dimensions==(lat_var,) and
               data.variables[lon_var].dimensions==(lon_var,) ):
               dimensions=(lat_var,lon_var)
            else:
               dimensions=data.variables[lat_var].dimensions

        return {dimensions[id]:
                       np.arange(region_mask.shape[id])[np.sum(region_mask,axis=1-id)>0] for id in [0,1]}
    else:
        return dict()

def get_region_mask(lat,lon,lonlatbox):
    """
    lat and lon must have an extra trailing dimension that can correspond to
    a vertices dimension
    """
    if np.diff(np.mod(lonlatbox[:2],360))<0:
        lon_region_mask=np.logical_not(np.logical_and(
                                   np.min(np.mod(lon,360),axis=-1)>=np.mod(lonlatbox[1],360),
                                   np.max(np.mod(lon,360),axis=-1)<=np.mod(lonlatbox[0],360)))
    else:
        lon_region_mask=np.logical_and(
                                   np.min(np.mod(lon,360),axis=-1)>=np.mod(lonlatbox[0],360),
                                   np.max(np.mod(lon,360),axis=-1)<=np.mod(lonlatbox[1],360))
    lat_region_mask=np.logical_and(
                                   np.min(lat,axis=-1)>=lonlatbox[2],
                                   np.max(lat,axis=-1)<=lonlatbox[3])
    return np.logical_and(lon_region_mask,lat_region_mask)


def get_and_write_vertices(data,output,lat_var,lon_var,comp_slices):
    if ( not lat_var+'_vertices' in output.variables.keys() or
         not lon_var+'_vertices' in output.variables.keys() ):
        lat_vertices,lon_vertices=get_vertices(data,lat_var,lon_var)
        record_vertices(data,output,lat_var,lat_vertices,comp_slices)
        record_vertices(data,output,lon_var,lon_vertices,comp_slices)
    return output

def record_vertices(data,output,var,vertices,comp_slices):
    dim='nv'
    if not dim in output.dimensions.keys():
        output.createDimension('nv',size=4)
    out_dims=data.variables[var].dimensions+('nv',)
    getitem_tuple=tuple([comp_slices[var_dim] if var_dim in comp_slices.keys()
                                                else slice(None,None,1) for var_dim in out_dims])
    temp=output.createVariable(var+'_vertices','f',out_dims)
    sliced_vertices=vertices
    for axis_id,indexing in enumerate(getitem_tuple):
        if isinstance(indexing,slice):
            sliced_vertices=np.ma.take(sliced_vertices,np.arange(sliced_vertices.shape[axis_id])[indexing],axis=axis_id)
        else:
            sliced_vertices=np.ma.take(sliced_vertices,indexing,axis=axis_id)
    temp[:]=sliced_vertices
    return

def get_vertices(data,lat_var,lon_var):
    if not set([lat_var+'_vertices',lon_var+'_vertices']).issubset(data.variables.keys()):
        if set([lat_var+'_bnds',lon_var+'_bnds']).issubset(data.variables.keys()):
            if ( data.variables[lat_var+'_bnds'].shape==data.variables[lat_var].shape+(4,) and
                 data.variables[lon_var+'_bnds'].shape==data.variables[lon_var].shape+(4,)):
                #lat_bnds and lon_bnds are in fact lat and lon vertices: 
                lat_vertices=data.variables[lat_var+'_bnds'][:]
                lon_vertices=data.variables[lon_var+'_bnds'][:]
            else:
                lat_vertices,lon_vertices=get_vertices_from_bnds(data.variables[lat_var+'_bnds'][:],
                                                                 np.mod(data.variables[lon_var+'_bnds'][:],360))
        elif set(['rlat_bnds','rlon_bnds']).issubset(data.variables.keys()):
            lat_vertices,lon_vertices=get_spherical_vertices_from_rotated_bnds(data.variables['rlat'][:],
                                                                               data.variables['rlon'][:],
                                                                               data.variables[lat_var][:],
                                                                               np.mod(data.variables[lon_var][:],360),
                                                                               data.variables['rlat_bnds'][:],
                                                                               data.variables['rlon_bnds'][:])
        else:
            lat_vertices,lon_vertices=get_vertices_voronoi(data.variables[lat_var][:],data.variables[lon_var][:])
    else:
        lat_vertices=data.variables[lat_var+'_vertices'][:]
        lon_vertices=np.mod(data.variables[lon_var+'_vertices'][:],360)
    return lat_vertices, lon_vertices

def get_vertices_voronoi(lat,lon,do_not_simplify_edge_number=4):
    """
    A general method to obtain grid vertices based on voronoi diagrams.
    """
    r=1.0 
    if len(lat.shape)==1 and len(lon.shape)==1:
        LON, LAT = np.meshgrid(lon,lat)
    elif lat.shape==lon.shape:
        LON, LAT = lon, lat
    else:
        raise InputError('latitude variable and longitute variable must either both be vectors or\
                          have the same shape')
    shape=LON.shape
    x, y, z = np.vectorize(sc_to_rc)(r,LAT,LON)

    points=np.concatenate((x.ravel()[:,np.newaxis],
                           y.ravel()[:,np.newaxis],
                           z.ravel()[:,np.newaxis]),axis=1)
    mask=np.logical_or.reduce(np.logical_not(np.ma.getmaskarray(points)),1)
    voronoi_diag=spatial.SphericalVoronoi(np.ma.filled(points[mask,:],fill_value=np.nan),radius=r)

    #create edges dataframe:
    df_regions=regions_dataframe(lat.ravel(),lon.ravel())
    df_vertices=vertices_dataframe(voronoi_diag.vertices)

    #sort vertices in counterclockwise direction:
    voronoi_diag.sort_vertices_of_regions()
    #Creates edges dataframe:
    df_edges=pd.concat(map(lambda x: region_edges_dataframe(*x), enumerate(voronoi_diag.regions)))

    edges_df=pd.concat(map(lambda x: get_region_edges(*x),enumerate(voronoi_diag.regions)))
    #label edges:
    edges_df['edge_id']=label_edges(edges_df)

    region_edges=edges_df.groupby('region').size()
    regions_to_consider=regions_edges['region'][region_edges['size']>do_not_simplify_edge_number,:]

    
    simplified_vertices_of_regions=map(simplify_to_four_spherical_vertices_recursive,vertices_of_regions)
    lat_vertices[mask,:],lon_vertices[mask,:] = map(np.concatenate,
                                                    zip(*simplified_vertices_of_regions))
    return map(lambda x: np.reshape(np.ma.fix_invalid(x),shape+(4,)),[lat_vertices,lon_vertices])

def regions_dataframe(lat,lon):
    df=pd.DataFrame()
    df['lat']=lat
    df['lon']=lon
    return df

def vertices_dataframe(vertices):
    df = pd.DataFrame()
    df['x']=zip(*vertices)[0]
    df['y']=zip(*vertices)[1]
    df['z']=zip(*vertices)[2]
    return df

def region_edges_dataframe(region_id,vx_ids):
    df=pd.DataFrame()
    df['A']=vx_ids
    df['B'][:-1]=vx_ids[1:]
    df['B'][-1]=vx_ids[0]
    df['region']=region_id
    #df['reversed']=False
    #df.loc[(df['A']
    return df

def get_region_vertices(vertices,region_indices):
    return np.take(vertices,region_indices,axis=0)

#def simplify_to_four_spherical_vertices_recursive(sorted_vertices):
#    if sorted_vertices.shape[0]==3:
#        return convert_to_lat_lon(np.concatenate((sorted_vertices,np.array([np.nan,np.nan,np.nan])[np.newaxis,:]),axis=0))
#    elif sorted_vertices.shape[0]==4:
#        return convert_to_lat_lon(sorted_vertices)
#    else:
#        min_id=find_minimum_arc_id(sorted_vertices)
#        midpoint=arc_midpoint(sorted_vertices[min_id,:],sorted_vertices[np.mod(min_id+1,sorted_vertices.shape[0]),:])
#        fewer_sorted_vertices=np.empty((sorted_vertices.shape[0]-1,3))
#        if min_id==sorted_vertices.shape[0]:
#            fewer_sorted_vertices[:-1,:]=sorted_vertices[1:min_id,:]
#            fewer_sorted_vertices[-1,:]=midpoint
#        else:
#            fewer_sorted_vertices[:min_id,:]=sorted_vertices[:min_id,:]
#            fewer_sorted_vertices[min_id,:]=midpoint
#            fewer_sorted_vertices[min_id+1:,:]=sorted_vertices[min_id+2:,:]
#        return simplify_to_four_spherical_vertices_recursive(fewer_sorted_vertices)
#
#def find_minimum_arc_id(sorted_vertices):
#    return np.argmin(map(lambda x: arc_length(sorted_vertices[x,:],
#                                              sorted_vertices[np.mod(x+1,sorted_vertices.shape[0]),:]),
#                                    range(len(sorted_vertices)-1)))
#
#def arc_length(a,b):
#    if np.allclose(a,b):
#        return 0.0
#    else:
#        return great_circle_arc.length(a,b)
#
#def arc_midpoint(a,b):
#    if np.allclose(a,b):
#        midpoint=0.5*(a+b)
#        midpoint*=np.sqrt((a**2).sum())/np.sqrt((midpoint**2).sum())
#        return midpoint
#    else:
#        return great_circle_arc.midpoint(a,b)

def convert_to_lat_lon(sorted_vertices):
    return map(np.transpose,np.split(np.apply_along_axis(rc_to_sc_vec,1,sorted_vertices)[:,1:],2,axis=1))

def get_vertices_from_bnds(lat_bnds,lon_bnds):
    #Create 4 vertices:
    return np.broadcast_arrays(np.append(lat_bnds[:,np.newaxis,:],lat_bnds[:,np.newaxis,:],axis=-1),
                               np.insert(lon_bnds[np.newaxis,:,:],[0,1],lon_bnds[np.newaxis,:,:],axis=-1))


def sort_vertices_counterclockwise_array(lat_vertices, lon_vertices):
    struct=np.empty(lat_vertices.shape,dtype=[('lat_vertices',lat_vertices.dtype),
                                              ('lon_vertices',lat_vertices.dtype)])
    struct['lat_vertices']=np.ma.filled(lat_vertices,fill_value=np.nan)
    struct['lon_vertices']=np.ma.filled(lon_vertices,fill_value=np.nan)
    out_struct=np.apply_along_axis(sort_vertices_counterclockwise_struct,-1,struct)
    return np.ma.fix_invalid(out_struct['lat_vertices']), np.ma.fix_invalid(out_struct['lon_vertices'])
    #return (np.ma.masked_where(mask,out_struct['lat_vertices']),
    #        np.ma.masked_where(mask,out_struct['lon_vertices']))

def sort_vertices_counterclockwise_struct(struct):
    out_struct=np.empty_like(struct)
    out_struct['lat_vertices'],out_struct['lon_vertices']=map(lambda x: np.ma.filled(x,fill_value=np.nan),
                                                                sort_vertices_counterclockwise(np.ma.fix_invalid(struct['lat_vertices']),
                                                                                               np.ma.fix_invalid(struct['lon_vertices'])))
    return out_struct

def sort_vertices_counterclockwise(lat_vertices,lon_vertices):
    '''
    Ensure that vertices are listed in a counter-clockwise fashion
    '''
    vec=np.ma.concatenate(np.vectorize(sc_to_rc)(1.0,lat_vertices[:,np.newaxis],lon_vertices[:,np.newaxis]),axis=1)
    vec_c=np.ma.mean(vec,axis=0)
    vec-=vec_c[np.newaxis,:]

    cross=np.zeros((4,4))
    for i in range(cross.shape[0]):
        for j in range(cross.shape[1]):
            cross[i,j]=np.ma.dot(vec_c,np.cross(vec[i,:],vec[j,:]))

    id0=np.argmax(np.mod(lon_vertices,360))
    for id1 in range(4):
        for id2 in range(4):
            for id3 in range(4):
                if (len(set([id0,id1,id2,id3]))==4 and
                    cross[id0,id1]>0.0 and
                    cross[id1,id2]>0.0 and
                    cross[id2,id3]>0.0 and
                    cross[id3,id0]>0.0):
                    id_list=np.array([id0,id1,id2,id3])
                    return lat_vertices[id_list],lon_vertices[id_list]
    return lat_vertices, lon_vertices

def rc_to_sc_vec(point):
    return np.array(rc_to_sc(*point))

def rc_to_sc(x,y,z):
    '''
    Spherical coordinates to rectangular coordiantes.
    '''
    if np.any(np.isnan([x,y,z])):
        return np.nan, np.nan,np.nan

    r=np.sqrt(x**2+y**2+z**2)
    if z==0.0:
        lat=0.0
    else:
        lat=(0.5*np.pi-np.arccos(z/r))*180.0/np.pi
    if x==0.0:
        lon=90.0
    else:
        lon=np.arctan(y/x)*180.0/np.pi
    return r, lat, lon

def sc_to_rc(r,lat,lon):
    '''
    Spherical coordinates to rectangular coordiantes.
    '''
    x=r*np.sin(0.5*np.pi-lat/180.0*np.pi)*np.cos(lon/180.0*np.pi)
    y=r*np.sin(0.5*np.pi-lat/180.0*np.pi)*np.sin(lon/180.0*np.pi)
    z=r*np.cos(0.5*np.pi-lat/180.0*np.pi)
    return x,y,z

def get_spherical_vertices_from_rotated_bnds(rlat,rlon,lat,lon,rlat_bnds,rlon_bnds):
    '''
    It is assumed that input longitudes (but not rotated longitudes) are 0 to 360 degrees
    '''
    rlat_vertices,rlon_vertices=get_vertices_from_bnds(rlat_bnds,rlon_bnds)
    rescale=np.pi/180.0
    rlon_bnds=fix_lon_bnds(rlon_bnds)
    rlon_offset=np.min(rlon_bnds)
    rlat_valid_points=np.logical_and(rlat_vertices<np.max(rlat),rlat_vertices>np.min(rlat))
    lat_vertices=np.ma.empty_like(rlat_vertices)
    lat_vertices[np.logical_not(rlat_valid_points)]=np.ma.masked
    lat_vertices[rlat_valid_points]=spherical_interp((rlat+90.0)*rescale,(rlon-rlon_offset)*rescale,lat,(rlat_vertices[rlat_valid_points]+90.0)*rescale,
                                                                                                   (rlon_vertices[rlat_valid_points]-rlon_offset)*rescale)
    rlon_valid_points=np.logical_and(rlon_vertices<np.max(rlon),rlon_vertices>np.min(rlon))
    lon_vertices=np.ma.empty_like(rlon_vertices)
    lon_vertices[np.logical_not(rlat_valid_points)]=np.ma.masked
    lon_vertices[rlat_valid_points]=spherical_interp((rlat+90.0)*rescale,(rlon-rlon_offset)*rescale,lon,(rlat_vertices[rlat_valid_points]+90.0)*rescale,
                                                                                                   (rlon_vertices[rlat_valid_points]-rlon_offset)*rescale)
    lon_vertices_mod=np.ma.empty_like(rlon_vertices)
    lon_vertices_mod[np.logical_not(rlat_valid_points)]=np.ma.masked
    lon_vertices_mod[rlat_valid_points]=np.mod(spherical_interp((rlat+90.0)*rescale,(rlon-rlon_offset)*rescale,np.mod(lon-180.0,360),(rlat_vertices[rlat_valid_points]+90.0)*rescale,
                                                                                                   (rlon_vertices[rlat_valid_points]-rlon_offset)*rescale)+180.0,360)
                   
    lon_vertices[lon<90.0,:]=lon_vertices_mod[lon<90.0,:]
    lon_vertices[lon>270.0,:]=lon_vertices_mod[lon>270.0,:]

    return lat_vertices, lon_vertices

def spherical_interp(rlat,rlon,arr,rlat_vertices,rlon_vertices):
    interpolants_simple=interpolate.RectSphereBivariateSpline(rlat,rlon,arr)
    interpolants=(lambda x: interpolants_simple.ev(*x))
    N=100
    return np.concatenate(map(interpolants,zip(np.array_split(rlat_vertices,N),
                                               np.array_split(rlon_vertices,N))),axis=0)

def fix_lon_bnds(lon_bnds):
    range=lon_bnds.max()-lon_bnds.min()
    if range<360.0:
        diff=360.0-range
        max_diff=np.max(np.diff(lon_bnds,axis=1))
        if (diff-0.1<=max_diff):
            lon_bnds[0,0]-=diff*0.5
            lon_bnds[-1,1]+=diff*0.5
    return lon_bnds

def check_basic_consistency(data,lat_var,lon_var):
    coords=[(lat_var,lon_var),('rlat','rlon')]
    bnds=['vertices','bnds']
    has_coordinates_bnds=np.any([ set(coordinates_bnds).issubset(data.variables.keys())
                                for coordinates_bnds in 
                                        itertools.chain.from_iterable([[ [single_coord+'_'+bnd 
                                                                        for single_coord in coord] 
                                                                    for coord in coords] 
                                                                        for bnd in bnds])] )
    if not has_coordinates_bnds:
        return False
    return True
