from __future__ import division, absolute_import, print_function

#External:
import netCDF4
import numpy as np
import itertools
import scipy.interpolate as interpolate
import scipy.spatial as spatial
import copy
import spherical_geometry.great_circle_arc as great_circle_arc

#Internal:
import netcdf4_soft_links.netcdf_utils as netcdf_utils

default_box=[0.0,360.0,-90.0,90.0]
def subset(input_file,output_file,lonlatbox=default_box,lat_var='lat',lon_var='lon'):
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
    optimal_slice = (lambda x: get_optimal_slices(x,mod_lonlatbox,lat_var,lon_var))
    with netCDF4.Dataset(input_file) as dataset:
        with netCDF4.Dataset(output_file,'w') as output:
            netcdf_utils.replicate_full_netcdf_recursive(dataset,output,slices=optimal_slice,check_empty=True)
    return

def get_optimal_slices(data,lonlatbox,lat_var,lon_var):
    if set([lat_var,lon_var]).issubset(data.variables.keys()):
        lat=data.variables[lat_var][:]
        lon=np.mod(data.variables[lon_var][:],360.0)
        if check_basic_consistency(data,lat_var,lon_var):
            lat_vertices, lon_vertices=get_vertices(data,lat_var,lon_var)
            region_mask=get_region_mask(lat_vertices,lon_vertices,lonlatbox)
            if ( set([lat_var+'_bnds',lon_var+'_bnds']).issubset(data.variables.keys())
                and not ( data.variables[lat_var+'_bnds'].shape==data.variables[lat_var].shape+(4,) and
                          data.variables[lon_var+'_bnds'].shape==data.variables[lon_var].shape+(4,))):
                dimensions=(lat_var,lon_var)
            else:
                dimensions=data.variables[lat_var].dimensions
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

def get_vertices_voronoi(lat,lon):
    """
    A general method to obtain grid vertices based on voronoi diagrams.
    """
    r=1.0 
    if len(lat.shape)==1 and len(lon.shape)==1:
        x, y, z = np.vectorize(sc_to_rc)(r,*np.meshgrid(lat,lon))
    elif lat.shape==lon.shape:
        x, y, z = np.vectorize(sc_to_rc)(r,lat,lon)
    else:
        raise InputError('latitude variable and longitute variable must either both be vectors or\
                          have the same shape')

    voronoi_diag=spatial.SphericalVoronoi(np.concatenate((x.ravel(),y.ravel(),z.ravel()),axis=1),radius=r)
    voronoi_diag.sort_vertices_of_regions()
    return map(simplify_to_four_spherical_vertices,voronoi_diag.regions)

def simplify_to_four_spherical_vertices_recursive(sorted_vertices):
    if sorted_vertices.shape[1]==3:
        return convert_to_lat_lon(np.concatenate((sorted_vertices,np.array([np.nan,np.nan,np.nan]),axis=0))
    elif sorted_vertices.shape[1]==4:
        return convert_to_lat_lon(sorted_vertices)
    else:
        min_id=find_minimum_arc_id(sorted_vertices)
        indices_list=range(sorted_vertices.shape[0])
        indices_list.remove(min_id)
        return simplify_to_four_spherical_vertices_recursive(np.take(sorted_vertices),indices_list,axis=0) 

def convert_to_lat_lon(sorted_vertices):
    return np.split(np.apply_along_axis(rc_to_sc_vec,1,sorted_vertices),2,axis=1)

def find_minimum_arc_id(sorted_vertices):
    return np.argmin(reduce(great_circle_arc.length,
                            np.split(
                                np.concatenate((sorted_vertices,sorted_vertices[0,:]),axis=0),
                                sorted_vertices.shape[0]+1,axis=0)),axis=0)

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
    return rc_to_sc(*point)

def rc_to_sc(x,y,z):
    '''
    Spherical coordinates to rectangular coordiantes.
    '''
    x=r*np.sin(0.5*np.pi-lat/180.0*np.pi)*np.cos(lon/180.0*np.pi)
    y=r*np.sin(0.5*np.pi-lat/180.0*np.pi)*np.sin(lon/180.0*np.pi)
    z=r*np.cos(0.5*np.pi-lat/180.0*np.pi)
    return lat,lon

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
