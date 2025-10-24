/*
    Copyright (C) 2025  Francesc Alted
    http://blosc.org
    License: MIT (see LICENSE.txt)

    Test for array datatypes (H5T_ARRAY).

    To compile this program:

    h5cc blosc_filter.c test_array.c -o test_array -lblosc -lpthread

    To run:

    $ ./test_array
    Blosc version info: 1.21.7.dev ($Date:: 2024-06-24 #$)
    Success!
    $ h5ls -v test_array.h5
    Opened "test_array.h5" with sec2 driver.
    dset                     Dataset {1000/1000}
        Location:  1:800
        Links:     1
        Chunks:    {10} 40000 bytes
        Storage:   4000000 logical bytes, 168312 allocated bytes, 2376.54% utilization
        Filter-0:  blosc-32001 OPT {2, 2, 4, 40000}
        Type:      [100,10] native float

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hdf5.h"
#include "blosc_filter.h"

#define SIZE 1000*1000
#define SHAPE {1000}
#define NDIM 1
#define TYPE_SHAPE {100, 10}
#define TYPE_NDIM 2
#define CHUNKSHAPE {10, 100, 10}

int main(){

    static float data[SIZE];
    static float data_out[SIZE];
    const hsize_t shape[] = SHAPE;
    const hsize_t chunkshape[] = CHUNKSHAPE;
    const hsize_t type_shape[] = TYPE_SHAPE;
    char *version, *date;
    int r, i;
    int return_code = 1;

    hid_t fid = 0, sid = 0, dset = 0, plist = 0, dtype = 0;

    for(i=0; i<SIZE; i++){
        data[i] = i;
        data_out[i] = -1;
    }

    /* Register the filter with the library */
    r = register_blosc(&version, &date);
    if(r<0) goto failed;
    printf("Blosc version info: %s (%s)\n", version, date);
    free(version);
    free(date);

    sid = H5Screate_simple(NDIM, shape, NULL);
    if(sid<0) goto failed;

    fid = H5Fcreate("test_array.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if(fid<0) goto failed;

    plist = H5Pcreate(H5P_DATASET_CREATE);
    if(plist<0) goto failed;

    /* Chunked layout required for filters */
    r = H5Pset_chunk(plist, NDIM, chunkshape);
    if(r<0) goto failed;

    r = H5Pset_filter(plist, FILTER_BLOSC, H5Z_FLAG_OPTIONAL, 0, NULL);
    if(r<0) goto failed;

    /* Define H5T_ARRAY datatype */
    dtype = H5Tarray_create(H5T_NATIVE_FLOAT, TYPE_NDIM, type_shape);
    if(dtype<0) goto failed;

    dset = H5Dcreate(fid, "dset", dtype, sid, H5P_DEFAULT, plist, H5P_DEFAULT);
    if(dset<0) goto failed;

    r = H5Dwrite(dset, dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
    if(r<0) goto failed;

    r = H5Dread(dset, dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out);
    if(r<0) goto failed;

    for(i=0;i<SIZE;i++){
        if(data[i] != data_out[i]) goto failed;
    }

    fprintf(stdout, "Success!\n");

    return_code = 0;

    failed:

    if(dtype>0) H5Tclose(dtype);
    if(dset>0)  H5Dclose(dset);
    if(sid>0)   H5Sclose(sid);
    if(plist>0) H5Pclose(plist);
    if(fid>0)   H5Fclose(fid);

    return return_code;
}
