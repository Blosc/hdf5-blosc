/*
    Copyright (C) 2025  Francesc Alted
    http://blosc.org
    License: MIT (see LICENSE.txt)

    Test for variable-width strings and other VLEN types.

    To compile this program:

    h5cc blosc_filter.c test_strings.c -o test_strings -lblosc -lpthread

    To run:

    $ ./test_strings
    Blosc version info: 1.21.7.dev ($Date:: 2024-06-24 #$)
    Success!
    $ h5ls -v test_strings.h5
    Opened "test_strings.h5" with sec2 driver.
    dset                     Dataset {100000/100000}
        Location:  1:800
        Links:     1
        Chunks:    {1000} 8000 bytes
        Storage:   800000 logical bytes, 505070 allocated bytes, 158.39% utilization
        Filter-0:  blosc-32001 OPT {2, 2, 1, 0}
        Type:      variable-length null-terminated UTF-8 string

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hdf5.h"
#include "blosc_filter.h"

#define SIZE 100000
#define SHAPE {100000}
#define NDIM 1
#define CHUNKSHAPE {1000}
#define MAX_STRING_LEN 14

int main(){

    static char* data[SIZE];
    static char* data_out[SIZE];
    const hsize_t shape[] = SHAPE;
    const hsize_t chunkshape[] = CHUNKSHAPE;
    char *version, *date;
    int r, i;
    int return_code = 1;

    hid_t fid = 0, sid = 0, dset = 0, plist = 0, dtype = 0;

    /* Note: for this example we could call a single malloc and fill it back
       to back with Hello 0\0World 0\0Hello 1\0World 1\0...
       However we want to test behaviour when the strings are originally 
       non-contiguous in memory. */
    memset(data, 0, sizeof(char*) * SIZE); /* For safe cleanup */
    for(i=0; i<SIZE; i++){
        data[i] = malloc(MAX_STRING_LEN);
        if(!data[i]) goto failed;
        snprintf(data[i], MAX_STRING_LEN, "Hello %d", i);
    }

    /* Register the filter with the library */
    r = register_blosc(&version, &date);
    if(r<0) goto failed;
    printf("Blosc version info: %s (%s)\n", version, date);
    free(version);
    free(date);

    sid = H5Screate_simple(NDIM, shape, NULL);
    if(sid<0) goto failed;

    fid = H5Fcreate("test_strings.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if(fid<0) goto failed;

    plist = H5Pcreate(H5P_DATASET_CREATE);
    if(plist<0) goto failed;

    /* Chunked layout required for filters */
    r = H5Pset_chunk(plist, NDIM, chunkshape);
    if(r<0) goto failed;

    /* FIXME https://github.com/HDFGroup/hdf5/issues/5942
       libhdf5 skips blosc_set_local() for H5T_VARIABLE data types, so you
       *must* compile by hand cd_values[0:4], which are normally filled in
       by blosc_set_local().
    */
    unsigned int cd_values[4] = {FILTER_BLOSC_VERSION, BLOSC_VERSION_FORMAT, 1, 0};
    r = H5Pset_filter(plist, FILTER_BLOSC, H5Z_FLAG_OPTIONAL, 4, cd_values);
    if(r<0) goto failed;

    /* Define variable-length (NULL-terminated) UTF-8 string datatype */
    dtype = H5Tcopy(H5T_C_S1);
    if(dtype<0) goto failed;
    r = H5Tset_size(dtype, H5T_VARIABLE);
    if(r<0) goto failed;
    /* Optional but recommended: store as UTF-8 strings */
    r = H5Tset_cset(dtype, H5T_CSET_UTF8);
    if(r<0) goto failed;

    dset = H5Dcreate(fid, "dset", dtype, sid, H5P_DEFAULT, plist, H5P_DEFAULT);
    if(dset<0) goto failed;

    r = H5Dwrite(dset, dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
    if(r<0) goto failed;

    r = H5Dread(dset, dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, data_out);
    if(r<0) goto failed;

    for(i=0;i<SIZE;i++){
        if(strcmp(data[i], data_out[i]) != 0) goto failed;
    }

    /* Reclaim memory allocated by HDF5 for vlen strings in data_out */
    r = H5Dvlen_reclaim(dtype, sid, H5P_DEFAULT, data_out);
    if(r<0) goto failed;

    fprintf(stdout, "Success!\n");

    return_code = 0;

    failed:

    for(i=0; i<SIZE; i++) free(data[i]);
    if(dtype>0) H5Tclose(dtype);
    if(dset>0)  H5Dclose(dset);
    if(sid>0)   H5Sclose(sid);
    if(plist>0) H5Pclose(plist);
    if(fid>0)   H5Fclose(fid);

    return return_code;
}
