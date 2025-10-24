/*
    Copyright (C) 2025  Francesc Alted
    http://blosc.org
    License: MIT (see LICENSE.txt)

    Test for compound datatypes (H5T_COMPOUND).
    This triggers a special case when the compound datatype is larger than
    BLOSC_MAX_TYPESIZE (255 bytes).

    To compile this program:

    h5cc blosc_filter.c test_compound.c -o test_compound -lblosc -lpthread

    To run:

    $ ./test_compound 255  # <= BLOSC_MAX_TYPESIZE
    Blosc version info: 1.21.7.dev ($Date:: 2024-06-24 #$)
    Success!
    $ h5ls -v test_compound.h5
    Opened "test_compound.h5" with sec2 driver.
    dset                     Dataset {100000/100000}
        Location:  1:800
        Links:     1
        Chunks:    {1000} 255000 bytes
        Storage:   25500000 logical bytes, 450900 allocated bytes, 5655.36% utilization
        Filter-0:  blosc-32001 OPT {2, 2, 255, 255000}
        Type:      struct {
                    "field_0"          +0    native unsigned char
                    [...]
                    "field_254"        +254  native unsigned char
                } 255 bytes

    $ ./test_compound 256  # > BLOSC_MAX_TYPESIZE
    Blosc version info: 1.21.7.dev ($Date:: 2024-06-24 #$)
    Success!
    $ h5ls -v test_compound.h5
    Opened "test_compound.h5" with sec2 driver.
    dset                     Dataset {100000/100000}
        Location:  1:800
        Links:     1
        Chunks:    {1000} 256000 bytes
        Storage:   25600000 logical bytes, 157400 allocated bytes, 16264.29% utilization
        Filter-0:  blosc-32001 OPT {2, 2, 1, 256000}
        Type:      struct {
                    "field_0"          +0    native unsigned char
                    [...]
                    "field_255"        +255  native unsigned char
                } 256 bytes

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

int main(int argc, char **argv){

    static unsigned char *data = NULL;
    static unsigned char *data_out = NULL;
    int struct_size = 0;
    const hsize_t shape[] = SHAPE;
    const hsize_t chunkshape[] = CHUNKSHAPE;
    char *version, *date;
    int r, i;
    int return_code = 1;

    hid_t fid = 0, sid = 0, dset = 0, plist = 0, dtype = 0;

    if (argc == 2) struct_size = atoi(argv[1]);
    if (struct_size < 1) {
        fprintf(stderr, "Usage: %s <struct size in bytes>\n", argv[0]);
        goto failed;
    }

    data = malloc(SIZE * struct_size);
    if(data == NULL) goto failed;
    data_out = malloc(SIZE * struct_size);
    if(data_out == NULL) goto failed;
    for (i=0; i<SIZE * struct_size; i++){
        data[i] = i % 256;
    }

    /* Register the filter with the library */
    r = register_blosc(&version, &date);
    if(r<0) goto failed;
    printf("Blosc version info: %s (%s)\n", version, date);
    free(version);
    free(date);

    sid = H5Screate_simple(NDIM, shape, NULL);
    if(sid<0) goto failed;

    fid = H5Fcreate("test_compound.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    if(fid<0) goto failed;

    plist = H5Pcreate(H5P_DATASET_CREATE);
    if(plist<0) goto failed;

    /* Chunked layout required for filters */
    r = H5Pset_chunk(plist, NDIM, chunkshape);
    if(r<0) goto failed;

    r = H5Pset_filter(plist, FILTER_BLOSC, H5Z_FLAG_OPTIONAL, 0, NULL);
    if(r<0) goto failed;

    /* Define H5T_COMPOUND datatype */
    dtype = H5Tcreate(H5T_COMPOUND, struct_size);
    if(dtype<0) goto failed;
    for (i=0; i<struct_size; i++) {
        char field_name[20];
        snprintf(field_name, sizeof(field_name), "field_%d", i);
        r = H5Tinsert(dtype, field_name, i, H5T_NATIVE_UCHAR);
        if (r<0) goto failed;
    }

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

    if(data)     free(data);
    if(data_out) free(data_out);
    if(dtype>0)  H5Tclose(dtype);
    if(dset>0)   H5Dclose(dset);
    if(sid>0)    H5Sclose(sid);
    if(plist>0)  H5Pclose(plist);
    if(fid>0)    H5Fclose(fid);

    return return_code;
}
