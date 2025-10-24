/*
    Copyright (C) 2010-2016  Francesc Alted
    http://blosc.org
    License: MIT (see LICENSE.txt)

    Filter program that allows the use of the Blosc filter in HDF5.

    This is based on the LZF filter interface (http://h5py.alfven.org)
    by Andrew Collette.

*/


#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "hdf5.h"
#include "blosc_filter.h"

#if defined(__GNUC__)
#define PUSH_ERR(func, minor, str, ...) H5Epush(H5E_DEFAULT, __FILE__, func, __LINE__, H5E_ERR_CLS, H5E_PLINE, minor, str, ##__VA_ARGS__)
#elif defined(_MSC_VER)
#define PUSH_ERR(func, minor, str, ...) H5Epush(H5E_DEFAULT, __FILE__, func, __LINE__, H5E_ERR_CLS, H5E_PLINE, minor, str, __VA_ARGS__)
#else
/* This version is portable but it's better to use compiler-supported
   approaches for handling the trailing comma issue when possible. */
#define PUSH_ERR(func, minor, ...) H5Epush(H5E_DEFAULT, __FILE__, func, __LINE__, H5E_ERR_CLS, H5E_PLINE, minor, __VA_ARGS__)
#endif	/* defined(__GNUC__) */


size_t blosc_filter(unsigned flags, size_t cd_nelmts,
                    const unsigned cd_values[], size_t nbytes,
                    size_t* buf_size, void** buf);

herr_t blosc_set_local(hid_t dcpl, hid_t type, hid_t space);


/* Register the filter, passing on the HDF5 return value */
int register_blosc(char **version, char **date){

    int retval;

    H5Z_class_t filter_class = {
        H5Z_CLASS_T_VERS,
        (H5Z_filter_t)(FILTER_BLOSC),
        1, 1,
        "blosc",
        NULL,
        (H5Z_set_local_func_t)(blosc_set_local),
        (H5Z_func_t)(blosc_filter)
    };

    retval = H5Zregister(&filter_class);
    if(retval<0){
        PUSH_ERR("register_blosc", H5E_CANTREGISTER, "Can't register Blosc filter");
    }
    if (version != NULL && date != NULL) {
        *version = strdup(BLOSC_VERSION_STRING);
        *date = strdup(BLOSC_VERSION_DATE);
    }
    return 1; /* lib is available */
}

/*  Filter setup.  Records the following inside the DCPL:

    1. If version information is not present, set slots 0 and 1 to the filter
       revision and Blosc version, respectively.

    2. Compute the type size in bytes and store it in slot 2.

    3. Compute the chunk size in bytes and store it in slot 3.
*/
herr_t blosc_set_local(hid_t dcpl, hid_t type, hid_t space) {

  int ndims;
  int i;
  herr_t r;

  unsigned int typesize, chunksize, basetypesize;
  hsize_t chunkdims[32];
  unsigned int flags;
  size_t cd_nelmts = 8;
  /* 
   * cd_values[0] = hdf5-blosc format version
   * cd_values[1] = blosc format version
   * cd_values[2] = typesize
   * cd_values[3] = uncompressed chunk size (unused)
   * cd_values[4] = compression level
   * cd_values[5] = 0: shuffle not active, 1: shuffle active
   * cd_values[6] = compressor, e.g. BLOSC_BLOSCLZ
   * cd_values[7] = unused
   */
  unsigned int cd_values[] = {0, 0, 0, 0, 0, 0, 0, 0};
  hid_t super_type;
  H5T_class_t classt;

  r = H5Pget_filter_by_id(
    dcpl, FILTER_BLOSC, &flags, &cd_nelmts, cd_values, 0, NULL, NULL
  );
  if (r < 0) return -1;

  if (cd_nelmts < 4) cd_nelmts = 4;  /* First 4 slots reserved. */

  /* Set Blosc info in first two slots */
  cd_values[0] = FILTER_BLOSC_VERSION;
  cd_values[1] = BLOSC_VERSION_FORMAT;

  ndims = H5Pget_chunk(dcpl, 32, chunkdims);
  if (ndims < 0) return -1;
  if (ndims > 32) {
    PUSH_ERR("blosc_set_local", H5E_CALLBACK, "Chunk rank exceeds limit");
    return -1;
  }

  typesize = H5Tget_size(type);
  if (typesize == 0) return -1;
  /* Get the size of the base type, even for ARRAY types */
  classt = H5Tget_class(type);
  if (classt == H5T_NO_CLASS) return -1;
  if (classt == H5T_ARRAY) {
    /* Get the array base component */
    super_type = H5Tget_super(type);
    basetypesize = H5Tget_size(super_type);
    /* Release resources */
    H5Tclose(super_type);
  } else {
    basetypesize = typesize;
  }

  /* Limit large typesizes (they are pretty expensive to shuffle
     and, in addition, Blosc does not handle typesizes larger than
     255 bytes). */
  if (basetypesize > BLOSC_MAX_TYPESIZE) basetypesize = 1;
  cd_values[2] = basetypesize;

  /* Get the size of the chunk. This is unused by blosc_filter().
     It is retained for backward compatibility.
  */
  chunksize = typesize;
  for (i = 0; i < ndims; i++) {
    chunksize *= chunkdims[i];
  }
  cd_values[3] = chunksize;

#ifdef BLOSC_DEBUG
  fprintf(stderr, "Blosc: typesize=%d; chunksize=%d\n",
          typesize, chunksize);
#endif

  r = H5Pmodify_filter(dcpl, FILTER_BLOSC, flags, cd_nelmts, cd_values);
  if (r < 0) return -1;

  return 1;
}


/* The filter function */
size_t blosc_filter(unsigned flags, size_t cd_nelmts,
                    const unsigned cd_values[], size_t nbytes,
                    size_t* buf_size, void** buf) {

  void* outbuf = NULL;
  int status = 0;                /* Return code from Blosc routines */
  size_t typesize;
  size_t outbuf_size;
  int clevel = 5;                /* Compression level default */
  int doshuffle = 1;             /* Shuffle default */
  int compcode;                  /* Blosc compressor */
  int code;
  const char* compname = "blosclz";    /* The compressor by default */
  const char* complist;
  char errmsg[256];

  assert(cd_nelmts >= 4);
  assert(cd_values[0] == FILTER_BLOSC_VERSION);
  assert(cd_values[1] == BLOSC_VERSION_FORMAT);
  assert(nbytes > 0);
  assert(*buf_size >= nbytes);

  /* Filter params that are always set */
  typesize = cd_values[2];      /* The datatype size */
  assert(typesize > 0 && typesize <= BLOSC_MAX_TYPESIZE);
  /* Optional params */
  if (cd_nelmts >= 5) {
    clevel = cd_values[4];        /* The compression level */
  }
  if (cd_nelmts >= 6) {
    doshuffle = cd_values[5];  /* BLOSC_SHUFFLE, BLOSC_BITSHUFFLE */
    /* bitshuffle is only meant for production in >= 1.8.0 */
#if ((BLOSC_VERSION_MAJOR <= 1) && (BLOSC_VERSION_MINOR < 8))
    if (doshuffle == BLOSC_BITSHUFFLE) {
      PUSH_ERR("blosc_filter", H5E_CALLBACK,
               "this Blosc library version is not supported.  Please update to >= 1.8");
      goto failed;
    }
#endif
  }
  if (cd_nelmts >= 7) {
    compcode = cd_values[6];     /* The Blosc compressor used */
    /* Check that we actually have support for the compressor code */
    complist = blosc_list_compressors();
    code = blosc_compcode_to_compname(compcode, &compname);
    if (code == -1) {
      PUSH_ERR("blosc_filter", H5E_CALLBACK,
               "this Blosc library does not have support for "
                 "the '%s' compressor, but only for: %s",
               compname, complist);
      goto failed;
    }
  }

  /* We're compressing */
  if (!(flags & H5Z_FLAG_REVERSE)) {

    /* Allocate an output buffer exactly as long as the input data; if
       the result is larger, we simply return 0.  The filter is flagged
       as optional, so HDF5 marks the chunk as uncompressed and
       proceeds.
    */

    outbuf_size = nbytes;

#ifdef BLOSC_DEBUG
    fprintf(stderr, "Blosc: Compress %zd bytes chunk (typesize=%d)\n", 
            nbytes, typesize);
#endif

    outbuf = malloc(nbytes);

    if (outbuf == NULL) {
      PUSH_ERR("blosc_filter", H5E_CALLBACK,
               "Can't allocate compression buffer");
      goto failed;
    }

    blosc_set_compressor(compname);
    status = blosc_compress(clevel, doshuffle, typesize, nbytes,
                            *buf, outbuf, nbytes);
    if (status == 0) goto failed;  /* compressed size > input size. This is OK. */
    if (status < 0) {
      /* Internal error */
      PUSH_ERR("blosc_filter", H5E_CALLBACK, "Blosc compression error");
      goto failed;
    }
    assert((size_t)status <= nbytes);

    /* We're decompressing */
  } else {
    /* declare dummy variables */
    size_t cbytes, blocksize;

    /* Extract the exact outbuf_size from the buffer header.
     *
     * NOTE: cd_values[3] contains the uncompressed chunk size.
     * It should not be used in general cases since other filters in the 
     * pipeline can modify the buffer size.
     */
    blosc_cbuffer_sizes(*buf, &outbuf_size, &cbytes, &blocksize);
    assert(cbytes == nbytes);

#ifdef BLOSC_DEBUG
    fprintf(stderr,
            "Blosc: Decompress %zd bytes compressed chunk into %zd bytes buffer\n",
            nbytes, outbuf_size);
#endif

    outbuf = malloc(outbuf_size);

    if (outbuf == NULL) {
      PUSH_ERR("blosc_filter", H5E_CALLBACK, "Can't allocate decompression buffer");
      goto failed;
    }

    status = blosc_decompress(*buf, outbuf, outbuf_size);
    if (status <= 0) {    /* decompression failed */
      PUSH_ERR("blosc_filter", H5E_CALLBACK, "Blosc decompression error");
      goto failed;
    }

  } /* compressing vs decompressing */

  assert(status > 0);
  assert(status <= outbuf_size);
  /* Compression successful */
  free(*buf);
  *buf = outbuf;
  *buf_size = outbuf_size;
  return status;  /* Size of compressed/decompressed data */

  failed:
  /* Note: we will reach this when compressed size > original size. */
  free(outbuf);
  return 0;

} /* End filter function */
