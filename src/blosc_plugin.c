/*
 * Dynamically loaded filter plugin for HDF5 blosc filter.
 *
 * Author: Kiyoshi Masui <kiyo@physics.ubc.ca>
 * Created: 2014
 *
 * For compiling, use:
 * $ h5cc -fPIC -shared blosc_plugin.c blosc_filter.c -o libH5Zblosc.so -lblosc
 *
 */

#include "blosc_filter.h"
#include "H5PLextern.h"

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_FILTER; }
const void* H5PLget_plugin_info(void) { return BLOSC_FILTER; }
