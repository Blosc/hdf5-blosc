Blosc filter for HDF5
=====================

This is an example of filter for HDF5 that uses Blosc.

You need to be a bit careful before using this filter because you
should not activate the shuffle right in HDF5, but rather from Blosc
itself.  This is because Blosc uses an SIMD shuffle internally which
is much faster.

You can find the filter itself at 'src/blosc_filter.c' as well as an
example of use in 'src/example.c'.  Also, you can use blosc as an HDF5
plugin; see 'src/blosc_pluing.c for details.


Acknowledgments
===============

See THANKS.rst.


----

  **Enjoy data!**


