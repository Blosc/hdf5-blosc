Blosc filter for HDF5
=====================

:Travis CI: |travis|
:And...: |powered|

.. |travis| image:: https://travis-ci.org/Blosc/hdf5.png?branch=master
        :target: https://travis-ci.org/Blosc/hdf5

.. |powered| image:: http://b.repl.ca/v1/Powered--By-Blosc-blue.png
        :target: https://blosc.org

This is an example of filter for HDF5 that uses the Blosc compressor.

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
