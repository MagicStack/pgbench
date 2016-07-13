PostgreSQL Client Driver Performance Benchmarking Toolbench
===========================================================

This is a collection of scripts intended to benchmark the efficiency of
various implementations of PostgreSQL client drivers.


Installation and Use
--------------------

Install the following:

- git
- NodeJS
- Go 1.6
- Python 3
- Numpy

Prepare the toolbench by running ``make``.  This will download and build
the driver implementations to prepare for benchmarks.

The benchmarks can then be ran with ``./pgbench``.  Use
``./pgbench --help`` for various options, including selective benchmark
running.
