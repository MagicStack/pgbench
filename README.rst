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

Run

.. code-block:: sh

    $ python3 -m venv pgbench-venv
    $ source pgbench-venv/bin/activate
    (pgbench-venv) $ pip install -r requirements.txt
    (pgbench-venv) $ make


The benchmarks can then be ran with ``./pgbench``.  Use
``./pgbench --help`` for various options, including selective benchmark
running.
