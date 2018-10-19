#!/usr/bin/env node
//
// Copyright (c) 2016 MagicStack Inc.
// All rights reserved.
//
// See LICENSE for details.
//


"use strict";

const fs = require('fs');
const path = require('path');
const process = require('process');

const argparse = require('argparse');
const pg = require('pg');
const pgcopy = require('pg-copy-streams').from;
const tspostgres = require('ts-postgres');
const csvwriter = require('csv-write-stream')


function _connect(driverName, args, callback) {
    let driver = null;

    if (driverName == 'pg-js') {
        driver = pg;
    } else if (driverName == 'pg-native') {
        driver = pg.native;
    } else if (driverName == 'ts-postgres') {
        driver = {
            'Client': function(config) {
                var client = new tspostgres.Client(config);
                return {
                    connect: function(callback) {
                        client.connect()
                            .then(function() { callback(null); })
                            .catch(function(err) { callback(err); });
                    },
                    end: function() {
                        client.end();
                    },
                    query: function(stmt, cb) {
                        client.query(stmt)
                            .then(
                                function(result) { cb(null, result); }
                            )
                            .catch(
                                function(err) {
                                    cb(err, null);
                                }
                            );
                    }
                }
            }
        };
    } else {
        throw new Error('unexected driver: ' + driverName)
    }

    var client = new driver.Client({
        'user': args.pguser,
        'database': 'postgres',
        'host': args.pghost,
        'port': args.pgport
    });

    client.connect(function(err) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, client);
        }
    });
}


// Return the current timer value in microseconds
function _now() {
    var [s, ns] = process.hrtime();
    return s * 1000000 + Math.round(ns / 1000);
}


function runner(args, querydata) {
    var duration = args.duration;
    var timeout_in_us = args.timeout * 1000000;

    var reported = 0;
    var min_latency = Infinity;
    var max_latency = 0.0;
    var queries = 0;
    var rows = 0;
    var latency_stats = null;
    var query = querydata.query;
    var query_args = querydata.args;
    var setup_query = querydata.setup;
    var teardown_query = querydata.teardown;

    function _report_results(t_queries, t_rows, t_latency_stats,
                             t_min_latency, t_max_latency, run_start) {
        queries += t_queries;
        rows += t_rows;

        if (t_max_latency > max_latency) {
            max_latency = t_max_latency;
        }

        if (t_min_latency < min_latency) {
            min_latency = t_min_latency;
        }

        if (latency_stats === null) {
            latency_stats = t_latency_stats;
        } else {
            for (var i = 0; i < latency_stats.length; i += 1) {
                latency_stats[i] += t_latency_stats[i];
            }
        }

        reported += 1;

        if (reported == args.concurrency) {
            var run_end = _now();

            var data = {
                'queries': queries,
                'rows': rows,
                'duration': (run_end - run_start) / 1000000,
                'min_latency': min_latency,
                'max_latency': max_latency,
                'latency_stats': Array.prototype.slice.call(latency_stats)
            };

            process.stdout.write(JSON.stringify(data));
        }
    }

    function _do_run(driver, query, query_args, concurrency, run_duration,
                     report, use_prepared_stmt, cb) {
        var run_start = _now();
        var complete = 0;
        var stmt = {text: query, values: query_args};
        var copy = null;

        if (query.startsWith('COPY ')) {
            var m = /COPY (\w+)\s*\(\s*((?:\w+)(?:,\s*\w+)*)\s*\)/.exec(query)
            if (m == null) {
                throw "Could not parse COPY query";
            }

            var rowcount = query_args[0]["count"];
            var row = query_args[0]["row"];
            var rows = Array(rowcount);
            for (var i = 0; i < rowcount; i += 1) {
                rows[i] = row;
            }

            copy = {
                "table": m[1],
                "columns": m[2].split(",").map(v => v.trim()),
                "rows": rows
            };
        }

        if (copy != null && driver !== 'pg-js') {
            cb({code: 3, msg: driver + " does not support COPY"});
            return;
        }

        if (use_prepared_stmt) {
            stmt.name = '_pgbench_query';
        }

        var query_runner = function(err, conn) {
            if (err) {
                throw err;
            }

            var queries = 0;
            var rows = 0;
            var latency_stats = new Float64Array(timeout_in_us / 10);
            var min_latency = Infinity;
            var max_latency = 0.0;
            var duration_in_us = run_duration * 1000000;
            var req_start;
            var req_time;

            var _cb = function(err, result) {
                if (err) {
                    throw err;
                }

                // Request time in tens of microseconds
                req_time = Math.round((_now() - req_start) / 10);

                if (req_time > max_latency) {
                    max_latency = req_time;
                }

                if (req_time < min_latency) {
                    min_latency = req_time;
                }

                latency_stats[req_time] += 1;
                queries += 1;
                if (Array.isArray(result)) {
                    result = result[result.length - 1];
                }

                rows += result.rows.length;

                if (_now() - run_start < duration_in_us) {
                    req_start = _now();
                    conn.query(stmt, _cb);
                } else {
                    conn.end();
                    if (report) {
                        _report_results(queries, rows, latency_stats,
                                        min_latency, max_latency,
                                        run_start);
                    }

                    complete += 1;
                    if (complete == concurrency && cb) {
                        cb();
                    }
                }
            };

            req_start = _now();
            conn.query(stmt, _cb);
        };

        var copy_runner = function(err, conn) {
            if (err) {
                throw err;
            }

            var queries = 0;
            var rows = 0;
            var latency_stats = new Float64Array(timeout_in_us / 10);
            var min_latency = Infinity;
            var max_latency = 0.0;
            var duration_in_us = run_duration * 1000000;
            var req_start;
            var req_time;

            var _start_copy = function(_cb) {
                req_start = _now();
                var csvstream = csvwriter({
                    sendHeaders: false,
                    separator: '\t',
                    headers: copy.columns
                });
                var copystream = conn.query(pgcopy(stmt.text));
                csvstream.pipe(copystream);
                copystream.on('end', _cb);
                copystream.on('error', _cb);
                for (var row of copy.rows) {
                   csvstream.write(row);
                }
                csvstream.end();
            }

            var _cb = function(err, result) {
                if (err) {
                    throw err;
                }

                // Request time in tens of microseconds
                req_time = Math.round((_now() - req_start) / 10);

                if (req_time > max_latency) {
                    max_latency = req_time;
                }

                if (req_time < min_latency) {
                    min_latency = req_time;
                }

                latency_stats[req_time] += 1;
                queries += 1;
                rows += copy.rows.length;

                if (_now() - run_start < duration_in_us) {
                    _start_copy(_cb);
                } else {
                    conn.end();
                    if (report) {
                        _report_results(queries, rows, latency_stats,
                                        min_latency, max_latency,
                                        run_start);
                    }

                    complete += 1;
                    if (complete == concurrency && cb) {
                        cb();
                    }
                }
            };

            _start_copy(_cb);
        };

        var runner = copy != null ? copy_runner : query_runner;

        for (var i = 0; i < concurrency; i += 1) {
            _connect(driver, args, runner);
        }
    }

    function _setup(cb) {
        if (setup_query) {
            function go(queries) {
                var query = queries.shift();
                var next = (query) ? function() { go(queries) } : cb;
                _do_run(args.driver, query, [], 1, 0, false, false, next);
            }
            go(setup_query.split(';'));
        } else {
            if (cb) {
                cb();
            }
        }
    }

    function _exit(err) {
        if (err) {
            console.error(err.msg);
            process.exit(err.code);
        }
    }

    function _teardown(cb) {
        if (teardown_query) {
            _do_run(args.driver, teardown_query, [], 1, 0, false, false, cb);
        } else {
            if (cb) {
                cb();
            }
        }
    }

    function _run() {
        _do_run(args.driver, query, query_args, args.concurrency, duration,
                true, true, (err) => _teardown(() => _exit(err)));
    }

    function _warmup_and_run() {
        if (args.warmup_time) {
            _do_run(args.driver, query, query_args, args.concurrency,
                    args.warmup_time, false, true, _run);
        } else {
            _run();
        }
    }

    _setup(_warmup_and_run);

    return;
}


function main() {
    let parser = argparse.ArgumentParser({
        addHelp: true,
        description: 'async pg driver benchmark [concurrent]'
    })

    parser.addArgument(
        '--concurrency',
        {type: Number, defaultValue: 10,
         help: 'number of concurrent connections'})
    parser.addArgument(
        '--duration',
        {type: Number, defaultValue: 30,
         help: 'duration of test in seconds'})
    parser.addArgument(
        '--timeout',
        {type: Number, defaultValue: 2,
         help: 'server timeout in seconds'})
    parser.addArgument(
        '--warmup-time',
        {type: Number, defaultValue: 5,
         help: 'duration of warmup period for each benchmark in seconds'})
    parser.addArgument(
        '--output-format',
        {type: String, defaultValue: 'text',
         help: 'output format',
         choices: ['text', 'json']})
    parser.addArgument(
        '--pghost',
        {type: String, defaultValue: '127.0.0.1',
         help: 'PostgreSQL server host'})
    parser.addArgument(
        '--pgport',
        {type: Number, defaultValue: 5432,
         help: 'PostgreSQL server port'})
    parser.addArgument(
        '--pguser',
        {type: String, defaultValue: 'postgres',
         help: 'PostgreSQL server user'})
    parser.addArgument(
        'driver',
        {type: String, help: 'driver implementation to use',
         choices: ['pg-js', 'pg-native', 'ts-postgres']})
    parser.addArgument(
        'queryfile',
        {type: String,
         help: 'file to read benchmark query information from'})

    let args = parser.parseArgs();
    let queryfile = null;

    if (args.queryfile == '-') {
        process.stdin.setEncoding('utf8');
        queryfile = process.stdin;
    } else {
        queryfile = fs.createReadStream(args.queryfile);
    }

    let querydata_json = '';

    queryfile.on('data', (chunk) => {querydata_json += chunk});
    queryfile.on('end', () => {
        let querydata = JSON.parse(querydata_json);
        runner(args, querydata);
    });
}


main();
