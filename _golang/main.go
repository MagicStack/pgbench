package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	"gopkg.in/alecthomas/kingpin.v2"
	"io/ioutil"
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"
)

type ReportFunc func(int64, int64, int64, int64, []int64)
type WorkerFunc func(time.Time, time.Duration, time.Duration,
	string, []interface{},
	*sync.WaitGroup, ReportFunc)

func lib_pq_worker(
	start time.Time, duration time.Duration, timeout time.Duration,
	query string, query_args []interface{}, wg *sync.WaitGroup,
	report ReportFunc) {

	defer wg.Done()

	conninfo := fmt.Sprintf(
		"user=%s dbname=%s host=%s port=%d sslmode=disable",
		*pguser, *pgdatabase, *pghost, *pgport)

	db, err := sql.Open("postgres", conninfo)
	if err != nil {
		log.Fatal(err)
	}

	latency_stats := make([]int64, timeout/1000/10)
	min_latency := int64(math.MaxInt64)
	max_latency := int64(0)
	queries := int64(0)
	nrows := int64(0)

	var record []interface{}
	var recordPtr []interface{}
	var colcount int

	for time.Since(start) < duration || duration == 0 {
		req_start := time.Now()
		rows, err := db.Query(query, query_args...)
		if err != nil {
			log.Fatal(err)
		}

		if cap(record) == 0 {
			columns, err := rows.Columns()
			if err != nil {
				log.Fatal(err)
			}

			colcount = len(columns)
			record = make([]interface{}, colcount)
			recordPtr = make([]interface{}, colcount)

			for i, _ := range record {
				recordPtr[i] = &record[i]
			}
		}

		havemore := rows.Next()
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
		for havemore {
			nrows += 1
			err := rows.Scan(recordPtr...)
			if err != nil {
				log.Fatal(err)
			}

			for i := 0; i < colcount; i += 1 {
				val := record[i]
				_, _ = val.([]byte)
			}

			havemore = rows.Next()
			err = rows.Err()
			if err != nil {
				log.Fatal(err)
			}
		}
		req_time := time.Since(req_start).Nanoseconds() / 1000 / 10
		latency_stats[req_time] += 1
		if req_time > max_latency {
			max_latency = req_time
		}
		if req_time < min_latency {
			min_latency = req_time
		}
		queries += 1
		if duration == 0 {
			break
		}
	}

	db.Close()
	if report != nil {
		report(queries, nrows, min_latency, max_latency, latency_stats)
	}
}

func pgx_worker(
	start time.Time, duration time.Duration, timeout time.Duration,
	query string, query_args []interface{}, wg *sync.WaitGroup,
	report ReportFunc) {

	defer wg.Done()

	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     *pghost,
		Port:     uint16(*pgport),
		Database: *pgdatabase,
		User:     *pguser,
	})
	if err != nil {
		log.Fatal(err)
	}

	latency_stats := make([]int64, timeout/1000/10)
	min_latency := int64(math.MaxInt64)
	max_latency := int64(0)
	queries := int64(0)
	nrows := int64(0)

	fixed_query_args := make([]interface{}, len(query_args))
	for i, arg := range query_args {
		fixed_query_args[i] = fmt.Sprintf("%v", arg)
	}

	_, err = db.Prepare("testquery", query)
	if err != nil {
		log.Fatal(err)
	}

	for time.Since(start) < duration || duration == 0 {
		req_start := time.Now()
		rows, err := db.Query("testquery", fixed_query_args...)
		if err != nil {
			log.Fatal(err)
		}

		havemore := rows.Next()
		err = rows.Err()
		if err != nil {
			log.Fatal(err)
		}
		for havemore {
			nrows += 1
			_, err = rows.Values()
			if err != nil {
				log.Fatal(err)
			}

			havemore = rows.Next()
			err = rows.Err()
			if err != nil {
				log.Fatal(err)
			}
		}
		req_time := time.Since(req_start).Nanoseconds() / 1000 / 10
		latency_stats[req_time] += 1
		if req_time > max_latency {
			max_latency = req_time
		}
		if req_time < min_latency {
			min_latency = req_time
		}
		queries += 1
		if duration == 0 {
			break
		}
	}

	db.Close()
	if report != nil {
		report(queries, nrows, min_latency, max_latency, latency_stats)
	}
}

var (
	app = kingpin.New(
		"golang-pg-bench",
		"PostgreSQL driver benchmark for Go.")

	concurrency = app.Flag(
		"concurrency", "number of concurrent connections").Default("10").Int()

	duration = app.Flag(
		"duration", "duration of test in seconds").Default("30").Int()

	timeout = app.Flag(
		"timeout", "server timeout in seconds").Default("2").Int()

	warmup_time = app.Flag(
		"warmup-time", "duration of warmup period for each benchmark in seconds").Default("5").Int()

	output_format = app.Flag(
		"output-format", "result output format").Default("text").Enum("text", "json")

	pghost = app.Flag(
		"pghost", "PostgreSQL server host").Default("127.0.0.1").String()

	pgport = app.Flag(
		"pgport", "PostgreSQL server port").Default("5432").Int()

	pguser = app.Flag(
		"pguser", "PostgreSQL server user").Default("postgres").String()

	pgdatabase = app.Flag(
		"pgdatabase", "database to connect to").Default("postgres").String()

	driver = app.Arg(
		"driver", "driver implementation to use").Required().Enum("libpq", "pgx")

	queryfile = app.Arg(
		"queryfile", "file to read benchmark query information from").Required().String()
)

type QueryInfo struct {
	Setup    string
	Teardown string
	Query    string
	Args     []interface{}
}

func main() {
	kingpin.MustParse(app.Parse(os.Args[1:]))

	runtime.GOMAXPROCS(1)

	duration, err := time.ParseDuration(fmt.Sprintf("%vs", *duration))
	if err != nil {
		log.Fatal(err)
	}
	timeout, err := time.ParseDuration(fmt.Sprintf("%vs", *timeout))
	if err != nil {
		log.Fatal(err)
	}
	warmup_time, err := time.ParseDuration(fmt.Sprintf("%vs", *warmup_time))
	if err != nil {
		log.Fatal(err)
	}

	var queryf *os.File

	if *queryfile == "-" {
		queryf = os.Stdin
	} else {
		queryf, err = os.Open(*queryfile)
		if err != nil {
			log.Fatal(err)
		}
	}

	querydata_json, err := ioutil.ReadAll(queryf)
	if err != nil {
		log.Fatal(err)
	}

	var querydata QueryInfo
	json.Unmarshal(querydata_json, &querydata)

	queries := int64(0)
	rows := int64(0)
	min_latency := int64(math.MaxInt64)
	max_latency := int64(0)
	latency_stats := make([]int64, timeout/1000/10)

	report := func(t_queries int64, t_rows int64,
		t_min_latency int64, t_max_latency int64, t_latency_stats []int64) {

		if t_min_latency < min_latency {
			min_latency = t_min_latency
		}

		if t_max_latency > max_latency {
			max_latency = t_max_latency
		}

		for i, elem := range t_latency_stats {
			latency_stats[i] += elem
		}

		queries += t_queries
		rows += t_rows
	}

	var worker WorkerFunc

	if *driver == "pgx" {
		worker = pgx_worker
	} else {
		worker = lib_pq_worker
	}

	do_run := func(
		worker WorkerFunc,
		query string, query_args []interface{},
		concurrency int,
		run_duration time.Duration,
		report ReportFunc) time.Duration {

		var wg sync.WaitGroup
		wg.Add(concurrency)

		start := time.Now()

		for i := 0; i < concurrency; i += 1 {
			go worker(start, run_duration, timeout, query,
				query_args, &wg, report)
		}

		wg.Wait()

		return time.Since(start)
	}

	if querydata.Setup != "" {
		do_run(lib_pq_worker, querydata.Setup, nil, 1, 0, nil)
	}

	if warmup_time > 0 {
		do_run(worker, querydata.Query, querydata.Args, *concurrency,
			warmup_time, nil)
	}

	duration = do_run(worker, querydata.Query, querydata.Args, *concurrency,
		duration, report)

	if querydata.Teardown != "" {
		do_run(lib_pq_worker, querydata.Teardown, nil, 1, 0, nil)
	}

	data := make(map[string]interface{})
	data["queries"] = queries
	data["rows"] = rows
	data["min_latency"] = min_latency
	data["max_latency"] = max_latency
	data["latency_stats"] = latency_stats
	data["duration"] = duration.Seconds()

	json, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(json))
}
