#![cfg_attr(feature="b_netsoup", feature(conservative_impl_trait, plugin))]
#![cfg_attr(feature="b_netsoup", plugin(tarpc_plugins))]

#[macro_use]
extern crate clap;

extern crate slog;
extern crate slog_term;

extern crate rand;

#[cfg(any(feature="b_mssql", feature="b_netsoup"))]
extern crate futures;
#[cfg(any(feature="b_mssql", feature="b_netsoup"))]
extern crate tokio_core;

#[cfg(feature="b_mssql")]
extern crate futures_state_stream;
#[cfg(feature="b_mssql")]
extern crate tiberius;

// Both MySQL *and* PostgreSQL use r2d2, but compilation fails with both feature flags active if we
// specify it twice.
#[cfg(any(feature="b_mysql", feature="b_postgresql", feature="b_hybrid"))]
extern crate r2d2;

#[cfg(any(feature="b_mysql", feature="b_hybrid"))]
#[macro_use]
extern crate mysql;
#[cfg(any(feature="b_mysql", feature="b_hybrid"))]
extern crate r2d2_mysql;

#[cfg(feature="b_postgresql")]
extern crate postgres;
#[cfg(feature="b_postgresql")]
extern crate r2d2_postgres;

extern crate distributary;

#[cfg(feature="b_netsoup")]
extern crate tarpc;

#[cfg(any(feature="b_memcached", feature="b_hybrid"))]
extern crate memcached;

extern crate hdrsample;

extern crate spmc;

mod targets;
mod exercise;

use std::time;

#[cfg_attr(rustfmt, rustfmt_skip)]
const BENCH_USAGE: &'static str = "\
EXAMPLES:
  vote soup://
  vote netsoup://127.0.0.1:7777
  vote memcached://127.0.0.1:11211
  vote mssql://server=tcp:127.0.0.1,1433;username=user;pwd=pwd;/database
  vote mysql://user@127.0.0.1/database
  vote postgresql://user@127.0.0.1/database
  vote hybrid://mysql=user@127.0.0.1/database,memcached=127.0.0.1:11211";

fn main() {
    use clap::{Arg, App};
    let mut backends = vec!["soup"];
    if cfg!(feature = "b_mssql") {
        backends.push("mssql");
    }
    if cfg!(feature = "b_mysql") {
        backends.push("mysql");
    }
    if cfg!(feature = "b_postgresql") {
        backends.push("postgresql");
    }
    if cfg!(feature = "b_memcached") {
        backends.push("memcached");
    }
    if cfg!(feature = "b_netsoup") {
        backends.push("netsoup");
    }
    if cfg!(feature = "b_hybrid") {
        backends.push("hybrid");
    }
    let backends = format!("Which database backend to use [{}]://<params>",
                           backends.join(", "));

    let args = App::new("vote")
        .version("0.1")
        .about("Benchmarks user-curated news aggregator throughput for different storage \
                backends.")
        .arg(Arg::with_name("avg")
            .long("avg")
            .takes_value(false)
            .help("compute average throughput at the end of benchmark"))
        .arg(Arg::with_name("cdf")
            .short("c")
            .long("cdf")
            .takes_value(false)
            .help("produce a CDF of recorded latencies for each client at the end"))
        .arg(Arg::with_name("stage")
            .short("s")
            .long("stage")
            .takes_value(false)
            .help("stage execution such that all writes are performed before all reads"))
        .arg(Arg::with_name("ngetters")
            .short("g")
            .long("getters")
            .value_name("N")
            .default_value("1")
            .help("Number of GET clients to start"))
        .arg(Arg::with_name("narticles")
            .short("a")
            .long("articles")
            .value_name("N")
            .default_value("100000")
            .help("Number of articles to prepopulate the database with"))
        .arg(Arg::with_name("runtime")
            .short("r")
            .long("runtime")
            .value_name("N")
            .default_value("60")
            .help("Benchmark runtime in seconds"))
        .arg(Arg::with_name("migrate")
            .short("m")
            .long("migrate")
            .value_name("N")
            .help("Perform a migration after this many seconds")
            .conflicts_with("stage"))
        .arg(Arg::with_name("BACKEND")
            .index(1)
            .help(&backends)
            .required(true))
        .after_help(BENCH_USAGE)
        .get_matches();

    let avg = args.is_present("avg");
    let cdf = args.is_present("cdf");
    let stage = args.is_present("stage");
    let dbn = args.value_of("BACKEND").unwrap();
    let runtime = time::Duration::from_secs(value_t_or_exit!(args, "runtime", u64));
    let migrate_after = args.value_of("migrate")
        .map(|_| value_t_or_exit!(args, "migrate", u64))
        .map(time::Duration::from_secs);
    let ngetters = value_t_or_exit!(args, "ngetters", usize);
    let narticles = value_t_or_exit!(args, "narticles", isize);
    assert!(ngetters > 0);
    assert!(!dbn.is_empty());

    if let Some(ref migrate_after) = migrate_after {
        assert!(migrate_after < &runtime);
    }

    let mut config = exercise::RuntimeConfig::new(ngetters, narticles, runtime);
    config.produce_cdf(cdf);
    if stage {
        config.put_then_get();
    }
    if let Some(migrate_after) = migrate_after {
        config.perform_migration_at(migrate_after);
    }

    // setup db
    println!("Attempting to connect to database using {}", dbn);
    let mut dbn = dbn.splitn(2, "://");
    let (put_stats, get_stats) = match dbn.next().unwrap() {
        // soup://
        "soup" => exercise::launch(targets::soup::make(dbn.next().unwrap(), ngetters), config),
        // mssql://server=tcp:127.0.0.1,1433;user=user;pwd=password/bench_mssql
        #[cfg(feature="b_mssql")]
        "mssql" => exercise::launch(targets::mssql::make(dbn.next().unwrap(), ngetters), config),
        // mysql://soup@127.0.0.1/bench_mysql
        #[cfg(feature="b_mysql")]
        "mysql" => exercise::launch(targets::mysql::make(dbn.next().unwrap(), ngetters), config),
        // hybrid://mysql=soup@127.0.0.1/bench_mysql,memcached=127.0.0.1:11211
        #[cfg(feature="b_hybrid")]
        "hybrid" => {
            let mut split_dbn = dbn.next().unwrap().splitn(2, ",");
            let mysql_dbn = &split_dbn.next().unwrap()[6..];
            let memcached_dbn = &split_dbn.next().unwrap()[10..];
            exercise::launch(targets::hybrid::make(memcached_dbn, mysql_dbn, ngetters),
                             config)
        }
        // postgresql://soup@127.0.0.1/bench_psql
        #[cfg(feature="b_postgresql")]
        "postgresql" => {
            exercise::launch(targets::postgres::make(dbn.next().unwrap(), ngetters),
                             config)
        }
        // memcached://127.0.0.1:11211
        #[cfg(feature="b_memcached")]
        "memcached" => {
            exercise::launch(targets::memcached::make(dbn.next().unwrap(), ngetters),
                             config)
        }
        // netsoup://127.0.0.1:7777
        #[cfg(feature="b_netsoup")]
        "netsoup" => {
            exercise::launch(targets::netsoup::make(dbn.next().unwrap(), ngetters),
                             config)
        }
        // garbage
        t => {
            panic!("backend not supported -- make sure you compiled with --features b_{}",
                   t)
        }
    };

    print_stats("PUT", &put_stats.pre, avg);
    for (i, s) in get_stats.iter().enumerate() {
        print_stats(format!("GET{}", i), &s.pre, avg);
    }
    if avg {
        let sum = get_stats.iter().fold((0f64, 0usize), |(tot, count), stats| {
            // TODO: do we *really* want an average of averages?
            let (sum, num) = stats.pre.sum_len();
            (tot + sum, count + num)
        });
        println!("avg GET: {:.2}", sum.0 as f64 / sum.1 as f64);
    }

    if migrate_after.is_some() {
        print_stats("PUT+", &put_stats.post, avg);
        for (i, s) in get_stats.iter().enumerate() {
            print_stats(format!("GET{}+", i), &s.post, avg);
        }
        if avg {
            let sum = get_stats.iter().fold((0f64, 0usize), |(tot, count), stats| {
                // TODO: do we *really* want an average of averages?
                let (sum, num) = stats.pre.sum_len();
                (tot + sum, count + num)
            });
            println!("avg GET+: {:.2}", sum.0 as f64 / sum.1 as f64);
        }
    }
}

fn print_stats<S: AsRef<str>>(desc: S, stats: &exercise::BenchmarkResult, avg: bool) {
    if let Some(perc) = stats.cdf_percentiles() {
        for (v, p, _, _) in perc {
            println!("percentile {} {:.2} {:.2}", desc.as_ref(), v, p);
        }
    }
    if avg {
        println!("avg {}: {:.2}", desc.as_ref(), stats.avg_throughput());
    }
}
