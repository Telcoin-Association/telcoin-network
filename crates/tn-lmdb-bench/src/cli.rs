//! The command line: arguments parsed by hand (the CLI is small), the progress on stderr, the
//! table on stdout, the JSON to `--out`. Exit 0 on success; 2 on a usage or environment error,
//! refused before any work (a `$TMPDIR` on tmpfs or ramfs, or one a temp dir cannot be made in;
//! an `--out` that cannot be written); 101 when the run itself fails (an engine error, such as a
//! full disk, or a failed check of a workload's own results: a panic, its message on stderr). An
//! interrupted run (a signal) leaves its rep's `tn-lmdb-bench-*` dir under `$TMPDIR`.

use std::path::{Path, PathBuf};

use crate::{memory_fs, run, table, Config, Group, Mode, Tier, ROWS};

/// The usage text `--help` prints.
pub const USAGE: &str = "\
usage: tn-lmdb-bench [--tier quick|default|full] [--rows <pat,..>] [--groups generic,reth]
                     [--sync nosync,durable] [--n N] [--reps K] [--page-size P]
                     [--out FILE] [--list] [--label S]

  --tier       quick (n 100k, 3 reps), default (n 1M, 3 reps), full (n 1M, 7 reps, both syncs);
               default: default
  --rows       row names, or prefixes ending in `*` (e.g. `reth.*`), comma-separated
  --groups     generic, reth (default both)
  --sync       nosync, durable (default nosync; full: both)
  --n          entries in each bulk table (the tier's by default; at least 1000)
  --reps       samples per row, each in a fresh environment (the tier's by default)
  --page-size  the environments' page size (default 4096)
  --out        write the schema-1 JSON report to FILE
  --list       print the rows that would run (name, group, sync) and exit
  --label      a label for the report

Temp dirs go under $TMPDIR; a tmpfs or ramfs TMPDIR is refused (syncs there cost nothing).";

/// What the arguments ask for.
#[derive(Debug)]
pub enum Command {
    /// Print [`USAGE`].
    Help,
    /// Print the (row, mode) pairs of the config, one a line.
    List(Config),
    /// Run the config, and write the JSON to the path if one is given.
    Run(Config, Option<PathBuf>),
}

/// Whether `pattern` names `row`: exactly, or as a prefix ending in `*`.
fn matches(pattern: &str, row: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => row.starts_with(prefix),
        None => pattern == row,
    }
}

/// A comma-separated list's items, empty ones dropped.
fn list(value: &str) -> Vec<&str> {
    value.split(',').map(str::trim).filter(|s| !s.is_empty()).collect()
}

/// Parse the arguments (without the program name) into a [`Command`]; an error is a message for
/// the user.
pub fn parse_args(args: &[String]) -> Result<Command, String> {
    let mut tier = Tier::Default;
    let (mut patterns, mut groups, mut modes) = (Vec::new(), None, None);
    let (mut n, mut reps, mut page_size) = (None, None, 4096u32);
    let (mut out, mut show_list, mut label) = (None, false, String::new());
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) if flag.starts_with("--") => (flag, Some(value.to_owned())),
            _ => (arg.as_str(), None),
        };
        let mut value = || {
            inline
                .clone()
                .or_else(|| args.next().cloned())
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        let number =
            |v: String| v.parse::<usize>().map_err(|_| format!("{flag}: not a number: {v}"));
        match flag {
            "-h" | "--help" => return Ok(Command::Help),
            "--list" => show_list = true,
            "--tier" => {
                tier = match value()?.as_str() {
                    "quick" => Tier::Quick,
                    "default" => Tier::Default,
                    "full" => Tier::Full,
                    other => return Err(format!("unknown tier {other}")),
                }
            }
            "--rows" => patterns.extend(list(&value()?).into_iter().map(str::to_owned)),
            "--groups" => {
                let mut picked = Vec::new();
                for g in list(&value()?) {
                    picked.push(match g {
                        "generic" => Group::Generic,
                        "reth" => Group::Reth,
                        other => return Err(format!("unknown group {other}")),
                    });
                }
                groups = Some(picked);
            }
            "--sync" => {
                let mut picked = Vec::new();
                for m in list(&value()?) {
                    let mode = match m {
                        "nosync" => Mode::NoSync,
                        "durable" => Mode::Durable,
                        other => return Err(format!("unknown sync mode {other}")),
                    };
                    if !picked.contains(&mode) {
                        picked.push(mode);
                    }
                }
                modes = Some(picked);
            }
            "--n" => n = Some(number(value()?)?),
            "--reps" => reps = Some(number(value()?)?),
            "--page-size" => {
                let v = number(value()?)?;
                page_size = u32::try_from(v).map_err(|_| format!("--page-size {v} too large"))?;
            }
            "--out" => out = Some(PathBuf::from(value()?)),
            "--label" => label = value()?,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let (tier_n, tier_reps, tier_modes) = tier.defaults();
    let n = n.unwrap_or(tier_n);
    let reps = reps.unwrap_or(tier_reps);
    let modes = modes.unwrap_or(tier_modes);
    if !(1000..=100_000_000).contains(&n) {
        return Err(format!("--n {n}: give 1000 to 100000000"));
    }
    if !(1..=1000).contains(&reps) {
        return Err(format!("--reps {reps}: give 1 to 1000"));
    }
    if !page_size.is_power_of_two() || !(4096..=65536).contains(&page_size) {
        return Err(format!("--page-size {page_size}: give a power of two in 4096..=65536"));
    }
    if modes.is_empty() {
        return Err("--sync names no mode".to_owned());
    }
    if let Some(p) = patterns.iter().find(|p| !ROWS.iter().any(|r| matches(p, r.name))) {
        return Err(format!("--rows {p} matches no row (see --list)"));
    }
    let rows = ROWS
        .iter()
        .filter(|r| groups.as_ref().is_none_or(|g: &Vec<Group>| g.contains(&r.group)))
        .filter(|r| patterns.is_empty() || patterns.iter().any(|p| matches(p, r.name)))
        .collect();
    let config = Config { tier, n, reps, page_size, modes, rows, label };
    if show_list {
        return Ok(Command::List(config));
    }
    if config.pairs().is_empty() {
        return Err("nothing to run: no selected row runs under the selected sync modes (see \
                    --list)"
            .to_owned());
    }
    Ok(Command::Run(config, out))
}

/// Refuse a temp dir the run cannot use, before any work: one on tmpfs or ramfs (syncs there cost
/// nothing), or one a temp dir cannot be made in (missing, not a directory, not writable), which
/// the first rep would otherwise find by a panic.
fn check_temp_dir(tmp: &Path) -> Result<(), String> {
    if let Some(fs) = memory_fs(tmp) {
        return Err(format!(
            "the temp dir {} is on {fs}: syncs there cost nothing. Set TMPDIR to a directory on a \
             real disk, for this command only.",
            tmp.display()
        ));
    }
    tempfile::Builder::new()
        .prefix("tn-lmdb-bench-")
        .tempdir_in(tmp)
        .and_then(tempfile::TempDir::close)
        .map_err(|e| {
            format!(
                "cannot make a temp dir under {}: {e}. Set TMPDIR to a writable directory.",
                tmp.display()
            )
        })
}

/// The command line: run `args` (without the program name) and return the exit code.
pub fn cli_main(args: Vec<String>) -> i32 {
    let (config, out) = match parse_args(&args) {
        Ok(Command::Help) => {
            println!("{USAGE}");
            return 0;
        }
        Ok(Command::List(config)) => {
            for (row, mode) in config.pairs() {
                println!("{}\t{}\t{}", row.name, row.group.name(), mode.name());
            }
            return 0;
        }
        Ok(Command::Run(config, out)) => (config, out),
        Err(message) => {
            eprintln!("tn-lmdb-bench: {message}\n\n{USAGE}");
            return 2;
        }
    };
    let tmp = std::env::temp_dir();
    if let Err(message) = check_temp_dir(&tmp) {
        eprintln!("tn-lmdb-bench: {message}");
        return 2;
    }
    if let Some(path) = &out {
        // Fail before the run, not after it, on a path that cannot be written.
        if let Err(e) = std::fs::write(path, b"") {
            eprintln!("tn-lmdb-bench: cannot write {}: {e}", path.display());
            return 2;
        }
    }
    eprintln!(
        "tn-lmdb-bench: tier {}, n {}, reps {}, {} rows, temp dirs under {}",
        config.tier.name(),
        config.n,
        config.reps,
        config.pairs().len(),
        tmp.display()
    );
    let start = std::time::Instant::now();
    let report = run(&config, &mut |line| eprintln!("{line}"));
    eprintln!("tn-lmdb-bench: done in {:.1} s", start.elapsed().as_secs_f64());
    print!("{}", table(&report));
    if let Some(path) = &out {
        let json = serde_json::to_string_pretty(&report).expect("a report serializes");
        if let Err(e) = std::fs::write(path, json + "\n") {
            eprintln!("tn-lmdb-bench: cannot write {}: {e}", path.display());
            return 2;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_owned).collect()
    }

    fn config(s: &str) -> Config {
        match parse_args(&args(s)).expect("parses") {
            Command::List(c) | Command::Run(c, _) => c,
            Command::Help => panic!("help"),
        }
    }

    #[test]
    fn list_pairs_are_unique_and_follow_the_modes() {
        let c = config("--list --sync nosync,durable");
        let pairs: Vec<(&str, &str)> = c.pairs().iter().map(|(r, m)| (r.name, m.name())).collect();
        let mut unique = pairs.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), pairs.len());
        assert!(pairs.contains(&("put.seq", "nosync")));
        assert!(!pairs.contains(&("put.seq", "durable")));
        assert!(pairs.contains(&("put.seq.commit", "durable")));
        assert!(pairs.contains(&("reth.block.pages_synced", "durable")));
        // The default is nosync only, so pages_synced is not listed.
        let c = config("--list");
        assert!(c.pairs().iter().all(|(r, m)| *m == Mode::NoSync && r.modes.has(*m)));
        assert_eq!(c.pairs().len(), ROWS.iter().filter(|r| r.modes.has(Mode::NoSync)).count());
    }

    #[test]
    fn tiers_patterns_and_errors() {
        let c = config("--tier quick");
        assert_eq!((c.n, c.reps, c.modes.clone()), (100_000, 3, vec![Mode::NoSync]));
        let c = config("--tier=full --reps 2 --n=5000");
        assert_eq!((c.n, c.reps), (5000, 2));
        assert_eq!(c.modes, vec![Mode::NoSync, Mode::Durable]);
        let c = config("--rows reth.* --groups reth");
        assert!(!c.rows.is_empty() && c.rows.iter().all(|r| r.group == Group::Reth));
        let c = config("--rows put.seq,get.hit");
        assert_eq!(c.rows.iter().map(|r| r.name).collect::<Vec<_>>(), ["put.seq", "get.hit"]);
        for bad in [
            "--rows nope",
            "--tier slow",
            "--sync fast",
            "--n 10",
            "--reps 0",
            "--page-size 5000",
            "--bogus",
            "--n",
            "--rows get.hit --sync durable",
        ] {
            assert!(parse_args(&args(bad)).is_err(), "{bad} is refused");
        }
    }

    #[test]
    fn a_temp_dir_the_run_cannot_use_is_refused() {
        let root = tempfile::tempdir().expect("a temp dir");
        check_temp_dir(root.path()).expect("a writable dir is used");
        assert_eq!(
            std::fs::read_dir(root.path()).expect("list").count(),
            0,
            "the probe is removed"
        );
        let missing = root.path().join("missing");
        let file = root.path().join("file");
        std::fs::write(&file, b"").expect("write");
        for dir in [missing, file] {
            let err = check_temp_dir(&dir).expect_err("refused");
            assert!(err.contains("cannot make a temp dir"), "{err}");
        }
    }
}
