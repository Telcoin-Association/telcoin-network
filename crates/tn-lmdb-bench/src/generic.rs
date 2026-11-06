//! The generic rows: the engine's basic operations on plain tables, the tn-lmdb workloads of
//! `crates/tn-lmdb-reth/src/engine_bench.rs` (its sequential and random inserts, reads, small
//! commits, delete rows, API put rows and large values). Each function is one job: it opens its
//! own environment(s) in the rep's fresh directory and closes them before it returns.

use std::{
    hint::black_box,
    path::Path,
    time::{Duration, Instant},
};

use tn_lmdb::{Env, EnvOptions, ReadTxn as _, Table};

use crate::{
    data::{deleted, Data, BIG, CHURN_ROUNDS, MIXED_BATCH, SEEK_RANGE, SMALL_PUTS},
    nanos, open, per_op, settle, used_bytes, Out,
};

/// `put.seq` (the put loop of `n` 8-byte big-endian sequential keys in one transaction, per put)
/// and `put.seq.commit` (that transaction's commit).
pub(crate) fn seq(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    let env = open(dir, "seq", options);
    let n = data.sizes.n;
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("seq").expect("create");
    let start = Instant::now();
    for i in 0..n {
        txn.put(&table, &(i as u64).to_be_bytes(), data.value(i)).expect("put");
    }
    let puts = start.elapsed();
    let start = Instant::now();
    txn.commit().expect("commit");
    let commit = start.elapsed();
    out.push("put.seq", per_op(puts, n));
    out.push("put.seq.commit", nanos(commit));
}

/// The random table's rows in one environment: `put.rand` (the put loop of `n` random 32-byte
/// keys in one transaction, per put), `put.rand.commit`, `put.rand.file_bytes`, then the reads of
/// that table (`get.hit`, `get.miss`, `scan.fwd`, `scan.rev`, `seek.range`, `seek.range.sorted`)
/// and `commit.small` in a table of its own.
pub(crate) fn rand(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    let env = open(dir, "rand", options);
    let n = data.sizes.n;
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("rand").expect("create");
    let start = Instant::now();
    for (i, k) in data.rand_keys.iter().enumerate() {
        txn.put(&table, k, data.value(i)).expect("put");
    }
    let puts = start.elapsed();
    let start = Instant::now();
    txn.commit().expect("commit");
    let commit = start.elapsed();
    out.push("put.rand", per_op(puts, n));
    out.push("put.rand.commit", nanos(commit));
    out.push("put.rand.file_bytes", used_bytes(&env));
    reads(&env, &table, data, out);
    if out.wants("commit.small") {
        out.push("commit.small", small(&env, data));
    }
}

/// The read rows over the random table of `n` entries.
fn reads(env: &Env, table: &Table, data: &Data, out: &mut Out<'_>) {
    let n = data.sizes.n;
    let ro = env.begin_ro().expect("begin ro");
    if out.wants("get.hit") {
        let start = Instant::now();
        let mut bytes = 0;
        for k in &data.rand_keys {
            bytes += ro.get(table, k).expect("get").expect("a hit").len();
        }
        let time = start.elapsed();
        assert_eq!(black_box(bytes), n * crate::data::VALUE_LEN);
        out.push("get.hit", per_op(time, n));
    }
    if out.wants("get.miss") {
        let start = Instant::now();
        let mut hits = 0;
        for k in &data.miss_keys {
            hits += usize::from(ro.get(table, k).expect("get").is_some());
        }
        let time = start.elapsed();
        assert_eq!(black_box(hits), 0, "every get misses");
        out.push("get.miss", per_op(time, n));
    }
    let mut cursor = ro.cursor(table).expect("cursor");
    if out.wants("scan.fwd") {
        let start = Instant::now();
        let (mut count, mut bytes) = (0, 0);
        let mut entry = cursor.first().expect("first");
        while let Some((k, v)) = entry {
            count += 1;
            bytes += k.len() + v.len();
            entry = cursor.next().expect("next");
        }
        let time = start.elapsed();
        assert_eq!(count, n);
        black_box(bytes);
        out.push("scan.fwd", per_op(time, n));
    }
    if out.wants("scan.rev") {
        let start = Instant::now();
        let (mut count, mut bytes) = (0, 0);
        let mut entry = cursor.last().expect("last");
        while let Some((k, v)) = entry {
            count += 1;
            bytes += k.len() + v.len();
            entry = cursor.prev().expect("prev");
        }
        let time = start.elapsed();
        assert_eq!(count, n);
        black_box(bytes);
        out.push("scan.rev", per_op(time, n));
    }
    let seeks = &data.miss_keys[..data.sizes.seeks];
    for (row, targets) in [("seek.range", seeks), ("seek.range.sorted", &data.sorted_seeks[..])] {
        if !out.wants(row) {
            continue;
        }
        let start = Instant::now();
        let mut bytes = 0;
        for k in targets {
            let mut entry = cursor.seek(k).expect("seek");
            for _ in 1..SEEK_RANGE {
                let Some((_, v)) = entry else { break };
                bytes += v.len();
                entry = cursor.next().expect("next");
            }
        }
        let time = start.elapsed();
        black_box(bytes);
        out.push(row, per_op(time, targets.len()));
    }
}

/// `commit.small`: `small_txns` transactions of `SMALL_PUTS` sequential puts into a table of
/// their own, each begun, filled and committed in turn (then settled, untimed); per
/// transaction, its commit included.
fn small(env: &Env, data: &Data) -> f64 {
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("small").expect("create small");
    txn.commit().expect("commit");
    settle(env);
    let txns = data.sizes.small_txns;
    let mut time = Duration::ZERO;
    for first in (0..(txns * SMALL_PUTS) as u64).step_by(SMALL_PUTS) {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        for key in first..first + SMALL_PUTS as u64 {
            txn.put(&table, &key.to_be_bytes(), data.value(key as usize)).expect("put");
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(env);
    }
    per_op(time, txns)
}

/// A fresh environment `name` in `dir` holding the random table: all `n` entries, put in one
/// transaction (untimed).
fn rand_env(dir: &Path, name: &str, options: EnvOptions, data: &Data) -> (Env, Table) {
    let env = open(dir, name, options);
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("rand").expect("create rand");
    for (i, k) in data.rand_keys.iter().enumerate() {
        txn.put(&table, k, data.value(i)).expect("put");
    }
    txn.commit().expect("commit");
    settle(&env);
    (env, table)
}

/// The API put rows, each `n` sequential keys into a fresh table of its own fresh environment,
/// timed over the put loop (per put): `put.append` (`RwTxn::append`), `put.cursor`
/// (`RwCursor::put`) and `put.txn_cursor` (`TxnCursor::put`).
pub(crate) fn api(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    let n = data.sizes.n;
    for row in ["put.append", "put.cursor", "put.txn_cursor"] {
        if !out.wants(row) {
            continue;
        }
        let env = open(dir, row, options);
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.create_table("api").expect("create");
        let start = Instant::now();
        match row {
            "put.append" => {
                for i in 0..n {
                    txn.append(&table, &(i as u64).to_be_bytes(), data.value(i)).expect("append");
                }
            }
            "put.cursor" => {
                let mut cursor = txn.cursor_mut(&table).expect("cursor");
                for i in 0..n {
                    cursor.put(&(i as u64).to_be_bytes(), data.value(i)).expect("put");
                }
            }
            _ => {
                let mut cursor = txn.cursor_detached(&table).expect("cursor");
                for i in 0..n {
                    cursor.put(&mut txn, &(i as u64).to_be_bytes(), data.value(i)).expect("put");
                }
            }
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        out.push(row, per_op(puts, n));
    }
}

/// `mixed`: `mixed_ops` operations over a fresh random table of `n` entries (filled untimed), in
/// transactions of `MIXED_BATCH` (each settled, untimed), half deletes of a present key and half
/// puts (almost all re-inserts); per operation, the commits included.
pub(crate) fn mixed(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    let (env, table) = rand_env(dir, "mixed", options, data);
    let mut time = Duration::ZERO;
    for (b, batch) in data.mixed.chunks(MIXED_BATCH).enumerate() {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        for (j, &(idx, delete)) in batch.iter().enumerate() {
            let k = &data.rand_keys[idx];
            if delete {
                assert!(txn.delete(&table, k).expect("delete"), "a hit");
            } else {
                txn.put(&table, k, data.value(b + j)).expect("put");
            }
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(&env);
    }
    out.push("mixed", per_op(time, data.mixed.len()));
}

/// `delete.rand90`: 90% of a fresh random table of `n` entries (filled untimed) deleted in a
/// random order in one transaction; the delete loop, per delete.
pub(crate) fn delete(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    let (env, table) = rand_env(dir, "delete", options, data);
    let gone = deleted(data.sizes.n);
    let mut txn = env.begin_rw().expect("begin");
    let start = Instant::now();
    for &i in &data.del_order[..gone] {
        assert!(txn.delete(&table, &data.rand_keys[i as usize]).expect("delete"), "a hit");
    }
    let deletes = start.elapsed();
    txn.commit().expect("commit");
    out.push("delete.rand90", per_op(deletes, gone));
}

/// `churn`: `CHURN_ROUNDS` rounds over a table of `n / 4` random keys, each a fill (the first
/// puts every key, a later one re-inserts the keys the last round deleted) and a delete of 90% of
/// the keys at random, one transaction each (each settled, untimed); per round. Then
/// `churn.file_bytes` (the file's used span) and `churn.table_bytes` (the table's pages, from
/// `Env::check`).
pub(crate) fn churn(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    let env = open(dir, "churn", options);
    let mut txn = env.begin_rw().expect("begin");
    let table = txn.create_table("churn").expect("create churn");
    txn.commit().expect("commit");
    settle(&env);
    let keys = data.sizes.churn;
    let gone = deleted(keys);
    let mut time = Duration::ZERO;
    for r in 0..CHURN_ROUNDS {
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        let mut put = |i: usize| txn.put(&table, &data.rand_keys[i], data.value(i)).expect("put");
        match r {
            0 => (0..keys).for_each(&mut put),
            _ => data.churn_order(r - 1)[..gone].iter().for_each(|&i| put(i as usize)),
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(&env);
        let start = Instant::now();
        let mut txn = env.begin_rw().expect("begin");
        for &i in &data.churn_order(r)[..gone] {
            assert!(txn.delete(&table, &data.rand_keys[i as usize]).expect("delete"), "a hit");
        }
        txn.commit().expect("commit");
        time += start.elapsed();
        settle(&env);
    }
    out.push("churn", per_op(time, CHURN_ROUNDS));
    out.push("churn.file_bytes", used_bytes(&env));
    if out.wants("churn.table_bytes") {
        let pages = env.check().expect("check").tables["churn"].pages;
        out.push("churn.table_bytes", (pages * u64::from(env.stat().page_size)) as f64);
    }
}

/// The large-value rows: for values of 2, 8 and 32 KiB (overflow runs at 4 KiB pages), each in
/// its own fresh environment, the put loop of its entries (8-byte keys in a scrambled order) in
/// one transaction, per put (`big.2k`, ...), then point gets of every key in another scrambled
/// order, per get (`big.2k.get`, ...).
pub(crate) fn big(dir: &Path, options: EnvOptions, data: &Data, out: &mut Out<'_>) {
    const ROWS: [[&str; 2]; 3] =
        [["big.2k", "big.2k.get"], ["big.8k", "big.8k.get"], ["big.32k", "big.32k.get"]];
    for ((&(len, _), &count), [put_row, get_row]) in BIG.iter().zip(&data.sizes.big).zip(ROWS) {
        if !out.wants(put_row) && !out.wants(get_row) {
            continue;
        }
        let env = open(dir, put_row, options);
        let mut txn = env.begin_rw().expect("begin");
        let table = txn.create_table("big").expect("create");
        let start = Instant::now();
        for (i, k) in data.big_keys[..count].iter().enumerate() {
            txn.put(&table, k, data.big_value(i, len)).expect("put");
        }
        let puts = start.elapsed();
        txn.commit().expect("commit");
        out.push(put_row, per_op(puts, count));
        if out.wants(get_row) {
            let ro = env.begin_ro().expect("begin ro");
            let start = Instant::now();
            let mut bytes = 0;
            for j in 0..count {
                bytes +=
                    ro.get(&table, data.big_get_key(j, count)).expect("get").expect("a hit").len();
            }
            let time = start.elapsed();
            assert_eq!(black_box(bytes), len * count);
            out.push(get_row, per_op(time, count));
        }
    }
}
