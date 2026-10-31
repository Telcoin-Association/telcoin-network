//! Dup tables with packed sets through the public API: the packed-set limit a table is created
//! with and the number its record holds.

use super::{check, temp};
use crate::{DupOptions, Env, EnvOptions, Error, ReadTxn as _};

/// `create_dup_table` resolves `packed_max` at the file's page size before it records it: 0, 7 and
/// the longest inline value of the outer key are kept, [`DupOptions::PACKED_DEFAULT`] becomes that
/// longest value (as [`DupOptions::resolved`] says), and 1..=6 or one past the longest value is
/// `InvalidDupOptions`, creating nothing and with no latch. Asking again for the default or for
/// its number gives the same handle and changes nothing; another number is `TableKind`. The table
/// stat, in the creating transaction, after its commit and after a reopen, reports the number.
#[test]
fn packed_dup_options_resolve() {
    const K: u16 = 20;
    let shape = |packed_max| DupOptions { key_len: K, packed_max };
    for page_size in [4096, 16384] {
        let (_dir, path) = temp("tndb_b_dup_packed_options");
        let env_options = EnvOptions { page_size, ..EnvOptions::default() };
        let env = Env::open(&path, env_options).expect("open");
        let most = env.inner().geometry.max_value(usize::from(K));
        let most = u16::try_from(most).expect("a u16 limit");
        assert_eq!(most, if page_size == 4096 { 987 } else { 4059 }, "{page_size}");
        let resolved = shape(most);
        assert_eq!(DupOptions::packed(K), shape(DupOptions::PACKED_DEFAULT));
        assert_eq!(DupOptions::packed(K).resolved(page_size).expect("resolved"), resolved);

        let mut txn = env.begin_rw().expect("rw");
        for packed_max in [1, 2, 3, 4, 5, 6, most + 1] {
            let result = txn.create_dup_table("bad", shape(packed_max));
            let what = format!("packed_max {packed_max} outside 7..={most} for this page size");
            assert!(
                matches!(&result, Err(Error::InvalidDupOptions(m)) if m.contains(&what)),
                "{page_size} {packed_max}: {result:?}"
            );
            assert!(shape(packed_max).resolved(page_size).is_err(), "{page_size} {packed_max}");
        }
        assert_eq!(txn.open_table("bad").expect("open"), None, "nothing created");
        for (name, packed_max) in [("never", 0), ("seven", 7), ("most", most)] {
            let t = txn.create_dup_table(name, shape(packed_max)).expect("a limit in range");
            assert_eq!(txn.table_stat(&t).expect("stat").dup, Some(shape(packed_max)), "{name}");
            assert_eq!(shape(packed_max).resolved(page_size).expect("kept"), shape(packed_max));
        }
        let d = txn.create_dup_table("d", DupOptions::packed(K)).expect("the default");
        assert_eq!(txn.table_stat(&d).expect("stat").dup, Some(resolved), "in its own txn");
        assert_eq!(txn.create_dup_table("d", DupOptions::packed(K)).expect("again"), d);
        let base = txn.commit().expect("commit");

        let mut txn = env.begin_rw().expect("rw");
        assert_eq!(txn.create_dup_table("d", DupOptions::packed(K)).expect("the default"), d);
        assert_eq!(txn.create_dup_table("d", resolved).expect("its number"), d);
        for other in [0, 7, most - 1] {
            let result = txn.create_dup_table("d", shape(other));
            assert!(matches!(&result, Err(Error::TableKind(n)) if n == "d"), "{other}: {result:?}");
        }
        let most_table = txn.open_table("most").expect("open").expect("most");
        let again = txn.create_dup_table("most", DupOptions::packed(K)).expect("the default");
        assert_eq!(again, most_table, "a table made with the number answers the default");
        assert_eq!(txn.table_stat(&d).expect("stat").dup, Some(resolved), "after its commit");
        assert_eq!(txn.commit().expect("commit"), base, "nothing changed");
        assert!(env.failure().is_none(), "{page_size}: no latch");
        check(&env);
        drop(env);

        let env = Env::open(&path, env_options).expect("reopen");
        let ro = env.begin_ro().expect("ro");
        let d = ro.open_table("d").expect("open").expect("d");
        assert_eq!(ro.table_stat(&d).expect("stat").dup, Some(resolved), "after a reopen");
        assert_eq!(check(&env).tables["d"].entries, 0);
    }
}
