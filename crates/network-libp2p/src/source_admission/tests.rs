//! Deterministic occupancy regressions with synthetic source populations.

use super::*;
use std::net::Ipv4Addr;

/// Deterministic observed IPv4 source in the synthetic population.
fn address(host: u8) -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(192, 0, 2, host))
}

/// Identity replacement cannot reset address occupancy.
#[test]
fn identity_replacement_retains_source_occupancy() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(8, 2, 2, 8, 8, (24, 64))?);
    let first = budget.acquire(address(1), vec![1])?;
    let second = budget.acquire(address(1), vec![2])?;
    assert!(matches!(budget.acquire(address(1), vec![3]), Err(AdmissionError::AddressFull)));
    drop(first);
    let replacement = budget.acquire(address(1), vec![3])?;
    assert!(matches!(budget.acquire(address(1), vec![4]), Err(AdmissionError::AddressFull)));
    drop((second, replacement));
    Ok(())
}

/// Prefix counts compose across sources in both address families.
#[test]
fn address_families_and_prefix_boundaries() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(8, 2, 1, 2, 8, (24, 64))?);
    let first = budget.acquire(address(1), vec![1])?;
    let second = budget.acquire(address(2), vec![2])?;
    assert!(matches!(budget.acquire(address(3), vec![3]), Err(AdmissionError::PrefixFull)));
    let different = budget.acquire(Ipv4Addr::new(192, 0, 3, 1).into(), vec![3])?;
    assert!(matches!(
        budget.acquire(Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped().into(), vec![4]),
        Err(AdmissionError::AddressFull)
    ));
    let v6 = budget.acquire(Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 1).into(), vec![5])?;
    let v6_next = budget.acquire(Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 2).into(), vec![6])?;
    assert!(matches!(
        budget.acquire(Ipv6Addr::new(0x2001, 0xdb8, 1, 0, 0, 0, 0, 3).into(), vec![7]),
        Err(AdmissionError::PrefixFull)
    ));
    let v6_other =
        budget.acquire(Ipv6Addr::new(0x2001, 0xdb8, 2, 0, 0, 0, 0, 1).into(), vec![7])?;
    drop((first, second, different, v6, v6_next, v6_other));
    Ok(())
}

/// Unrelated sources still consume one process-wide ceiling.
#[test]
fn unrelated_sources_share_process_budget() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(2, 2, 2, 2, 2, (32, 128))?);
    let first = budget.acquire(address(1), vec![1])?;
    let second = budget.clone().acquire(address(2), vec![2])?;
    assert!(matches!(budget.acquire(address(3), vec![3]), Err(AdmissionError::ProcessFull)));
    drop(first);
    let reconnect = budget.acquire(address(3), vec![3])?;
    drop((second, reconnect));
    Ok(())
}

/// The peer ceiling covers all of its observed sources and swarms.
#[test]
fn peer_budget_is_shared_across_sources() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(8, 1, 2, 8, 8, (24, 64))?);
    let first = budget.acquire(address(1), vec![1])?;
    assert!(matches!(budget.clone().acquire(address(2), vec![1]), Err(AdmissionError::PeerFull)));
    drop(first);
    let reconnect = budget.acquire(address(2), vec![1])?;
    drop(reconnect);
    Ok(())
}

/// The source table stays bounded and its final lease expires every key.
#[test]
fn tables_are_bounded_and_reclaimed() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(8, 2, 2, 8, 1, (24, 64))?);
    let first = budget.acquire(address(1), vec![1])?;
    let second = budget.acquire(address(1), vec![2])?;
    assert!(matches!(budget.acquire(address(2), vec![3]), Err(AdmissionError::SourcesFull)));
    drop(first);
    assert!(matches!(budget.acquire(address(2), vec![3]), Err(AdmissionError::SourcesFull)));
    drop(second);
    let reconnect = budget.acquire(address(2), vec![3])?;
    drop(reconnect);
    let occupancy = budget.occupancy.lock().map_err(|_| AdmissionError::Poisoned)?;
    assert_eq!(occupancy.connections, 0);
    assert!(occupancy.peers.is_empty());
    assert!(occupancy.addresses.is_empty());
    assert!(occupancy.prefixes.is_empty());
    Ok(())
}

/// Synthetic shared-NAT observers can join and reconnect on every swarm.
#[test]
fn shared_nat_population_joins_all_swarms() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(12, 3, 12, 12, 12, (24, 64))?);
    let swarms = [budget.clone(), budget.clone(), budget.clone()];
    let leases = swarms
        .iter()
        .flat_map(|swarm| (1..=4).map(move |identity| swarm.acquire(address(1), vec![identity])))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(matches!(budget.acquire(address(1), vec![5]), Err(AdmissionError::ProcessFull)));
    drop(leases);
    let reconnects = swarms
        .iter()
        .map(|swarm| swarm.acquire(address(1), vec![1]))
        .collect::<Result<Vec<_>, _>>()?;
    drop(reconnects);
    Ok(())
}

/// Invalid budgets fail and zero-length masks aggregate the entire family.
#[test]
fn limits_and_zero_length_prefixes() -> Result<(), AdmissionError> {
    assert!(matches!(Limits::new(0, 1, 1, 1, 1, (24, 64)), Err(AdmissionError::InvalidLimits)));
    assert!(matches!(Limits::new(2, 1, 1, 2, 2, (33, 64)), Err(AdmissionError::InvalidLimits)));
    assert!(matches!(Limits::new(2, 1, 1, 2, 2, (24, 129)), Err(AdmissionError::InvalidLimits)));
    let budget = Budget::new(Limits::new(8, 2, 1, 1, 8, (0, 0))?);
    let first = budget.acquire(address(1), vec![1])?;
    assert!(matches!(
        budget.acquire(Ipv4Addr::new(198, 51, 100, 1).into(), vec![2]),
        Err(AdmissionError::PrefixFull)
    ));
    let second = budget.acquire(Ipv6Addr::LOCALHOST.into(), vec![3])?;
    assert!(matches!(
        budget.acquire(Ipv6Addr::UNSPECIFIED.into(), vec![4]),
        Err(AdmissionError::PrefixFull)
    ));
    drop((first, second));
    Ok(())
}

/// Concurrent swarm admission atomically enforces the aggregate ceiling.
#[test]
fn concurrent_swarms_share_one_atomic_budget() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(4, 1, 1, 4, 4, (24, 64))?);
    let ready = std::sync::Barrier::new(8);
    let results = std::thread::scope(|scope| {
        let callers = (1..=8)
            .map(|identity| {
                let budget = &budget;
                let ready = &ready;
                scope.spawn(move || {
                    ready.wait();
                    budget.acquire(address(identity), vec![identity])
                })
            })
            .collect::<Vec<_>>();
        callers
            .into_iter()
            .map(|caller| caller.join().map_err(|_| AdmissionError::Poisoned))
            .collect::<Result<Vec<_>, _>>()
    })?;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 4);
    assert_eq!(
        results.iter().filter(|result| matches!(result, Err(AdmissionError::ProcessFull))).count(),
        4
    );
    drop(results);
    let reconnect = budget.acquire(address(9), vec![9])?;
    drop(reconnect);
    Ok(())
}
