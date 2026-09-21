//! Exercise the external reader against the node's real protocol and penalty handling.

use super::*;
use futures::{future, stream, TryStreamExt};
use libp2p::multiaddr::Protocol;
use std::net::Ipv4Addr;

/// A standalone reader verifies records and completes gossip negotiation without being banned.
#[tokio::test]
async fn standalone_reader_negotiates_gossip_and_reads_node_record() -> eyre::Result<()> {
    let TestTypes { peer1, peer2: _peer2, _task_manager } =
        create_test_types::<TestWorkerRequest, TestWorkerResponse>();
    let NetworkPeer { config, network_events: _events, network_handle: _handle, mut network } =
        peer1;
    let key = config.key_config().primary_public_key();
    let chain_id = config.network_config().libp2p_config().chain_id;
    let peer_id = *network.swarm.local_peer_id();
    let address = Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::LOCALHOST))
        .with(Protocol::Udp(0))
        .with(Protocol::QuicV1);
    network.swarm.listen_on(address)?;
    let address = network
        .swarm
        .by_ref()
        .filter_map(|event| {
            future::ready(if let SwarmEvent::NewListenAddr { address, .. } = event {
                Some(address)
            } else {
                None
            })
        })
        .next()
        .await
        .ok_or_else(|| eyre!("node listener ended"))?
        .with(Protocol::P2p(peer_id));
    network.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
    network.provide_our_data();

    let (negotiated, negotiation) = oneshot::channel();
    let node = stream::try_unfold(
        (network, Some(negotiated)),
        |(mut network, mut negotiated)| async move {
            let event = network.swarm.select_next_some().await;
            network.process_event(event).await?;
            let supports_gossip =
                network.swarm.behaviour().gossipsub.peer_protocol().any(|(_, kind)| {
                    // Peers initially appear as Floodsub; wait for a negotiated gossip version.
                    format!("{kind:?}").starts_with("Gossipsub")
                });
            if supports_gossip {
                negotiated.take().into_iter().for_each(|sender| {
                    let _ = sender.send(());
                });
            }
            Ok::<_, NetworkError>(Some(((), (network, negotiated))))
        },
    )
    .try_for_each(|()| future::ready(Ok(())));

    let verify = async move {
        let mut reader = tn_kad_client::Client::new(
            chain_id,
            NetworkType::Primary,
            [address],
            Duration::from_secs(5),
        )?;
        let record = reader.lookup(&key).await?.ok_or_else(|| eyre!("own node record missing"))?;
        assert!(record.verify(RecordDomain::new(chain_id, NetworkType::Primary), &key).is_some());

        // Continue driving the reader while awaiting the node's protocol negotiation signal.
        let reads = stream::try_unfold(reader, |mut reader| async move {
            let record = reader.lookup(&key).await?;
            record.ok_or_else(|| eyre!("node record disappeared after the first lookup"))?;
            Ok::<_, eyre::Report>(Some(((), reader)))
        })
        .try_for_each(|()| future::ready(Ok(())));
        tokio::select! {
            result = negotiation => result.map_err(Into::into),
            result = reads => result.and_then(|()| Err(eyre!("reader stopped before gossip negotiation"))),
        }
    };
    timeout(Duration::from_secs(10), async {
        tokio::select! {
            result = verify => result,
            result = node => result.map_err(Into::into).and_then(|()| Err(eyre!("node event driver stopped"))),
        }
    }).await?
}
