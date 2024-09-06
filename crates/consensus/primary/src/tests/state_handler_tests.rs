// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use fastcrypto::{
    serde_helpers::ToFromByteArray,
    traits::{KeyPair, ToFromBytes},
};
use tn_types::test_utils::CommitteeFixture;

#[tokio::test]
async fn start_dkg() {
    let fixture =
        CommitteeFixture::builder().stake_distribution(vec![2500, 2500, 2500, 2500].into()).build();
    let committee = fixture.committee();
    let primary = fixture.authorities().next().unwrap();
    let name = primary.id();

    let randomness_private_key = RandomnessPrivateKey::from(
        fastcrypto::groups::bls12381::Scalar::from_byte_array(
            primary
                .keypair()
                .copy()
                .private()
                .as_bytes()
                .try_into()
                .expect("key length should match"),
        )
        .expect("should work to convert BLS key to Scalar"),
    );
    let randomness_state = RandomnessState::try_new(
        &ChainIdentifier::unknown(),
        // &protocol_config,
        committee,
        randomness_private_key,
        Some(800),
    )
    .unwrap();

    let (tx_system_messages, mut rx_system_messages) = tn_types::test_channel!(1);
    randomness_state.start_dkg(&tx_system_messages).await;

    let dkg_message = rx_system_messages.recv().await.unwrap();
    match dkg_message {
        SystemMessage::DkgMessage(msg) => {
            assert_eq!(msg.sender, name.0);
        }
        _ => panic!("wrong type of message sent"),
    }
}
