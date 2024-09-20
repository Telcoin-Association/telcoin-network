// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Aggregate messages from peers.
mod certificates;
mod votes;
pub(crate) use certificates::CertificatesAggregator;
pub(crate) use votes::VotesAggregator;
