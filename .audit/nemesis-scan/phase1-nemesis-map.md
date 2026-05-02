# Phase 1: Nemesis Map — issue-629

_Git hash: 9b398938_
_Scope: NEW or MODIFIED `pub` / `pub(crate)` functions in the listed files (delta from `main`)._
_Generated: 2026-04-27_

This file delivers the three artifacts required by Phase 1: the
**Function-State Matrix** (1A), the **Coupled State Dependency Map** (1B), and
the **Cross-Reference** of which functions update only one side of a coupled
pair (1C). The 1C "GAP" rows are the primary audit targets for Phase 2.

---

## 1A. Function-State Matrix

State variables and channels referenced (canonical names):

- `pending_epoch_requests : Arc<Mutex<HashMap<(BlsPublicKey, B256), PendingEpochStream>>>` (primary network/mod.rs:417)
- `epoch_stream_semaphore : Arc<Semaphore>` (primary network/mod.rs:412), `MAX_CONCURRENT_EPOCH_STREAMS=5`
- `pending_batch_requests : Arc<Mutex<HashMap<(BlsPublicKey, B256), PendingBatchStream>>>` (worker network/mod.rs:121)
- `batch_stream_semaphore : Arc<Semaphore>` (worker network/mod.rs:123)
- `consensus_idx : PositionIndex` (consensus_pack.rs:427) — round-offset → file position
- `consensus_digests : HdxIndex` (consensus_pack.rs:428) — header digest → file position
- `batch_digests : HdxIndex` (consensus_pack.rs:429) — batch digest → file position
- `data : Pack<PackRecord>` (consensus_pack.rs:426) — append-only data file
- `epoch_meta : EpochMeta` (consensus_pack.rs:430) — first record in pack
- `current_pack : Arc<Mutex<Option<ConsensusPack>>>` (consensus.rs:281)
- `recent_packs : Arc<Mutex<VecDeque<ConsensusPack>>>` (consensus.rs:285)
- `latest_consensus : LatestConsensus` (consensus.rs:283) — `(epoch, number, current_slot)` plus 2 slot files on disk
- `tx_last_consensus_header : watch<Option<ConsensusHeader>>` (consensus_bus.rs:216)
- `tx_last_published_consensus_num_hash : watch<(Epoch, u64, BlockHash)>` (consensus_bus.rs:218)
- `epoch_request_queue_tx / _rx : mpsc::Sender/Receiver<EpochRecord>` (consensus_bus.rs:236-238)
- `auth_last_vote : Arc<HashMap<AuthorityIdentifier, TokioMutex<Option<...>>>>` (handler.rs:64)
- `consensus_certs : Arc<Mutex<HashMap<BlockHash, u32>>>` (handler.rs:66)
- `requested_parents : Arc<Mutex<BTreeMap<(Round, CertificateDigest), AuthorityIdentifier>>>` (handler.rs:61)
- ConsensusHeaderCache table (DB) (consensus.rs:37, 51)
- `value_buffer : Vec<u8>` inside `PackInner` (pack.rs:151) — peer-driven `resize`

### 1A.1 — `crates/consensus/primary/src/network/mod.rs` (new)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `PrimaryNetworkHandle::request_epoch_pack` (mod.rs:322-395) | `epoch_record.epoch` | none (caller side; no local state) | none — no auth on response peer claim | `send_request_any`, `open_stream`, `consensus_chain.stream_import`, `report_penalty(peer, Mild)` (line 381) | libp2p send/recv, libp2p stream open, write `request_digest`, `consensus_chain.stream_import` (DB writes via inner) |
| `PrimaryNetwork::spawn` (mod.rs:463-488) | `network_events`, prune ticker | spawns critical task | none | `process_network_event`, `cleanup_stale_pending_requests` | tokio::interval, tokio::select |
| `PrimaryNetwork::cleanup_stale_pending_requests` (mod.rs:491-496) | `pending_epoch_requests` (read created_at) | `pending_epoch_requests` (retain — drops `_permit` on retained entries) | parking_lot mutex on map | `now()`, `retain` | none |
| `PrimaryNetwork::process_network_event` (mod.rs:499-542) | event variant | (delegates) | none | dispatch to `process_*` | none |
| `PrimaryNetwork::process_epoch_stream` (mod.rs:666-749) | `epoch_stream_semaphore` (try_acquire), `pending_epoch_requests` (count by peer, map insert) | `pending_epoch_requests` (insert), `_permit` (held in inserted value) | `try_acquire_owned`, `MAX_PENDING_REQUESTS_PER_PEER`, parking_lot mutex | hash to compute `request_digest`, spawn task to send response | spawn task → libp2p `send_response` |
| `PrimaryNetwork::process_inbound_stream` (mod.rs:772-813) | stream bytes (request_digest), `pending_epoch_requests` (lookup+remove) | `pending_epoch_requests` (remove) | 5s timeout for digest read, parking_lot mutex | `request_handler.process_request_epoch_stream`, `report_penalty` | libp2p stream read, spawned task |
| `PrimaryNetwork::process_consensus_output_request` (mod.rs:604-635) | `consensus_chain.latest_consensus_number`, `request_handler.retrieve_consensus_header` | none locally | spawns task | `retrieve_consensus_header`, `report_penalty(peer, Mild)` on Err | libp2p send_response |
| `PrimaryNetwork::process_epoch_record_request` (mod.rs:638-663) | (delegates) | none locally | spawns task | `retrieve_epoch_record` | libp2p send_response |
| `PrimaryNetwork::process_gossip` (mod.rs:752-767) | (delegates) | none locally | spawns task | `request_handler.process_gossip`, `report_penalty` | none |
| `PrimaryNetworkHandle::report_penalty` (mod.rs:307-309) | `peer`, `penalty` | (downstream peer manager) | none | `handle.report_penalty` | libp2p peer-manager command |

### 1A.2 — `crates/consensus/primary/src/network/handler.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `RequestHandler::process_request_epoch_stream` (handler.rs:941-990) | `pending_request: Option<PendingEpochStream>` (consumes), stream | none (no map updates here — already removed by caller) | early-return error if `None` (penalty source) | `Self::send_epoch_over_stream`, `stream.close` | libp2p stream write/close, `consensus_chain.get_epoch_stream` (DB read) |
| `RequestHandler::send_epoch_over_stream` (handler.rs:916-938) | `consensus_chain.get_epoch_stream(epoch)` | none | `SEND_STREAM_BUFFER_TIMEOUT=10s` per write | `read`/`write_all` | file read of finalised epoch pack, libp2p write |
| `RequestHandler::process_gossip` — `Consensus(...)` path (handler.rs:233-298) | `consensus_certs`, `published_consensus_num_hash`, committee | `consensus_certs` (insert/clear), `last_published_consensus_num_hash` (send_replace), `requested_missing_epoch` (send_replace) | committee membership, BLS verify, 1/3+1 sig threshold | `behind_consensus`, `get_committee` | watch send |
| `RequestHandler::vote` (handler.rs:330-440) | `auth_last_vote`, `node_storage.read_vote_info` | `auth_last_vote` (set) | per-authority TokioMutex, peer-author check, MaxHeaderDelay timeout | `vote_inner`, `Vote::new` | DB read, DB write of vote |
| `RequestHandler::retrieve_consensus_header` (handler.rs:821-834) | `consensus_chain.latest_consensus_number`, `consensus_chain.consensus_header_by_digest` | none | sleep loop until +1 | `get_header_by_hash` | DB read |
| `RequestHandler::retrieve_epoch_record` (handler.rs:837-849) | `consensus_chain.epochs()...` | none | rejects (None,None) | `get_epoch_by_hash` / `get_epoch_by_number` | DB read |

### 1A.3 — `crates/consensus/primary/src/error/network.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `From<&PrimaryNetworkError> for Option<Penalty>` (network.rs:70-132) | error variant | (returns Penalty) | exhaustive match | `penalty_from_header_error` | none |
| `penalty_from_header_error` (network.rs:137-171) | header error | (returns Penalty) | exhaustive match | none | none |

NEW variants relevant to Phase 1:
- `UnknownStreamRequest(BlockHash)` → `Penalty::Mild` (network.rs:64, 117)
- `StreamUnavailable(Epoch)` → `None` (no penalty) (network.rs:67, 128)
- `UnavailableEpoch / UnavailableEpochDigest` → `None`

### 1A.4 — `crates/consensus/primary/src/network/message.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `PrimaryRequest::StreamEpoch { epoch }` (message.rs:132-136) | n/a (variant) | n/a | n/a | n/a | wire format |
| `PrimaryResponse::RequestEpochStream { ack, peer }` (message.rs:247-258) | n/a (variant) | n/a | n/a | n/a | wire format — note `peer` is the responder's claim, not authenticated |
| `PrimaryResponse::into_error_ref` (message.rs:267-295) | error variant | none | exhaustive match | none | none |

### 1A.5 — `crates/consensus/primary/src/consensus_bus.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `ConsensusBusApp::request_epoch_pack_file` (consensus_bus.rs:551-553) | `epoch_request_queue_tx` | `epoch_request_queue_tx` (push EpochRecord; capacity 10_000) | none — return value discarded with `let _ =` | mpsc send | mpsc::Sender |
| `ConsensusBusApp::get_next_epoch_pack_file_request` (consensus_bus.rs:558-560) | `epoch_request_queue_rx` | `epoch_request_queue_rx` (pop) | tokio::Mutex on receiver | mpsc recv | mpsc::Receiver |
| `ConsensusBusApp::last_consensus_header` (consensus_bus.rs:356-358) | (returns watch handle) | (caller writes) | none | none | none |
| `ConsensusBusApp::last_published_consensus_num_hash` (consensus_bus.rs:363-365) | (returns watch handle) | (caller writes) | none | none | none |
| `ConsensusBusApp::reset_for_epoch` (consensus_bus.rs:304-307) | none | `tx_committed_round_updates`, `tx_primary_round_updates` reset to 0 | none | watch::send_replace | none — does NOT clear `auth_last_vote`, `consensus_certs`, `requested_parents`, `pending_epoch_requests`, `last_consensus_header`, `last_published_consensus_num_hash` |

### 1A.6 — `crates/state-sync/src/consensus.rs` (rewritten)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `get_consensus_header` (consensus.rs:20-81) | `consensus_chain.latest_consensus_number`, `node_storage::ConsensusHeaderCache`, `last_consensus_header` (read) | `node_storage.insert::<ConsensusHeaderCache>`, `last_consensus_header.send_replace` (line 65) | strict-hash check is in caller (`request_consensus`); just compares header digest implicitly via that helper | `network.request_consensus`, watch send | DB write |
| `spawn_track_recent_consensus` (consensus.rs:85-152) | `last_published_consensus_num_hash` (subscribe), `epochs.record_by_epoch`, `consensus_header_by_number` | enqueues to mpsc `tx`, calls `consensus_bus.request_epoch_pack_file` (line 116) | shutdown noticer, gossipped epoch tracking | `get_consensus_header`, `request_epoch_pack_file` | spawn task |
| `spawn_fetch_consensus` (consensus.rs:159-219) | `epoch_request_queue` via `get_next_epoch_pack_file_request`, `epochs.get_epoch_by_number` | indirectly via `network.request_epoch_pack` → `consensus_chain.stream_import` | semaphore limit 1 (per-task), retry loop, shutdown noticer | `network.request_epoch_pack` | libp2p, DB import |

### 1A.7 — `crates/state-sync/src/epoch.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `epoch_committee_valid` (epoch.rs:20-42) | `epoch_rec.committee`, committee | none | size & 2/3 checks | none | none |
| `collect_epoch_records` (epoch.rs:46-161) | `consensus_chain.epochs()`, `published_consensus_num_hash` | `epochs.save`, `epochs.persist`, `last_published_consensus_num_hash.send_replace` (line 157) | parent-hash chain, committee BLS verify (`verify_with_cert`) | `request_epoch_cert`, `epoch_committee_valid`, `verify_with_cert`, `save`, `persist` | DB writes; watch send |
| `spawn_epoch_record_collector` (epoch.rs:166-206) | `requested_missing_epoch` (subscribe) | (delegated via collect_epoch_records) | shutdown, retry timer | `collect_epoch_records` | spawn task |

### 1A.8 — `crates/storage/src/archive/pack.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `Pack::open` (pack.rs:42-44) | path, uid_idx, ro flag | creates/opens data file | header CRC, uid match | `PackInner::open` | file open, header read/write |
| `Pack::append` (pack.rs:78-80) | value | appends to data file (size, body, CRC) | `read_only`/`failed` | `append_inner` | file write |
| `Pack::truncate` (pack.rs:127-129) | new_len | sets data file length | none | `data_file.set_len` | file truncate |
| `Pack::raw_iter` (pack.rs:134-136) | data file | none (read) | header validation | `PackIter::open` | file open clone |
| `Pack::record_size` (pack.rs:58-60) | data file | seeks; updates `value_buffer` (ephemeral) | CRC32 | `PackInner::record_size` | file read |
| `PackInner::read_record` / `record_size` (pack.rs:313-355) | data file at `position` | `value_buffer.resize(val_size as usize, 0)` (line 320, 344) — **no upper bound on `val_size`** | CRC32 check | `try_decode` | file read |

### 1A.9 — `crates/storage/src/archive/pack_iter.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `PackIter::open` (pack_iter.rs:40-44) | reader | (DataHeader load) | uid_idx + CRC32 | `DataHeader::load_header` | sync read |
| `PackIter::read_record_file` (pack_iter.rs:59-87) | file bytes | `buffer.resize(val_size as usize, 0)` (line 76) — **untrusted `val_size`** | CRC32 | `decode` | sync read |
| `AsyncPackIter::open` (pack_iter.rs:128-131) | reader | (DataHeader load) | uid_idx + CRC32 | `DataHeader::load_header_async` | async read |
| `AsyncPackIter::read_record_file` (pack_iter.rs:135-160) | reader | `buffer.resize(val_size as usize, 0)` (line 149) — **untrusted `val_size`** | CRC32 | `decode` | async read |
| `AsyncPackIter::next` (pack_iter.rs:163-171) | reader | (delegates) | none | `read_record_file` | async read |

### 1A.10 — `crates/storage/src/consensus.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `LatestConsensus::update` (consensus.rs:229-249) | self.state | swaps `current_slot`, `epoch`, `number`; sends mpsc Update to writer thread | parking_lot Mutex on state | mpsc send | (writer thread) writes 16 bytes + CRC + sync_all to disk |
| `LatestConsensus::persist` (consensus.rs:252-256) | none | sends `Persist`; awaits oneshot ack | mpsc + oneshot | none | sync_all on slot files |
| `LatestConsensus::epoch / number / current_slot` (consensus.rs:259-272) | self.state | none | mutex | none | none |
| `LatestConsensus::read_slot` (consensus.rs:95-117) | slot file | (none — return value) | CRC32 | none | file read |
| `ConsensusChain::new` (consensus.rs:294-303) | base_path | initializes `current_pack`, `latest_consensus`, `recent_packs`, `epochs` | none | `LatestConsensus::new`, `ConsensusPack::open_append_exists`, `EpochRecordDb::open` | file open |
| `ConsensusChain::new_epoch` (consensus.rs:325-348) | committee, previous_epoch, `current_pack` | rotates `current_pack`; pushes old to `recent_packs` (with cap pop_front) | `previous_epoch.epoch == committee.epoch()-1` | `persist`, `ConsensusPack::open_append`, `pack.persist` | DB writes (epoch meta first record) |
| `ConsensusChain::stream_import` (consensus.rs:357-438) | `epoch_record`, `previous_epoch`, stream | writes to `import-{epoch}` dir, then renames to `epoch-{epoch}`; clears `current_pack` if it matches; invalidates `recent_packs` cache; calls `pack.persist` | final-consensus number+hash compared to imported pack's `latest_consensus_header` (lines 388-395) | `get_static`, `ConsensusPack::stream_import`, `pack.latest_consensus_header`, `pack.persist`, `std::fs::rename`, `std::fs::remove_dir_all` | filesystem rename / unlink |
| `ConsensusChain::get_epoch_stream` (consensus.rs:442-472) | `get_static`, `epochs.get_epoch_by_number`, `pack.latest_consensus_header` | none | final-consensus number+hash match | `get_static`, `latest_consensus_header` | open file for read |
| `ConsensusChain::save_consensus_output` (consensus.rs:476-491) | `current_pack`, `latest_consensus.number` | `pack.save_consensus_output(consensus)`, `latest_consensus.update(epoch, number)` (line 485) | `consensus.number() > latest_consensus.number()` | `save_consensus_output`, `latest_consensus.update` | DB writes via PackInner |
| `ConsensusChain::consensus_header_by_digest / _by_number` (consensus.rs:506-541) | `current_pack`, `get_static` | none | epoch must match | `pack.consensus_header_by_*` | DB read |
| `ConsensusChain::consensus_header_latest` (consensus.rs:544-548) | `latest_consensus.epoch()` | none | none | `latest_consensus_header_from_pack` | DB read |
| `ConsensusChain::latest_consensus_number / _epoch` (consensus.rs:551-558) | `latest_consensus.{epoch,number}` | none | none | none | none |
| `ConsensusChain::persist_current` (consensus.rs:561-567) | `current_pack` | persists current pack and `latest_consensus` | none | `pack.persist`, `latest_consensus.persist` | sync_all |
| `ConsensusChain::latest_consensus_header_from_pack` (consensus.rs:572-586) | `current_pack`, `recent_packs` | none | none | `pack.latest_consensus_header` | DB read |
| `ConsensusChain::get_static` (consensus.rs:683-704) | `current_pack`, `recent_packs` | inserts to `recent_packs` (push_back) with `pop_front` if oversized | parking_lot mutex on `recent_packs` | `ConsensusPack::open_static`, `pack.persist` | file open |

### 1A.11 — `crates/storage/src/consensus_pack.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `ConsensusPack::open_append` (consensus_pack.rs:203-222) | path, prev epoch, committee | spawns writer thread; opens 4 files | (delegated) | `Inner::open_append` | file open |
| `ConsensusPack::open_append_exists` (consensus_pack.rs:225-232) | path, epoch | reopens existing | (delegated) | `Inner::open_append_exists` | file open |
| `ConsensusPack::open_static` (consensus_pack.rs:237-251) | path, epoch | opens read-only | (delegated; calls `files_consistent`) | `Inner::open_static` | file open |
| `ConsensusPack::stream_import` (consensus_pack.rs:254-269) | path, stream, epoch, prev_epoch, timeout | builds Inner from stream (importing into all 4 files) | `epoch == epoch_meta.epoch` (704), parent-hash chain (752), batch-set match (754-763), per-record timeout | `Inner::stream_import` | filesystem; libp2p stream |
| `ConsensusPack::save_consensus_output` (consensus_pack.rs:287-294) | consensus | sends `ConsensusOutput` msg to bg thread (which calls `inner.save_consensus_output`) | mpsc | none direct | mpsc send |
| `ConsensusPack::persist` (consensus_pack.rs:351-359) | none | persists Inner | oneshot | mpsc send | sync_all |
| `ConsensusPack::latest_consensus_header` (consensus_pack.rs:382-389) | none | none | none | mpsc Q | DB read |
| `Inner::open_append` (consensus_pack.rs:531-592) | files exist | creates/opens `data`, `consensus_idx`, `consensus_digests`, `batch_digests`; writes `EpochMeta` first record on new pack (line 558); calls `trunc_and_heal` | epoch_meta equality check (line 554) | `trunc_and_heal` | file create/open/write |
| `Inner::open_append_exists` (consensus_pack.rs:595-630) | files | reopens; calls `trunc_and_heal` | epoch_meta load | `trunc_and_heal` | file open |
| `Inner::open_static` (consensus_pack.rs:633-670) | files | opens RO | `files_consistent` (else `CorruptPack`) | `files_consistent` | file open |
| `Inner::files_consistent` (consensus_pack.rs:440-471) | `data.file_len`, `consensus_digests.data_file_length`, `batch_digests.data_file_length`, `consensus_idx.load(last)` | none | three lengths must equal pack_len; last consensus_idx record must end at pack_len | none | file read |
| `Inner::trunc_and_heal` (consensus_pack.rs:474-527) | data.file_len, index lengths | `data.truncate`, `consensus_idx.truncate_to_index` / `truncate_all`; **no truncation of `consensus_digests` or `batch_digests`** (note in code) | size comparison | `data.record_size`, `data.truncate` | file truncate |
| `Inner::stream_import` (consensus_pack.rs:673-785) | stream records | `data.append`, `batch_digests.save`, `consensus_idx.save`, `consensus_digests.save`, `set_data_file_length` on both digest indexes | per-record `tokio::time::timeout`; parent-hash chain (line 751); batches set bookkeeping (HashSet) (lines 754-763); first record must be `EpochMeta` (697); epoch number match (702) | `next` (timeout wrapper), `data.append`, `<index>.save`, `set_data_file_length` | filesystem writes via PackInner |
| `Inner::save_consensus_output` (consensus_pack.rs:788-845) | output, current state | for each batch: `data.append` then `batch_digests.save`; for header: `data.append` then `consensus_digests.save` then `consensus_idx.save`; updates `set_data_file_length` on both digest indexes | strict consensus_idx contiguity (793-802) | `data.append`, `<index>.save`, `set_data_file_length` | filesystem write |
| `Inner::get_consensus_output` (consensus_pack.rs:848-909) | `consensus_idx.load`, `data.fetch`, `batch_digests.load` | none | start_consensus_number bounds | `consensus_idx.load`, `data.fetch`, `batch_digests.load` | file read |
| `Inner::contains_consensus_header` (consensus_pack.rs:918-927) | `consensus_digests.load`, `data.file_len` | none | `pos < data.file_len` (defensive!) | none | none |
| `Inner::consensus_header_by_digest / _by_number` (consensus_pack.rs:930-948) | `consensus_digests` / `consensus_idx`, `data` | none | bounds checks | `<index>.load`, `data.fetch` | file read |
| `Inner::latest_consensus_header` (consensus_pack.rs:963-970) | `consensus_idx.len`, `epoch_meta.start_consensus_number` | none | non-empty idx | `consensus_header_by_number` | file read |
| `Inner::persist` (consensus_pack.rs:950-958) | none | `data.commit`, all 3 indexes' `sync` | `read_only` | sync calls | sync_all |

### 1A.12 — `crates/consensus/worker/src/network/mod.rs` (modified)

| Function | Reads | Writes | Guards | Internal Calls | External / IO |
|---|---|---|---|---|---|
| `WorkerNetwork::spawn` (mod.rs:155-180) | events, prune ticker | spawns critical task | none | `process_network_event`, `cleanup_stale_pending_requests` | tokio::interval |
| `WorkerNetwork::cleanup_stale_pending_requests` (mod.rs:436-441) | `pending_batch_requests` (read created_at) | `pending_batch_requests` (retain — drops `_permit`) | parking_lot mutex | `now()`, `retain` | none |
| `WorkerNetwork::process_request_batches_stream` (mod.rs:280-387) | `batch_stream_semaphore` (try_acquire), `pending_batch_requests` (count), batch_digests | `pending_batch_requests` (insert), `_permit` held in entry; truncates oversized batch_digests | `try_acquire_owned`, `MAX_PENDING_REQUESTS_PER_PEER`, `MAX_BATCH_DIGESTS_PER_REQUEST`, parking_lot mutex | `generate_batch_request_id`, spawn task | libp2p send_response |
| `WorkerNetwork::process_inbound_stream` (mod.rs:392-433) | stream digest, `pending_batch_requests` (lookup+remove) | `pending_batch_requests` (remove) | 5s timeout for digest read | `request_handler.process_request_batches_stream`, `report_penalty` | libp2p stream read |

---

## 1B. Coupled State Dependency Map

For each pair: state A, state B, the invariant linking them, code that reads
**both** together (verification), and the verdict. Refuted hypotheses are
listed in the "Refuted" subsection.

| # | State A | State B | Invariant | Code that reads BOTH | Functions that update only one | Verdict |
|---|---------|---------|-----------|----------------------|-------------------------------|---------|
| 1 | `consensus_idx` (round-offset → pos) | `consensus_digests` (digest → pos) | For every consensus header at offset `i`, both indexes point to the same `pos` in `data`; bidirectional injection | `Inner::trunc_and_heal` (consensus_pack.rs:474-527, only truncates `consensus_idx`, leaves `consensus_digests` with stale entries — see comment at lines 489-493); `Inner::contains_consensus_header` (consensus_pack.rs:918-927) — defensive check `pos < data.file_len()`; `Inner::save_consensus_output` (consensus_pack.rs:828-839) writes to both | `Inner::trunc_and_heal` (writes only `consensus_idx`); `Inner::stream_import` consensus arm (consensus_pack.rs:750-781) writes both, but on early return after `consensus_digests.save` (e.g. parent-hash failure on next record) the persisted `consensus_digests` entry can outlive an absent `consensus_idx` entry | **VERIFIED** (defensive code at line 923 explicitly handles index/data divergence — confirms invariant is broken in failure paths) |
| 2 | `consensus_idx` / `consensus_digests` (any consensus header recorded) | `batch_digests` (every batch digest in any header's payload) | Every header in `consensus_idx` references batches that are present in `batch_digests` | `Inner::stream_import` consensus arm (consensus_pack.rs:754-763) — verifies `batches.remove(digest)` for every payload digest at the moment a header arrives; `Inner::get_consensus_output` (consensus_pack.rs:885-895) loads batch from `batch_digests` for every digest in header payload | `Inner::stream_import` batch arm (consensus_pack.rs:737-749) writes to `batch_digests` and `data` BEFORE the corresponding header (and its parent-hash check) is processed; if the stream errors after batches but before header, the batches are persisted into the import dir but the import dir IS removed on error (consensus.rs:434). However the in-memory `batch_digests` index in the import context is what is checked against the header — the worked-example "orphan-batch scar" only manifests if the import dir is reused or the rename succeeds for a partial pack. | **VERIFIED** (semantic check exists per-header but the WAL ordering is "data+batch_digests written, then header validates") |
| 3 | `data` file content (PackInner) | `consensus_idx`, `consensus_digests`, `batch_digests` | Every offset stored in any of the three indexes points to a valid record inside `data` (offset < `data.file_len`) | `Inner::contains_consensus_header` (consensus_pack.rs:918-927); `Inner::files_consistent` (consensus_pack.rs:440-471) — equates lengths and verifies the last `consensus_idx` record terminates exactly at `pack_len` | `Inner::trunc_and_heal` truncates `data` and `consensus_idx` only, leaving stale entries in `consensus_digests` / `batch_digests` (explicit comment in code at 489-493) | **VERIFIED** |
| 4 | `pending_epoch_requests` map size | `epoch_stream_semaphore` available permits | `permits + map.len() == MAX_CONCURRENT_EPOCH_STREAMS` (and per-peer count ≤ 2) | `process_epoch_stream` (mod.rs:674-723) — acquires permit then checks per-peer count under the mutex; on rejection drops permit (line 688) | `cleanup_stale_pending_requests` retain (mod.rs:491-496) — drops the `PendingEpochStream`, which **drops `_permit`** (Drop trait) restoring 1 permit; `process_inbound_stream` (mod.rs:798-800) `remove` from map then proceeds (permit dropped when entry goes out of scope) | **VERIFIED** (relies entirely on Drop; any panic between permit acquisition and entry-insert leaks a permit) |
| 5 | `pending_epoch_requests` entry lifetime | An importer task that produces the pack file (the inbound stream task spawned in `process_inbound_stream`) | Either an active importer task is running for the (peer, request_digest) OR the entry is removed | `process_inbound_stream` (mod.rs:778-812) — looks up & removes entry, then spawns importer; **after `remove` the entry is gone whether or not the importer succeeds**. The reverse direction: an entry exists in the map even when no inbound stream has arrived (the receiving side, between RPC ack and stream open). | `process_epoch_stream` inserts entry on RPC ack (mod.rs:702-711); only `cleanup_stale_pending_requests` (15s × 30s timeout) reclaims it if peer never opens stream; `process_inbound_stream` removes it on stream arrival | **VERIFIED with caveat** — the entry is for the *responder* side, which holds the slot while waiting for the requester to open the stream. There is NO importer task while pending; reclamation depends on the prune loop only. |
| 6 | `epoch_stream_semaphore` permits | Active import work | `permits + active = MAX_CONCURRENT_EPOCH_STREAMS` | Permit is held inside `PendingEpochStream._permit` from `process_epoch_stream` until either `cleanup_stale_pending_requests` retains it out, or `process_inbound_stream` removes it | Any panic in the spawned task in `process_inbound_stream` between `pending_map.lock().remove(...)` and a subsequent `Drop` of the `PendingEpochStream` value: tokio task panic isolation means the spawn finishes silently, but Rust's drop semantics still drop locals — so the permit IS released. The risk is BEFORE the remove: permit is held by the entry in the map; if the prune loop itself panics (no panic guard) the permit leaks indefinitely. | **VERIFIED** (no panic guard around `cleanup_stale_pending_requests`; the spawn at mod.rs:464 marks it "critical" but the inner select on `prune_requests.tick()` calls `self.cleanup_stale_pending_requests()` synchronously — a panic there ends the task) |
| 7 | `tx_last_consensus_header` watch | The set of fully-validated, durable headers in the consensus DB (`consensus_chain`) | Watch points only at headers that are in `consensus_chain` AND verified | `state-sync/consensus.rs:60-65`: reads watch's current value, compares numbers, then calls `send_replace(Some(header))` only for a header that just came back from `request_consensus(number, hash)` AND was inserted into `ConsensusHeaderCache` (line 51). The header has been validated against `hash` by `request_consensus`'s digest check at the network layer (mod.rs:243). | `consensus.rs:65` writes the watch the moment the header is cached, not after `consensus_chain.consensus_header_by_*` confirms it's in the persisted pack file. There is no other watch reader. | **NEEDS-PHASE-2-INTERROGATION** — the strict-hash check makes this safer than worked-example #5 implies; question for Phase 2 is whether the watch can ever advance during a `stream_import` that ultimately fails. Note: `stream_import` (storage/consensus.rs:357) writes via `ConsensusPack::stream_import` to a side directory and does not touch this watch directly. |
| 8 | `epoch_request_queue` (mpsc, capacity 10_000) | "Idempotency" replacement state from #633 — i.e. the property that two live pack-fetch tasks never run for the same epoch | Channel never has two live entries for the same epoch | `state-sync/consensus.rs:111-121` enqueues epoch records into the channel; `state-sync/consensus.rs:178` (`next_epoch`) dequeues under a `Semaphore::new(1)` per task. **There is no dedup at enqueue time** — `request_epoch_pack_file` (consensus_bus.rs:551) blindly sends. | `request_epoch_pack_file` (consensus_bus.rs:551-553) writes the channel without checking pending or in-flight epoch records; `spawn_track_recent_consensus` is the only producer (consensus.rs:116). Multiple `spawn_fetch_consensus` tasks each take their own permit so they CAN run two epochs in parallel — but commit `cb0e147c`'s "idempotent request replacement" is in `process_epoch_stream` on the responder side, not the requester side. | **VERIFIED — idempotency is on the responder, not the requester.** The pair is broken on the requester side: a duplicate `request_epoch_pack_file(epoch_record)` enqueues the same record twice; both will eventually be dequeued and trigger `network.request_epoch_pack` twice. The second call returns `Ok` quickly via the early-return in `ConsensusChain::stream_import` (consensus.rs:365-374) once the first has finished. **However**: if the second is dequeued WHILE the first is in-flight, both run concurrently. Combined with adversarial sequence #5, a peer can re-trigger at near-completion. |
| 9 | Worker `network/mod.rs` peer-score state (libp2p-managed) | Primary's `report_penalty` calls (mod.rs:381, 626, 736, 763, 809) | The PeerId penalised must be the actual on-wire responder, not the BLS claim in the response payload | `request_epoch_pack` (mod.rs:342-381) — receives `RequestEpochStream { ack, peer }` where `peer: BlsPublicKey` is the peer's CLAIM in the body; uses that `peer` to (a) `open_stream(peer)` and (b) `report_penalty(peer, Mild)` | `request_epoch_pack` reads `peer` from the response payload (line 342) and uses it for both the stream open AND the penalty (line 381). There is no cross-check that this BLS key matches the on-wire libp2p PeerId | **VERIFIED — broken pair.** A peer can claim someone else's BlsPublicKey in the response. The penalty is then mis-applied to the wrong validator. (Whether the libp2p layer's `open_stream(peer)` resolves BLS→PeerId via the trusted committee mapping is the Phase 2 question.) |
| 10 | `auth_last_vote` map | Current epoch | Map cleared (or invalidated) on epoch transition so stale entries don't influence cross-epoch vote handling | `RequestHandler::vote` (handler.rs:330-440) — uses `header.author()` to look up `auth_last_vote` (line 365) and stores `(epoch, round, digest, response)` at line 435; `vote_inner`'s epoch-mismatch returns ClosedWatchChannel | `RequestHandler::new` (handler.rs:81-87) preloads with current committee's authority IDs once at construction; `ConsensusBusApp::reset_for_epoch` (consensus_bus.rs:304-307) does NOT clear `auth_last_vote`, `consensus_certs`, `requested_parents`, or `pending_epoch_requests` | **VERIFIED — broken pair.** `auth_last_vote` is keyed by `AuthorityIdentifier`, which can be re-used across epochs for the same validator. The `take()` on entry (handler.rs:369) consumes the prior entry, but `last_epoch < header.epoch()` comparisons later (line 406-415) only check ordering, not epoch identity. A vote-replay attacker who carried over a stale entry can cause an `AlreadyVotedForLaterRound` rejection or `Recast` of an old vote. (Mitigated by sanity check at line 343-347 forcing peer-to-author match — confirm in Phase 2 that this fully closes the gap.) Also: `consensus_certs` and `requested_parents` carry across epochs without explicit reset. |

### 1B Refuted hypotheses

- **Pre-seeded pair "Three-index torn write" (#9 in domain-patterns.md):** This is a re-statement of pairs 1+2+3 above; it's covered by VERIFIED entries.
- **Pre-seeded pair "Pending-request slot leak via panic" (#10):** Already captured by pair 4 (semaphore↔map) and pair 6 (semaphore↔active). Not refuted; folded into 4 and 6.

---

## 1C. Cross-Reference

For each verified pair, list every function that writes to either side. Mark as ✅ SYNCED, ⚠️ GAP (primary audit target), or ❓ UNCLEAR.

| Pair | Function (file:line) | A-write | B-write | Verdict |
|------|----------------------|---------|---------|---------|
| **1: consensus_idx ↔ consensus_digests** | `Inner::save_consensus_output` (consensus_pack.rs:788-845) | `consensus_idx.save` (line 837) | `consensus_digests.save` (line 834) | ✅ SYNCED — both written before function returns Ok; if any intermediate fails, the function returns Err |
| | `Inner::stream_import` consensus arm (consensus_pack.rs:750-781) | `consensus_idx.save` (line 775) | `consensus_digests.save` (line 770) | ✅ SYNCED within a single record, but if Err returns between line 770 and 775 (no error returnable in that span — saves are immediately consecutive). However, on a LATER record's failure, `consensus_digests` keeps the entry from this record while `consensus_idx` does too. |
| | `Inner::trunc_and_heal` (consensus_pack.rs:474-527) | truncates `consensus_idx` (line 508/516) | **does NOT truncate `consensus_digests`** (comment at 489-493 acknowledges this); also doesn't truncate `batch_digests` | ⚠️ **GAP** — the explicit code comment confirms divergence; defensive `pos < data.file_len()` guards in `contains_consensus_header` exist but other readers (`consensus_header_by_digest` at line 930) don't filter, so a stale digest can return a corrupted record |
| | `Inner::stream_import` early-return failure paths (consensus_pack.rs:751-763) | only `consensus_digests` was written for the previous header; subsequent record errors out before the next consensus_idx write | (none for the failing record) | ⚠️ **GAP** — failure between `consensus_digests.save` (770) and `consensus_idx.save` (775) is unreachable in current code (consecutive saves), BUT failure on the NEXT record after this header is processed only damages the NEXT record. Net: pair stays consistent across the failure boundary because both indexes were saved for the just-committed header. |
| **2: header-indexes ↔ batch_digests** | `Inner::save_consensus_output` (consensus_pack.rs:807-839) | header indexes saved AFTER all batches | `batch_digests.save` (line 822) — saved BEFORE the header | ⚠️ **GAP** — if a batch save fails (`Err` at line 822-823), the function returns and the header is never saved. Result: `batch_digests` has entries for batches whose owning header is not in indexes. Defensive check via `Inner::files_consistent` (line 449-450) catches this on next open by comparing lengths. |
| | `Inner::stream_import` batch arm (consensus_pack.rs:737-749) | (none — batch arm doesn't touch consensus_idx) | `batch_digests.save` (line 743) | ⚠️ **GAP** — batches written to data file and indexed BEFORE the consumer header arrives. The accumulator `batches: HashSet<B256>` (line 730) is only checked when a header arrives (line 754-763); on stream failure between this write and the next header, the import dir is removed wholesale at consensus.rs:434, so the on-disk pollution is recovered. The risk is the import-dir's transient `batch_digests` state is mutated atomically per-batch; a partial stream means orphaned batches in the temp dir, then dir removal — net effect: no permanent corruption IF dir-removal is atomic with respect to readers. |
| | `Inner::stream_import` consensus arm (consensus_pack.rs:750-781) | `consensus_idx.save`, `consensus_digests.save` for header | clears nothing in `batch_digests` for verified batches (HashSet `batches.remove(digest)` is in-memory only) | ✅ SYNCED for the matched batches (because batches removed from HashSet at lines 754-763 means they're known to be in `batch_digests`) |
| **3: data file ↔ all 3 indexes** | `Inner::save_consensus_output` (consensus_pack.rs:817-842) | `data.append` × N | each `<index>.save` after | ✅ SYNCED on success path |
| | `Inner::save_consensus_output` (consensus_pack.rs:817-842) — mid-loop failure | `data.append` writes bytes to file unconditionally before `<index>.save` returns | (index untouched) | ⚠️ **GAP** — between line 820's `data.append` and line 822's `batch_digests.save`, an index error leaves `data` longer than indexed. Recovered by `trunc_and_heal` next open (truncates data to `consensus_final` or `batch_final`) — but recovery TRUNCATES legitimate just-appended data; net OK because the failure path bubbled up and the caller sees Err. |
| | `Inner::trunc_and_heal` (consensus_pack.rs:474-527) | `data.truncate` (line 485, 487, 523) | only `consensus_idx.truncate_*` (508, 516); `consensus_digests` and `batch_digests` keep stale entries | ⚠️ **GAP** — explicit by design comment but creates the pair-1 divergence already noted |
| **4: pending_epoch_requests ↔ epoch_stream_semaphore** | `process_epoch_stream` (mod.rs:666-749) | inserts entry into map (line 703) | acquires permit (line 674) — held inside the inserted entry | ✅ SYNCED — permit is OWNED by the entry; map.insert and permit-grab are atomic relative to each other under the same mutex critical section |
| | `process_epoch_stream` denial branch (mod.rs:686-688) | (no map insert) | permit dropped at end of arm (line 688) | ✅ SYNCED |
| | `cleanup_stale_pending_requests` (mod.rs:491-496) | `retain` removes timed-out entries | dropping the entry drops `_permit` | ✅ SYNCED (Drop-based) — but **a panic inside this function leaves entries in place AND leaks permits**: the function holds the parking_lot mutex across `retain`. Phase 2: confirm no panic vector. |
| | `process_inbound_stream` (mod.rs:798-800) | `remove` returns the entry; the spawned task owns it | permit dropped when the spawned task ends | ✅ SYNCED (the spawn captures `opt_pending_req` and `_permit` rides along) |
| **5: pending_epoch_requests entry ↔ importer task** | `process_epoch_stream` (mod.rs:702-711) | inserts entry | (no importer running yet — entry is "awaiting") | ⚠️ **GAP by design** — entry exists without a running importer. Reconciliation only via 15s prune (30s timeout). If peer opens the stream with a digest that does NOT match (e.g. forged), entry stays until prune. |
| | `process_inbound_stream` (mod.rs:798-800) | removes entry | spawns task that consumes the stream | ✅ SYNCED (consumes entry → spawns task = 1:1) |
| | `cleanup_stale_pending_requests` | drops entry | (no importer to cancel — entry was awaiting) | ✅ SYNCED |
| **6: semaphore permits ↔ active import work** | `process_epoch_stream` then `process_inbound_stream` chain | (permit held in entry until removed) | importer task lifetime = entry lifetime + spawn lifetime | ⚠️ **GAP** — `process_inbound_stream` REMOVES the entry from the map (mod.rs:798-800) which immediately frees the permit — but the spawned task is still running (it owns the `pending_request: Option<PendingEpochStream>` via its closure). Wait — let me re-read: `pending_map.lock().remove(...)` at line 798 returns `Option<PendingEpochStream>`; that value is bound to `opt_pending_req` (line 798), then captured by the closure passed to `request_handler.process_request_epoch_stream` (line 803-804). So the permit lives as long as the spawned task. ✅ Actually SYNCED. |
| | A panic inside the spawned task (mod.rs:778) | (entry already removed) | `_permit` is held in the closure; tokio task panic drops the closure, drops the permit | ✅ SYNCED via tokio's drop-on-panic |
| | A panic inside `cleanup_stale_pending_requests` (mod.rs:491-496) | leaves entries undropped (mutex held; parking_lot poisons on panic; subsequent `.lock()` panics) | permits leaked | ⚠️ **GAP** — there is no panic guard. The outer task is "critical" but if the prune-tick's call panics, the spawn ends. Critical-task panic recovery is not visible in this scope; needs Phase 2 verification. |
| **7: last_consensus_header watch ↔ validated DB headers** | `state-sync/consensus.rs:65` `send_replace(Some(header))` | watch advanced | `node_storage.insert::<ConsensusHeaderCache>` at line 51 happened first; the header was hash-validated by the helper that fetched it | ✅ SYNCED — but writes to the watch BEFORE `consensus_chain.consensus_header_by_digest` would return this header, because `ConsensusHeaderCache` is in `node_storage` not the pack file. |
| | `ConsensusChain::stream_import` (storage/consensus.rs:357-438) | does NOT touch `last_consensus_header` watch | writes new pack file | ❓ UNCLEAR — Phase 2: verify no stale watch survives a failed import. |
| | `ConsensusChain::save_consensus_output` (storage/consensus.rs:476-491) | does NOT touch `last_consensus_header` watch | writes pack + updates `latest_consensus` | ✅ SYNCED — separate channel; the watch is only advanced by state-sync helper. |
| **8: epoch_request_queue ↔ idempotency dedup** | `request_epoch_pack_file` (consensus_bus.rs:551-553) | sends to channel | (no dedup) | ⚠️ **GAP** — caller has no view of in-flight or pending epoch fetches; the dedup happened (per commit cb0e147c) on the responder's `process_epoch_stream`, not here. |
| | `spawn_track_recent_consensus` enqueue site (consensus.rs:116) | sends epoch record | preceded by `consensus_chain.consensus_header_by_number(...).is_some()` check — does NOT verify "no pending fetch" | ⚠️ **GAP** — same as above; the only guard is "we don't already have the final header". A re-enqueue at near-completion is possible. |
| | `spawn_fetch_consensus` (consensus.rs:159-219) | dequeues; per-task semaphore prevents one task from running 2 in parallel; **does not coordinate across multiple tasks** | calls `network.request_epoch_pack` | ⚠️ **GAP** — multiple `spawn_fetch_consensus` tasks each have their own `Arc<Semaphore::new(1)>` (line 174 — note the `Arc::new` is INSIDE the function, so each invocation creates an independent semaphore). |
| **9: peer-score ↔ penalty target** | `request_epoch_pack` (mod.rs:381) | `report_penalty(peer, Mild)` | `peer` came from response payload (line 342) | ⚠️ **GAP** — penalty applied to BLS key claimed by responder, not the libp2p PeerId of the actual responder. This is the headline attack vector "penalty arbitrage / forged BLS claim". |
| | `process_consensus_output_request` (mod.rs:626) | `report_penalty(peer, Mild)` | `peer` is the inbound request's PeerId-derived BLS — **trusted side** | ✅ SYNCED |
| | `process_inbound_stream` (mod.rs:809) | `report_penalty(peer, ...)` | `peer` is the inbound stream's PeerId — **trusted side** | ✅ SYNCED |
| **10: auth_last_vote ↔ current epoch** | `RequestHandler::new` (handler.rs:81-87) | preloads with committee at construction | (epoch-bound at construction) | ⚠️ **GAP** — the handler is held for the lifetime of the `PrimaryNetwork`, which is created per `spawn` (mod.rs:463). Verify in Phase 2 whether the network is rebuilt each epoch. If not, the map carries stale entries. |
| | `vote` (handler.rs:330-440) | sets `auth_last_vote[author] = (epoch, round, digest, response)` | doesn't clear on epoch change | ⚠️ **GAP** — `last_epoch < header.epoch()` etc. are ordering checks but the `take()` at line 369 always consumes the prior entry, then re-inserts the new one (line 435). Cross-epoch attack vector: vote at end of epoch N is cached, attacker at start of epoch N+1 sends a vote with the same digest → `last_digest == header.digest()` matches → bypasses the round/epoch check (line 371-405) and returns the cached response. |
| | `ConsensusBusApp::reset_for_epoch` (consensus_bus.rs:304-307) | resets `committed_round` and `primary_round` only | does NOT touch `auth_last_vote`, `consensus_certs`, `requested_parents` | ⚠️ **GAP** — confirms cross-epoch state is not cleared by the documented reset path |

---

## Primary Audit Targets (GAP rows, ranked)

Each entry: file:line, what's missing, why it matters.

### CRITICAL/HIGH

1. **Pair 9 — `request_epoch_pack` mis-targets penalty (mod.rs:342, 381)**
   _What's missing:_ no verification that the `peer: BlsPublicKey` in `PrimaryResponse::RequestEpochStream { peer }` matches the libp2p PeerId of the actual responder.
   _Why it matters:_ a single misbehaving peer P can return a response with another peer Q's BLS key; penalty on stream failure is applied to Q. Repeatable — Q gets evicted, P stays in mesh. Combined with `Penalty::Mild` (no escalation), this is asymmetric griefing.

2. **Pair 1+3 — `Inner::trunc_and_heal` deliberately leaves stale digest entries (consensus_pack.rs:489-493)**
   _What's missing:_ the explicit comment says digests aren't truncated. `consensus_header_by_digest` (line 930) does `consensus_digests.load(digest).ok()?` then `data.fetch(pos)` without checking `pos < data.file_len()`. If the data file was truncated mid-record, `data.fetch(pos)` either reads garbage past the truncation or returns FetchError, masquerading as "header not found". `contains_consensus_header` (918) has the defensive check; `consensus_header_by_digest` does not.
   _Why it matters:_ inconsistent reader behaviour after crash recovery; corruption persists across reopens.

3. **Pair 2 — `Inner::stream_import` writes batches to data file before validating header (consensus_pack.rs:737-749)**
   _What's missing:_ batches and `batch_digests` are written to the import dir before the matching header arrives. The "orphan-batch scar" relies on someone reusing the import dir or a partial rename. Caller `ConsensusChain::stream_import` (storage/consensus.rs:434) removes the dir on Err. Phase 2: verify all error paths inside `consensus_pack::stream_import` reach that `remove_dir_all`.
   _Why it matters:_ if any error path causes the import dir to survive (e.g. panic between PackError and the `Err(...)` return; rename succeeds for partial pack), batches whose header never validated end up in the persisted DB.

4. **Pair 5+6 — `cleanup_stale_pending_requests` has no panic guard (mod.rs:491-496, worker mod.rs:436-441)**
   _What's missing:_ the function holds a parking_lot mutex across `retain`, with no `catch_unwind` or panic-safe wrapper. parking_lot `Mutex` does not poison on panic but the spawned outer task ends; this is a "critical" task but no restart logic visible in the scope.
   _Why it matters:_ slot leak vector #3 from domain-patterns.md. Combined with `MAX_CONCURRENT_EPOCH_STREAMS=5` and `MAX_PENDING_REQUESTS_PER_PEER=2`, three peers can each fill the global cap in a few requests if prune ever stops.

5. **Pair 8 — `request_epoch_pack_file` lacks dedup at enqueue (consensus_bus.rs:551-553) AND `spawn_fetch_consensus` creates per-task semaphore (state-sync/consensus.rs:174)**
   _What's missing:_ no pre-enqueue dedup; the `next_sem` semaphore is created inside `spawn_fetch_consensus`'s body so each task has an independent permit-of-1; multiple tasks can run two epochs concurrently.
   _Why it matters:_ adversarial sequence #5 in domain-patterns. Attacker times re-issue at near-completion. The first import succeeds (writes via rename), the second sees the new pack via `stream_import`'s early-return at consensus.rs:365-374. Net: probably benign for the first scenario but two parallel imports of two DIFFERENT epochs from a peer that misclaims epoch numbers can collide on the import dir naming `import-{epoch}` (consensus.rs:377). Same epoch from same peer also stomps because line 379 unconditionally `remove_dir_all(&path)` — if task A is mid-import, task B blows away its scratch dir. **Confirmed bug surface.**

6. **Pair 10 — `auth_last_vote` cached response replay across epochs (handler.rs:330-440)**
   _What's missing:_ the cached response check at line 371 (`last_digest == header.digest()`) returns the cached PrimaryResponse without verifying the cached entry's epoch matches the new header's epoch.
   _Why it matters:_ at an epoch boundary, replaying the prior epoch's vote-request digest can produce a stale `Vote` from this validator. Mitigated by `header.author` peer-check at line 343 (peer must equal author), but if the author equivocates across epochs the equivocation guard FIRES on `last_digest == header.digest()` and short-circuits before any new validation. **Verify equivocation guard's intent matches behaviour.**

### MEDIUM

7. **Pair 1 — `Inner::stream_import` consensus arm save-order between two indexes (consensus_pack.rs:770-777)**
   _What's missing:_ if `consensus_digests.save` succeeds and `consensus_idx.save` fails, the in-memory `Inner` returns Err and the import dir is wiped — fine. But the failure path during `Inner::save_consensus_output` (line 834-839) writes both INTO PERSISTED files; on `consensus_idx.save` failure (line 837-839), `consensus_digests` already has the entry on disk. **In-process recovery via `trunc_and_heal` does not remove the `consensus_digests` entry** (see GAP 2).
   _Why it matters:_ enduring index/data divergence on the live current_pack.

8. **Pair 7 — `last_consensus_header` watch advances before pack contains header**
   _What's missing:_ `state-sync/consensus.rs:65` writes the watch immediately after `ConsensusHeaderCache.insert`, NOT after the pack-file (consensus_chain) finalizes. Downstream consumers reading the watch can request the header via `consensus_chain.consensus_header_by_*` and get None.
   _Why it matters:_ inconsistent visibility; not memory-corruption but causes flaky retries that increase load.

9. **Pair 5 — pending entry has no liveness check tied to actual peer connectivity**
   _What's missing:_ the entry stays for 30s regardless of whether the libp2p connection to that peer dropped. `process_inbound_stream` is the only consumer.
   _Why it matters:_ slot leak per disconnected peer.

### LOW / INFO

10. **`pack_iter::read_record_file` `buffer.resize(val_size as usize, 0)` (pack_iter.rs:76, 149) and `pack.rs:320, 344`** — no upper bound on `val_size`. A peer crafting `val_size = u64::MAX` triggers OOM.
    _Why it matters:_ adversarial sequence #4 (unbounded allocation OOM).

11. **`process_epoch_stream` rejection drops permit silently (mod.rs:688)** — the deny branch returns `false`; the response is then `RequestEpochStream { ack: false, peer: self.bls_public_key }` which the requester treats as "try next peer". No penalty against the malicious caller.

12. **`reset_for_epoch` (consensus_bus.rs:304)** does not clear `auth_last_vote`, `consensus_certs`, `requested_parents`, `pending_epoch_requests`, `last_consensus_header`, `last_published_consensus_num_hash`.

---

## Open Questions for Phase 2

1. **Is `epoch_stream_semaphore.permits + pending_epoch_requests.len()` strictly preserved across all panic boundaries in `cleanup_stale_pending_requests` and `process_inbound_stream`?**
   - Specifically, what happens if `request_handler.process_request_epoch_stream` panics? The closure capturing `opt_pending_req` is dropped, so the permit is released — good. But `task_spawner.spawn_task` semantics: does the spawn handle propagate the panic to the supervisor, and does the supervisor restart the prune loop?

2. **Does `PrimaryResponse::RequestEpochStream { peer }` get cross-checked against the libp2p PeerId of the responder anywhere (network layer)?**
   - If yes, the GAP 1 finding is partially mitigated.
   - If no, this is HIGH severity.

3. **Does `ConsensusChain::stream_import` (consensus.rs:377) handle the "import-{epoch}" path collision when two `spawn_fetch_consensus` tasks fetch the same epoch concurrently?**
   - Line 379 unconditionally `remove_dir_all(&path)` would erase the in-progress import of the other task.

4. **Is the `auth_last_vote` cache cleared on epoch transition?**
   - `RequestHandler::new` is called once at `PrimaryNetwork::new` (mod.rs:438). Phase 2: confirm whether `PrimaryNetwork` is rebuilt per-epoch via `epoch_task_spawner` or persists for application lifetime.

5. **Does `Inner::stream_import` (consensus_pack.rs:673) call `consensus_idx.sync()` / `digest indexes' sync()` between writes, or only at end via `Inner::persist`?**
   - If only at end, a process crash mid-stream-import leaves the import dir with on-disk data file + partially-written index files; recovery via `trunc_and_heal` on next open of the import dir.
   - But the import dir is removed on Err (consensus.rs:434). What if the process is killed (no Err return)? Phase 2: verify the import dir cleanup path on process restart (is there one?).

6. **`Inner::trunc_and_heal` digest-index staleness — can `consensus_header_by_digest` return a header at a position that was truncated out of `data`?**
   - `Inner::contains_consensus_header` (line 922-923) defensively checks `pos < data.file_len()`, but `Inner::consensus_header_by_digest` (930-934) does not.

7. **`request_epoch_pack` retries 3 peers on failure (mod.rs:338-393), but the second peer attempt blindly trusts a fresh `RequestEpochStream { peer }` claim. Is the BlsPublicKey of the third peer guaranteed to differ?**
   - `send_request_any` likely picks a different peer per call, but Phase 2: confirm and check for the case where libp2p has only 2-3 peers connected.

8. **`spawn_fetch_consensus` instantiates `Arc<Semaphore::new(1)>` inside its async fn body (consensus.rs:174). Multiple spawn calls produce independent semaphores. Was this intentional?**
   - If global concurrency limit was the goal, the Arc should be a parameter or a static.

9. **`PrimaryNetworkError::StreamUnavailable(Epoch)` returns `None` for Penalty (network.rs:128).**
   - A peer could repeatedly request streams for epochs we don't have, getting "stream unavailable" each time. No reputation cost. Phase 2: Should this be `Penalty::Mild` to deter reconnaissance probes?

10. **`MAX_PENDING_REQUESTS_PER_PEER = 2` check (mod.rs:679) is a `keys().filter().count()` under the mutex.**
    - Confirmed: race between two concurrent inbound requests is bounded by the same `pending_map.lock()` so they serialise. But `try_acquire_owned` happens BEFORE the lock; a peer could repeatedly probe to keep permits cycling, exhausting the global semaphore even when the per-peer count is enforced. Worth tracing in Phase 2.

---

## Summary

- **1A**: 50+ functions catalogued across 12 modules.
- **1B**: 10 coupled pairs with 8 VERIFIED, 1 NEEDS-PHASE-2-INTERROGATION, 0 strict refutations (one was folded).
- **1C**: 12 ⚠️ GAP rows identified; 6 ranked CRITICAL/HIGH, 3 MEDIUM, 3 LOW.

The highest-leverage Phase 2 targets are:
- (HIGH) Pair 9 — penalty mis-targeting via unauthenticated BLS in response payload.
- (HIGH) Pair 1+3 — `trunc_and_heal` deliberately divergent digest indexes.
- (HIGH) Pair 5/6 — prune-loop panic guard / slot-leak vector.
- (HIGH) Pair 8 — `import-{epoch}` directory collision under concurrent fetches.

These four directly map to adversarial sequences #3, #6, #3+#10, and #5 respectively from the domain-patterns analysis.
